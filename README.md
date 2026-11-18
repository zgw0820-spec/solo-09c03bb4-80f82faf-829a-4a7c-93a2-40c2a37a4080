# 机房设备故障日志查询与保留期清理 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

另有**保留期分批清理**入口 `POST /retention/step`：使用独立的可写维护
连接（不占用两条只读连接），每步最多删除 50 条早于固定截止时间的
`fault_logs`，删除、进度与步骤回执在同一事务提交；请求丢失或服务重启
后凭任务 ID 即可接续，重试返回原回执、绝不重复删除。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志、只读角色 `fault_ro` 与
  清理维护角色 `fault_maint`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest             # 只读查询 46 项
docker compose exec api python -m tests.selftest_retention   # 清理接口 HTTP 51 项
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数，以及独立维护连接状态。

### `POST /retention/step`（保留期分批清理）

```json
{
  "task_id": "cleanup-2026-11",
  "cutoff_at": "2026-11-10T00:00:00Z",
  "step_no": 1
}
```

`cutoff_at` 为**任务固定截止时间**（ISO 8601、必须带时区），仅删除
`occurred_at` 早于该时间的日志；`step_no` 从 1 开始，同一任务必须按序
提交。每步最多删除 50 条，本步删除数不足 50 即表示没有更多符合条件的
记录，任务完成。

响应示例：

```json
{
  "task_id": "cleanup-2026-11",
  "step_no": 1,
  "cutoff_at": "2026-11-10T00:00:00Z",
  "step_deleted": 50,
  "total_deleted": 50,
  "completed": false,
  "retried": false
}
```

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 正常步骤 | 200 | `step_deleted`（本步）、`total_deleted`（累计）、`completed`、`retried=false` |
| 已提交步骤重试/请求丢失重发 | 200 | `retried=true`，返回**原始回执**，不再删除、不推进 |
| 截止时间与已登记值不一致 | 409 | `error.code=cutoff_conflict`，删除前拒绝 |
| 跳步（未知任务非 1、或步骤号不连续） | 409 | `error.code=step_gap`，不创建任务、不删除 |
| 任务完成后提交新步骤 | 409 | `error.code=task_completed`，不再删除 |
| 请求体不合法（缺时区、`step_no<1` 等） | 422 | Pydantic 校验回执 |
| 维护连接/事务失败 | 500 | 整步回滚，进度不推进，下次请求自动重连 |

## 保留期清理的关键设计

### 1. 独立维护连接，查询零挤占

清理走单独的 `fault_maint` 可写角色连接（`app/retention.py`），与容量
恒为 2 的 `fault_ro` 只读池物理隔离。清理进行期间两条只读查询连接的
排队/超时行为完全不变，`/query` 持续可用（自测 R6 会在数据库侧核对
`fault_ro` 后端数始终不超过 2）。`fault_ro` 角色在数据库层就无法
DELETE（只读默认事务 + 缺权限的纵深防御）。

### 2. 进度与回执全部落库，单事务原子提交

两张状态表（`db/01_init.sql` 建表，应用启动也会 `CREATE IF NOT EXISTS`
自愈）：

- `retention_tasks`：任务 ID、固定截止时间、下一个期望步骤号、累计
  删除数、完成标志；
- `retention_step_receipts`：每个已提交步骤的不可变回执（本步/累计
  删除数、完成标志、重试标志）。

每个步骤在**一个显式事务**内完成：`SELECT ... FOR UPDATE` 锁定任务行
→ 查重（已提交则返回原回执）→ 受限 DELETE → 更新进度 → 插入回执 →
提交。任一步失败整体回滚，失败不推进；提交后即使响应丢失，重试也只
会取回原回执。

### 3. 顺序约束与完成语义

- 未知任务的首个请求必须是 `step_no=1`，否则按跳步拒绝且不落任何状态；
- 截止时间是任务的固定参数，冲突在任何写入前拒绝；
- 完成判定以“本步删除数 < 50”为准（窗口已无符合条件的行）；完成后
  新步骤被 `task_completed` 拒绝，但**已提交步骤的重试仍返回原回执**；
- 进程内 `asyncio.Lock` 与数据库行锁双重串行化，并发重复提交只会产生
  一次删除（自测 R5 覆盖 4 并发同步骤）。

### 4. 重启接续与连接自愈

任务状态只存在 PostgreSQL，进程内存不保存任何进度。服务重启后新进程
凭任务 ID 直接续做；维护连接被带外终止时，下一次请求先做存活探测、
必要时自动重连（状态不丢）。本机自测可用
`tests/test_retention_restart.py` 验证重启接续、回滚不推进与重连。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn（数据目录持久化，可模拟重启接续）
python -m tests.selftest         # 端到端自测（46 项）
python -m tests.selftest_retention   # 清理接口 HTTP 端到端（51 项）
python tests/test_protocol.py    # 超时/迟到结果/排队协议直测（11 项）
python tests/test_retention_restart.py  # 清理重启接续/回滚/重连直测（20 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/retention/validation/
                schemas/errors/main）
db/01_init.sql  建表、预置日志、fault_ro/fault_maint 角色与清理状态表
                （Compose 首次初始化执行）
tests/          selftest.py（只读 HTTP 端到端）、
                selftest_retention.py（清理 HTTP 端到端）、
                test_protocol.py（超时协议直测）、
                test_retention_restart.py（清理持久化直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
