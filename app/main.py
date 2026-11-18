"""FastAPI application: read-only fault-log queries plus retention cleanup.

The read-only side keeps exactly two PostgreSQL connections; the retention
side (``/retention/step``) uses a separate writable maintenance connection.

Run locally::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Swagger UI is served at ``/docs``, ReDoc at ``/redoc``.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import settings
from .errors import InvalidDeadlineError, QueryServiceError
from .executor import QueryExecutor
from .pool import QueryConnectionPool
from .retention import MaintenanceConnection
from .schemas import (
    HealthResponse,
    QueryRequest,
    QueryResponse,
    RetentionStepRequest,
    RetentionStepResponse,
)
from .validation import validate_single_select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")

pool = QueryConnectionPool(settings)
executor = QueryExecutor(pool, settings)
# Independent writable connection for retention cleanup; never borrows from
# the two-connection read-only query pool.
maintenance = MaintenanceConnection(settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Eagerly open both read-only connections and the maintenance connection
    # at startup; close them on shutdown.
    await pool.open()
    await maintenance.open()
    try:
        yield
    finally:
        await maintenance.close()
        await pool.close()


app = FastAPI(
    title="机房设备故障日志查询与保留期清理 API",
    description=(
        "值班分析员共用的只读 SQL 查询入口。固定两条 PostgreSQL 连接，"
        "提交单条 SELECT 与总截止时间；排队时间计入预算，超时立即取消语句，"
        "确认事务结束才复用连接，无法确认则弃置并补建。"
        "另有独立维护连接支撑 /retention/step 分批清理，不占用查询连接。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.exception_handler(QueryServiceError)
async def _service_error_handler(_: Request, exc: QueryServiceError):
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "error": {"code": exc.code, "message": str(exc)},
            **exc.extra,
        },
    )


@app.exception_handler(Exception)
async def _unexpected_handler(_: Request, exc: Exception):
    logger.exception("unhandled error")
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": f"{type(exc).__name__}: {exc}",
            }
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["运维"])
async def health() -> HealthResponse:
    """连接池健康状态：容量、空闲/占用数、累计创建与弃置数。"""
    status = await pool.status()
    return HealthResponse(
        status="healthy",
        pool={
            "size": status.size,
            "idle": status.idle,
            "in_use": status.in_use,
            "total_created": status.total_created,
            "total_discarded": status.total_discarded,
            "closed": status.closed,
        },
        maintenance=await maintenance.status(),
    )


@app.post(
    "/query",
    response_model=QueryResponse,
    tags=["查询"],
    responses={
        400: {"description": "SQL 不符合单条只读 SELECT 约束"},
        422: {"description": "请求体或截止时间不合法"},
        503: {"description": "排队超时 / 连接被弃置"},
        504: {"description": "语句执行超过总截止时间"},
    },
)
async def query(body: QueryRequest) -> QueryResponse:
    """执行一条只读 SELECT，``timeout_ms`` 是包含排队时间的总预算。"""
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    sql = validate_single_select(body.sql, settings.max_sql_chars)

    # Absolute monotonic deadline: waiting + execution share one budget.
    deadline = time.monotonic() + body.timeout_ms / 1000.0
    # A deadline that expires while queued surfaces as a 503
    # acquire_timeout; an execution deadline as a 504 query_timeout.
    result = await executor.execute(sql, deadline)
    return QueryResponse(**result)


@app.post(
    "/retention/step",
    response_model=RetentionStepResponse,
    tags=["保留期清理"],
    responses={
        409: {"description": "截止时间冲突 / 跳步 / 任务已完成，本次未删除任何记录"},
        422: {"description": "请求体不合法（缺时区、step_no 小于 1 等）"},
        500: {"description": "维护连接或事务失败，已回滚、进度不推进"},
    },
)
async def retention_step(body: RetentionStepRequest) -> RetentionStepResponse:
    """执行一个保留期清理步骤（独立维护连接，每步最多删除 50 条）。

    步骤必须按序提交；已提交过的步骤重试时返回原始回执且不再删除；
    截止时间冲突、跳步或任务已完成都会在删除前被拒绝。删除、进度与
    回执在同一事务提交，服务重启后凭任务 ID 即可接续。
    """
    result = await maintenance.run_step(
        body.task_id, body.cutoff_at, body.step_no
    )
    return RetentionStepResponse(**result)
