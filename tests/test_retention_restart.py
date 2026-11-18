"""Durability/restart protocol tests for the retention maintenance connection.

These tests do not need the HTTP server.  They drive
:class:`app.retention.MaintenanceConnection` directly against an ephemeral
PostgreSQL and concentrate on the hardest guarantees:

* progress and receipts are persisted in the database, so a brand-new
  process (a new MaintenanceConnection pointed at the same database) resumes
  a task by its task id with no loss or double deletion;
* an in-flight failure rolls the whole step back (failure does not advance);
* a terminated maintenance backend is transparently reconnected;
* the read-only fault_ro role physically cannot perform the cleanup
  (defence in depth), while fault_maint can.

Run:  python tests/test_retention_restart.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import (  # noqa: E402
    RetentionCutoffConflict,
    RetentionStepGap,
    RetentionTaskCompleted,
)
from app.retention import MaintenanceConnection  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def with_role(dsn: str, role: str) -> str:
    parts = urlparse(dsn)
    old = parts.username or "postgres"
    return urlunparse(
        parts._replace(netloc=parts.netloc.replace(old, role, 1))
    )


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="ret-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    fault_admin = urlunparse(urlparse(admin_dsn)._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
        cur.execute("ALTER ROLE fault_maint PASSWORD NULL")
        # Disjoint eligible sets.  job-A deletes old rows under a past
        # cutoff; job-B deletes *future-dated* rows under a future cutoff.
        # A row can never be older than both cutoffs, so the tasks cannot
        # consume each other's input.
        cutoff_a = datetime(2025, 1, 1, tzinfo=timezone.utc)
        cutoff_b = datetime.now(timezone.utc) + timedelta(days=30)
        cur.execute(
            "INSERT INTO fault_logs "
            "(device_id, site, severity, fault_code, message, occurred_at) "
            "SELECT 'OLD-' || g, '机房', 'INFO', 'R', 'x', "
            "%s - (g || ' days')::interval "
            "FROM generate_series(1, 120) g",
            (cutoff_a,),
        )
        # g=1 -> cutoff_b-1d ... g=120 -> cutoff_b-120d: all in the future,
        # hence invisible to job-A's past cutoff, all older than cutoff_b.
        cur.execute(
            "INSERT INTO fault_logs "
            "(device_id, site, severity, fault_code, message, occurred_at) "
            "SELECT 'JOBB-' || g, '机房', 'INFO', 'R', 'x', "
            "%s - (g || ' days')::interval "
            "FROM generate_series(1, 120) g",
            (cutoff_b,),
        )
        cur.execute(
            "INSERT INTO fault_logs "
            "(device_id, site, severity, fault_code, message) "
            "VALUES ('NEW-1', '机房', 'INFO', 'R', 'x')"
        )
    cutoff = cutoff_a  # default for job-A below

    maint_dsn = with_role(fault_admin, "fault_maint")
    ro_dsn = with_role(fault_admin, "fault_ro")
    settings = Settings(
        database_dsn=ro_dsn,
        maintenance_dsn=maint_dsn,
        pool_size=2,
        retention_step_limit=50,
    )

    print("=== A. 服务重启后凭任务 ID 接续 ===")
    # "Process 1": run steps 1 and 2, then close (simulate shutdown).
    conn1 = MaintenanceConnection(settings)
    await conn1.open()
    r = await conn1.run_step("job-A", cutoff, 1)
    check("步骤1 删 50", r["step_deleted"] == 50 and r["total_deleted"] == 50
          and r["completed"] is False and r["retried"] is False, r)
    r = await conn1.run_step("job-A", cutoff, 2)
    check("步骤2 再删 50", r["step_deleted"] == 50 and r["total_deleted"] == 100, r)
    await conn1.close()

    # "Process 2": a completely new instance, no in-memory state at all.
    conn2 = MaintenanceConnection(settings)
    await conn2.open()
    r = await conn2.run_step("job-A", cutoff, 3)
    check("新进程接续步骤3 删剩余 20",
          r["step_deleted"] == 20 and r["total_deleted"] == 120
          and r["completed"] is True, r)

    # Retrying an old step after restart returns the persisted receipt.
    r = await conn2.run_step("job-A", cutoff, 1)
    check("重启后重试步骤1 返回持久化原回执",
          r["retried"] is True and r["step_deleted"] == 50
          and r["total_deleted"] == 50 and r["completed"] is False, r)

    # New step after completion is rejected, and nothing more is deleted.
    try:
        await conn2.run_step("job-A", cutoff, 4)
        check("完成后拒绝新步骤", False)
    except RetentionTaskCompleted:
        check("完成后拒绝新步骤", True)

    with psycopg.connect(fault_admin) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
                    (cutoff_a,))
        check("job-A 窗口过期行全部删除", cur.fetchone()[0] == 0)
        cur.execute("SELECT count(*) FROM fault_logs")
        # 7 预置 + 1 新增 + 120 条属于 job-B 窗口的行
        check("新行与其它任务的行保留", cur.fetchone()[0] == 128)
        cur.execute(
            "SELECT next_step_no, total_deleted, completed "
            "FROM retention_tasks WHERE task_id='job-A'"
        )
        check("任务表状态为 4/120/完成", cur.fetchone() == (4, 120, True))
        cur.execute(
            "SELECT count(*), sum(step_deleted) "
            "FROM retention_step_receipts WHERE task_id='job-A'"
        )
        check("三份回执合计删除 120", cur.fetchone() == (3, 120))

    print("=== B. 失败不推进：事务回滚后可原样重试 ===")
    # Kill the maintenance backend mid-request by revoking its privilege;
    # the operation errors, the transaction must roll back and the task's
    # next_step_no must stay untouched.  job-B has its own cutoff window.
    await conn2.run_step("job-B", cutoff_b, 1)  # establishes task at step 2
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute("REVOKE DELETE ON fault_logs FROM fault_maint")
    try:
        await conn2.run_step("job-B", cutoff_b, 2)
        check("权限撤销后步骤失败抛错", False)
    except psycopg.Error as exc:
        check("权限撤销后步骤失败抛错", True, str(exc)[:80])
    with psycopg.connect(fault_admin) as c, c.cursor() as cur:
        cur.execute(
            "SELECT next_step_no, total_deleted, completed "
            "FROM retention_tasks WHERE task_id='job-B'"
        )
        check("失败后进度不推进(仍为 2/50/未完成)",
              cur.fetchone() == (2, 50, False))
        cur.execute(
            "SELECT count(*) FROM retention_step_receipts "
            "WHERE task_id='job-B' AND step_no=2"
        )
        check("失败步骤无回执残留", cur.fetchone()[0] == 0)
    # Restore privilege; the same step retries successfully and reconnects
    # transparently (the failed connection was discarded).
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute("GRANT SELECT, DELETE ON fault_logs TO fault_maint")
    r = await conn2.run_step("job-B", cutoff_b, 2)
    check("失败步骤原样重试成功",
          r["step_deleted"] == 50 and r["total_deleted"] == 100
          and r["retried"] is False, r)

    print("=== C. 维护连接被带外终止后自动重连 ===")
    # Record what step 3 must do against the live state (section B's rolled
    # back step deleted nothing, but the rows it touched remain eligible).
    with psycopg.connect(fault_admin) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
                    (cutoff_b,))
        eligible_before = cur.fetchone()[0]
        cur.execute(
            "SELECT total_deleted FROM retention_tasks WHERE task_id='job-B'"
        )
        deleted_before = cur.fetchone()[0]
        cur.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE usename='fault_maint' AND pid <> pg_backend_pid()"
        )
        pids = [row[0] for row in cur.fetchall()]
    check("存在 fault_maint 后端", len(pids) >= 1, pids)
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        for pid in pids:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
    # Next step must reconnect by itself and still honour the protocol.
    r = await conn2.run_step("job-B", cutoff_b, 3)
    expect_deleted = min(50, eligible_before)
    check(
        "终止后自动重连，步骤3 按实际余量完成任务",
        r["step_deleted"] == expect_deleted
        and r["completed"] is True
        and r["total_deleted"] == deleted_before + expect_deleted,
        r,
    )
    with psycopg.connect(fault_admin) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
                    (cutoff_b,))
        check("job-B 窗口最终清空", cur.fetchone()[0] == 0)

    print("=== D. 冲突规则在新进程中同样生效 ===")
    try:
        await conn2.run_step("job-A", cutoff_a + timedelta(days=1), 2)
        check("截止时间冲突拒绝", False)
    except RetentionCutoffConflict:
        check("截止时间冲突拒绝", True)
    try:
        await conn2.run_step("brand-new-job", cutoff_a, 7)
        check("未知任务跳步拒绝", False)
    except RetentionStepGap:
        check("未知任务跳步拒绝", True)

    print("=== E. 角色纵深防御：fault_ro 无法执行清理 ===")
    try:
        with psycopg.connect(ro_dsn) as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM fault_logs WHERE occurred_at < %s", (cutoff_a,)
            )
        check("只读角色 DELETE 被数据库拒绝", False)
    except (psycopg.errors.InsufficientPrivilege,
            psycopg.errors.ReadOnlySqlTransaction) as exc:
        # ReadOnlySqlTransaction comes from the role-level
        # default_transaction_read_only=on; InsufficientPrivilege from the
        # missing GRANT.  Either way the database itself blocks the write.
        check("只读角色 DELETE 被数据库拒绝", True, type(exc).__name__)
    with psycopg.connect(maint_dsn) as c, c.cursor() as cur:
        cur.execute("SELECT count(*) FROM retention_tasks")
        check("维护角色可读取进度表", cur.fetchone()[0] >= 1)

    await conn2.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
