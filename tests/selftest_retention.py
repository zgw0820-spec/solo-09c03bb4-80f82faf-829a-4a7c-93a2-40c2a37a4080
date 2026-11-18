"""End-to-end tests for POST /retention/step (retention cleanup workflow).

Uses only the Python standard library for the HTTP calls, like
``tests/selftest.py``; ``psycopg`` with the admin DSN is used to seed and
to verify the database-side state.

    python -m tests.selftest_retention

Scenarios:
  1. a full task runs in capped 50-row steps until completion;
  2. retried steps return the original receipt and never double-delete;
  3. cutoff conflicts, step gaps and post-completion steps delete nothing;
  4. concurrent duplicate submissions of one step are serialised (one delete);
  5. read-only /query stays available and bounded to two connections while
     cleanup is running;
  6. receipts/progress are durable and resumable across a service restart
     (covered separately by tests/test_retention_restart.py).
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "http://127.0.0.1:8000"

PASS = 0
FAIL = 0

# Cutoff for the seeded rows: all SEED_OLD_ROWS seeded rows are older, the
# baseline rows created by db/01_init.sql (recent) are newer and survive.
CUTOFF = datetime(2026, 11, 10, 0, 0, 0, tzinfo=timezone.utc)
SEED_OLD_ROWS = 130
STEP_LIMIT = 50


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def get(path: str, timeout: float = 10.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def admin_connect():
    import psycopg

    admin_dsn = os.environ.get("ADMIN_DSN")
    if not admin_dsn:
        dsn_file = os.path.join(tempfile.gettempdir(), "faultdb_admin_dsn")
        if os.path.exists(dsn_file):
            admin_dsn = open(dsn_file, encoding="utf-8").read().strip()
    if not admin_dsn:
        raise RuntimeError("未设置 ADMIN_DSN，无法进行数据库侧核对")
    return psycopg.connect(admin_dsn, autocommit=True)


def step_payload(task: str, no: int, cutoff: datetime | None = None) -> dict:
    return {
        "task_id": task,
        "cutoff_at": (cutoff or CUTOFF).isoformat(),
        "step_no": no,
    }


def run_case(title: str, fn) -> None:
    print(f"\n=== {title} ===")
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - test harness keeps going
        check(f"{title} 未抛异常", False, repr(exc))


# ---------------------------------------------------------------------- #
def seed_and_count() -> tuple[int, int]:
    with admin_connect() as conn, conn.cursor() as cur:
        # Remove rows/state from previous runs, then seed exactly
        # SEED_OLD_ROWS RET-SEED-* rows older than the cutoff.  All test
        # tasks in this suite share one prefix/window; tasks use distinct
        # task ids and run against freshly reset state each seed.
        cur.execute("DELETE FROM retention_step_receipts")
        cur.execute("DELETE FROM retention_tasks")
        cur.execute(
            "DELETE FROM fault_logs WHERE device_id LIKE 'RET-SEED-%'"
        )
        cur.execute(
            "INSERT INTO fault_logs "
            "(device_id, site, severity, fault_code, message, occurred_at) "
            "SELECT 'RET-SEED-' || g, '测试机房', 'INFO', 'RET', '保留期测试', "
            "%s - (g || ' minutes')::interval "
            "FROM generate_series(1, %s) AS g",
            (CUTOFF, SEED_OLD_ROWS),
        )
        cur.execute("SELECT count(*) FROM fault_logs")
        total = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
            (CUTOFF,),
        )
        eligible = cur.fetchone()[0]
    return total, eligible


def case_health_reports_maintenance() -> None:
    status, body = get("/health")
    check("健康检查 200", status == 200, body)
    check("只读池容量仍为 2", body["pool"]["size"] == 2, body)
    check(
        "维护连接独立在线",
        body.get("maintenance", {}).get("connected") is True,
        body,
    )


def case_happy_path_and_cap() -> None:
    total, eligible = seed_and_count()
    check(f"预置 {SEED_OLD_ROWS} 条过期日志", eligible == SEED_OLD_ROWS, eligible)
    task = "task-happy"

    # Step 1: exactly 50 rows, not yet complete.
    status, body = post("/retention/step", step_payload(task, 1))
    check("步骤1 返回 200", status == 200, body)
    check("步骤1 删除 50 条", body.get("step_deleted") == 50, body)
    check("步骤1 累计 50", body.get("total_deleted") == 50, body)
    check("步骤1 尚未完成", body.get("completed") is False, body)
    check("首次提交 retried=false", body.get("retried") is False, body)

    # Step 2: another 50.
    status, body = post("/retention/step", step_payload(task, 2))
    check("步骤2 删除 50 条", status == 200 and body["step_deleted"] == 50, body)
    check("步骤2 累计 100", body["total_deleted"] == 100, body)

    # Step 3: remaining 30 (< 50) -> completed.
    status, body = post("/retention/step", step_payload(task, 3))
    check("步骤3 删除剩余 30 条", status == 200 and body["step_deleted"] == 30, body)
    check("步骤3 累计 130", body["total_deleted"] == 130, body)
    check("步骤3 标记完成", body["completed"] is True, body)

    with admin_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
                    (CUTOFF,))
        remaining_old = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM fault_logs")
        remaining_total = cur.fetchone()[0]
        cur.execute(
            "SELECT next_step_no, total_deleted, completed "
            "FROM retention_tasks WHERE task_id = %s",
            (task,),
        )
        task_row = cur.fetchone()
        cur.execute(
            "SELECT count(*) FROM retention_step_receipts WHERE task_id = %s",
            (task,),
        )
        receipt_count = cur.fetchone()[0]
    check("过期日志已全部删除", remaining_old == 0, remaining_old)
    check("未过期日志原样保留", remaining_total == total - SEED_OLD_ROWS,
          (remaining_total, total))
    check("任务进度持久化(4/130/完成)", task_row == (4, 130, True), task_row)
    check("每步一份持久化回执(3 份)", receipt_count == 3, receipt_count)


def case_retry_is_idempotent() -> None:
    seed_and_count()
    task = "task-retry"
    status, first = post("/retention/step", step_payload(task, 1))
    check("重试场景步骤1 成功", status == 200, first)
    check("首步删满 50 条", first.get("step_deleted") == STEP_LIMIT, first)

    status, retry = post("/retention/step", step_payload(task, 1))
    check("重试步骤1 仍 200", status == 200, retry)
    check("重试返回原回执(本步删除数一致)",
          retry.get("step_deleted") == first["step_deleted"], (first, retry))
    check("重试累计数不翻倍",
          retry.get("total_deleted") == first["total_deleted"], (first, retry))
    check("重试标记 retried=true", retry.get("retried") is True, retry)

    # Multiple retries keep returning the same receipt.
    status, retry2 = post("/retention/step", step_payload(task, 1))
    check("再次重试仍是原回执",
          status == 200 and retry2["total_deleted"] == first["total_deleted"]
          and retry2["retried"] is True, retry2)

    with admin_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT total_deleted, next_step_no FROM retention_tasks "
            "WHERE task_id = %s",
            (task,),
        )
        check("重试不推进步骤号/累计数", cur.fetchone() == (50, 2), )
        cur.execute(
            "SELECT count(*) FROM retention_step_receipts "
            "WHERE task_id = %s AND step_no = 1",
            (task,),
        )
        check("重试不新增回执行", cur.fetchone()[0] == 1)


def case_conflicts_delete_nothing() -> None:
    seed_and_count()
    task = "task-conflict"
    post("/retention/step", step_payload(task, 1))
    other_cutoff = CUTOFF + timedelta(days=1)

    with admin_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
                    (CUTOFF,))
        before = cur.fetchone()[0]

    # Different cutoff on the same task id.
    status, body = post(
        "/retention/step", step_payload(task, 2, cutoff=other_cutoff)
    )
    check("截止时间冲突返回 409", status == 409, body)
    check("错误码 cutoff_conflict",
          body.get("error", {}).get("code") == "cutoff_conflict", body)

    # Skipping step 2.
    status, body = post("/retention/step", step_payload(task, 3))
    check("跳步返回 409", status == 409, body)
    check("错误码 step_gap",
          body.get("error", {}).get("code") == "step_gap", body)

    # New task starting at step 2.
    status, body = post("/retention/step", step_payload("task-never-seen", 2))
    check("未知任务跳步返回 409", status == 409, body)
    check("未知任务错误码 step_gap",
          body.get("error", {}).get("code") == "step_gap", body)

    # step_no below 1 is a schema-level 422.
    status, body = post("/retention/step", step_payload(task, 0))
    check("step_no=0 返回 422", status == 422, body)

    # Naive datetime is rejected at validation.
    payload = step_payload(task, 2)
    payload["cutoff_at"] = "2026-11-10T00:00:00"
    status, body = post("/retention/step", payload)
    check("无时区截止时间返回 422", status == 422, body)

    with admin_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fault_logs WHERE occurred_at < %s",
                    (CUTOFF,))
        after = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM retention_tasks WHERE task_id = 'task-never-seen'"
        )
        never_created = cur.fetchone()[0]
    check("冲突请求未删除任何记录", after == before, (before, after))
    check("被拒任务未创建任务记录", never_created == 0)

    # Drive the task to completion, then prove later steps delete nothing.
    post("/retention/step", step_payload(task, 2))
    status, done = post("/retention/step", step_payload(task, 3))
    check("冲突场景任务最终完成", status == 200 and done["completed"] is True, done)

    # Retry of the final step returns the original receipt (idempotency
    # beats the completed-task rejection).
    status, retry_final = post("/retention/step", step_payload(task, 3))
    check("完成后重试末步返回原回执",
          status == 200 and retry_final["retried"] is True
          and retry_final["step_deleted"] == done["step_deleted"],
          retry_final)

    # A genuinely new step on the completed task is rejected.
    status, body = post("/retention/step", step_payload(task, 4))
    check("完成后新步骤返回 409", status == 409, body)
    check("错误码 task_completed",
          body.get("error", {}).get("code") == "task_completed", body)


def case_concurrent_duplicate_serialised() -> None:
    seed_and_count()
    task = "task-concurrent"
    results: list = []

    def fire() -> None:
        results.append(post("/retention/step", step_payload(task, 1)))

    threads = [threading.Thread(target=fire) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = sorted(s for s, _ in results)
    check("并发重复提交全部 200", statuses == [200] * 4, results)
    totals = {b["total_deleted"] for _, b in results}
    check("所有响应累计数一致(只删一次)", totals == {50}, results)
    check("仅一个首次提交、其余为重试",
          sum(1 for _, b in results if b["retried"] is False) == 1
          and sum(1 for _, b in results if b["retried"] is True) == 3,
          results)

    with admin_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT total_deleted, next_step_no FROM retention_tasks "
            "WHERE task_id = %s",
            (task,),
        )
        check("数据库侧只推进一次", cur.fetchone() == (50, 2))
        cur.execute(
            "SELECT count(*) FROM retention_step_receipts WHERE task_id = %s",
            (task,),
        )
        check("数据库侧仅一份回执", cur.fetchone()[0] == 1)


def case_queries_unaffected_during_cleanup() -> None:
    """Cleanup uses its own connection; /query keeps two working backends."""
    # Keep the cleanup busy for a while: a fresh task on a full window.
    seed_and_count()
    task = "task-during-query"
    stop = threading.Event()
    errors: list = []

    def cleanup_loop() -> None:
        step = 1
        while not stop.is_set():
            status, body = post(
                "/retention/step", step_payload(task, step), timeout=15
            )
            if status != 200:
                errors.append((step, status, body))
                return
            if body["completed"]:
                return
            step += 1
            time.sleep(0.05)

    worker = threading.Thread(target=cleanup_loop)
    worker.start()
    try:
        ok = 0
        t0 = time.monotonic()
        while time.monotonic() - t0 < 3.0:
            status, body = post(
                "/query",
                {"sql": "SELECT count(*) FROM fault_logs", "timeout_ms": 5000},
            )
            if status == 200 and body["status"] == "ok":
                ok += 1
            else:
                errors.append(("query", status, body))
            time.sleep(0.1)
    finally:
        stop.set()
        worker.join(timeout=15)

    check("清理期间只读查询持续可用", ok >= 20, f"ok={ok}")
    check("清理与查询均无错误", not errors, errors[:3])

    status, health = get("/health")
    check("只读池仍为两条连接", health["pool"]["size"] == 2, health)
    check(
        "维护连接独立于只读池(role=fault_maint)",
        health["maintenance"]["connected"] is True
        and health["maintenance"]["role"] == "fault_maint",
        health,
    )

    # Physical proof: the query pool never opens a third fault_ro backend.
    observed: list[int] = []
    barrier = threading.Barrier(2)

    def slow(i: int) -> None:
        barrier.wait()
        post("/query",
             {"sql": f"SELECT {i}, pg_sleep(1.5)", "timeout_ms": 8000},
             timeout=15)

    threads = [threading.Thread(target=slow, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 2.5
    with admin_connect() as conn, conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE usename='fault_ro'"
            )
            observed.append(cur.fetchone()[0])
            time.sleep(0.1)
    for t in threads:
        t.join()
    check("清理期间 fault_ro 后端数从未超过 2",
          observed and max(observed) <= 2, f"max={max(observed)}")


def main() -> int:
    print(f"retention self-test against {BASE}")
    run_case("R1. 健康检查包含独立维护连接", case_health_reports_maintenance)
    run_case("R2. 完整任务：每步 50 条直至完成", case_happy_path_and_cap)
    run_case("R3. 重试幂等：返回原回执、不重复删除", case_retry_is_idempotent)
    run_case("R4. 截止冲突/跳步/完成后提交均不删除", case_conflicts_delete_nothing)
    run_case("R5. 并发重复提交被串行化为一次删除",
             case_concurrent_duplicate_serialised)
    run_case("R6. 清理期间只读查询持续可用且容量仍为 2",
             case_queries_unaffected_during_cleanup)
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
