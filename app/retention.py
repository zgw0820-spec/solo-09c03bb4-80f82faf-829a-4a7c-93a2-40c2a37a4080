"""Retention cleanup on a **dedicated** maintenance connection.

The read-only query service keeps exactly two ``fault_ro`` backends (see
:mod:`app.pool`).  Deleting expired ``fault_logs`` needs a writable role and
must never starve the on-duty analysts, so this module owns one *separate*
``fault_maint`` connection, created independently of the query pool.

Durability protocol (all state lives in PostgreSQL, never in process memory):

* ``retention_tasks`` records each task id with its fixed cutoff, the next
  expected step number, cumulative deletions and completion flag.
* ``retention_step_receipts`` stores the immutable receipt of every accepted
  step, so a retried (possibly duplicated) request returns the original
  receipt instead of deleting more rows.
* Each step runs inside a single transaction: the row lock on the task
  serialises concurrent submissions, the DELETE, progress upsert and receipt
  insert either all commit together or all roll back together.  A request
  lost before commit therefore leaves no trace and can simply be resubmitted;
  after a service restart the task continues from the persisted state.

Conflict rules (rejected before any deletion, HTTP 409/422):

* unknown task + step > 1           -> ``step_gap`` (no implicit skipping);
* known task, different cutoff      -> ``cutoff_conflict``;
* step below the next expected one  -> ``step_gap``;
* task already complete             -> ``task_completed``.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime
from typing import Any

import psycopg

from .config import Settings
from .errors import (
    RetentionConflictError,
    RetentionCutoffConflict,
    RetentionStepGap,
    RetentionTaskCompleted,
    RetentionTaskIdError,
)

logger = logging.getLogger("retention")

_DDL = """
CREATE TABLE IF NOT EXISTS retention_tasks (
    task_id          TEXT PRIMARY KEY,
    cutoff_at        TIMESTAMPTZ NOT NULL,
    next_step_no     INTEGER NOT NULL DEFAULT 1,
    total_deleted    BIGINT NOT NULL DEFAULT 0,
    completed        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS retention_step_receipts (
    task_id          TEXT NOT NULL,
    step_no          INTEGER NOT NULL,
    cutoff_at        TIMESTAMPTZ NOT NULL,
    step_deleted     BIGINT NOT NULL,
    total_deleted    BIGINT NOT NULL,
    completed        BOOLEAN NOT NULL,
    retried          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, step_no),
    FOREIGN KEY (task_id) REFERENCES retention_tasks(task_id)
);
"""


class MaintenanceConnection:
    """One writable connection reserved for retention maintenance work."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._conn: psycopg.AsyncConnection | None = None
        # Serialise submissions inside the process as well; row locks in the
        # database make cross-process concurrency safe too.
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Open the maintenance connection and ensure state tables exist."""
        conn = await self._connect()
        try:
            await conn.execute(_DDL)
            await conn.commit()
        except Exception:
            await self._safe_close(conn)
            raise
        self._conn = conn
        logger.info("maintenance connection opened; retention tables ready")

    async def close(self) -> None:
        if self._conn is not None:
            await self._safe_close(self._conn)
            self._conn = None
            logger.info("maintenance connection closed")

    async def status(self) -> dict[str, Any]:
        conn = self._conn
        return {
            "connected": conn is not None and not conn.closed,
            "role": "fault_maint",
        }

    async def _connect(self) -> psycopg.AsyncConnection:
        # Autocommit at session level; each step opens an explicit
        # transaction so its DELETE + progress + receipt commit atomically
        # and any error rolls the whole step back.
        conn = await psycopg.AsyncConnection.connect(
            self._settings.maintenance_dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
        )
        return conn

    @staticmethod
    async def _safe_close(conn: psycopg.AsyncConnection | None) -> None:
        if conn is None:
            return
        with contextlib.suppress(Exception):
            await conn.close()

    async def _healthy(self) -> bool:
        return self._conn is not None and not self._conn.closed

    async def _ensure_connection(self) -> psycopg.AsyncConnection:
        """Lazily (re)connect; state is durable so no task progress is lost."""
        if not await self._healthy():
            await self._reconnect()
        else:
            # ``closed`` stays False right after pg_terminate_backend until
            # the next command fails.  Prove liveness cheaply so a request
            # never observes the dead backend.
            try:
                await self._conn.execute("SELECT 1")
            except psycopg.Error:
                logger.warning("maintenance backend failed liveness check; "
                               "reconnecting")
                await self._reconnect()
        assert self._conn is not None
        return self._conn

    async def _reconnect(self) -> None:
        old = self._conn
        self._conn = None
        await self._safe_close(old)
        conn = await self._connect()
        await conn.execute(_DDL)
        await conn.commit()
        self._conn = conn
        logger.info("maintenance connection (re)established")

    # ------------------------------------------------------------------ #
    # Retention step protocol
    # ------------------------------------------------------------------ #
    async def run_step(
        self, task_id: str, cutoff_at: datetime, step_no: int
    ) -> dict[str, Any]:
        if not isinstance(task_id, str) or not task_id.strip():
            raise RetentionTaskIdError("task_id 不能为空")
        if len(task_id) > 200:
            raise RetentionTaskIdError("task_id 长度不能超过 200")
        if step_no < 1:
            raise RetentionStepGap("step_no 必须从 1 开始")
        if cutoff_at.tzinfo is None:
            raise RetentionTaskIdError("cutoff_at 必须携带时区信息")

        async with self._lock:
            return await self._run_step_locked(task_id, cutoff_at, step_no)

    async def _run_step_locked(
        self, task_id: str, cutoff_at: datetime, step_no: int
    ) -> dict[str, Any]:
        conn = await self._ensure_connection()
        limit = self._settings.retention_step_limit
        try:
            return await self._transaction(
                conn, task_id, cutoff_at, step_no, limit
            )
        except RetentionConflictError:
            # Expected protocol-level rejection (gap, cutoff mismatch,
            # completed task): the explicit transaction has rolled back
            # already; let the global handler build the 409/422 receipt.
            raise
        except Exception:
            # Failure must never advance progress: discard the backend and
            # reconnect on the next request.  The transaction context has
            # already rolled back every statement of this step.
            self._conn = None
            await self._safe_close(conn)
            logger.exception("retention step failed; transaction rolled back")
            raise

    async def _transaction(
        self,
        conn: psycopg.AsyncConnection,
        task_id: str,
        cutoff_at: datetime,
        step_no: int,
        limit: int,
    ) -> dict[str, Any]:
        # One explicit transaction for the whole step; the task row lock is
        # held until COMMIT, serialising concurrent submissions.
        async with conn.transaction(), conn.cursor() as cur:
            # 1) Lock/create the task row so concurrent steps serialise and
            #    no two requests can observe the same next_step_no.
            await cur.execute(
                "SELECT task_id, cutoff_at, next_step_no, total_deleted, "
                "completed FROM retention_tasks WHERE task_id = %s FOR UPDATE",
                (task_id,),
            )
            row = await cur.fetchone()

            if row is None:
                # First contact must be step 1; anything else is a gap and
                # must not create state nor delete rows.
                if step_no != 1:
                    raise RetentionStepGap(
                        f"任务 {task_id!r} 不存在，首个步骤必须为 1，"
                        f"收到 step_no={step_no}（疑似跳步）"
                    )
                await cur.execute(
                    "INSERT INTO retention_tasks (task_id, cutoff_at) "
                    "VALUES (%s, %s)",
                    (task_id, cutoff_at),
                )
                existing_cutoff = cutoff_at
                next_step = 1
                total_deleted = 0
                completed = False
            else:
                _, existing_cutoff, next_step, total_deleted, completed = row
                # The cutoff is fixed for the lifetime of the task.  Checked
                # before everything else so a tampered retry cannot reuse an
                # old receipt under a different cutoff.
                if existing_cutoff != cutoff_at:
                    raise RetentionCutoffConflict(
                        "同一任务的截止时间必须保持一致："
                        f"已登记 {existing_cutoff.isoformat()}，"
                        f"本次提交 {cutoff_at.isoformat()}"
                    )

            # 2) Idempotent retry: a receipt for this step means it already
            #    committed.  The original receipt wins (even for the final
            #    step of a completed task) and nothing is deleted again.
            await cur.execute(
                "SELECT step_deleted, total_deleted, completed, retried "
                "FROM retention_step_receipts "
                "WHERE task_id = %s AND step_no = %s",
                (task_id, step_no),
            )
            receipt_row = await cur.fetchone()
            if receipt_row is not None:
                step_deleted, new_total, done, retried = receipt_row
                if not retried:
                    await cur.execute(
                        "UPDATE retention_step_receipts SET retried = TRUE "
                        "WHERE task_id = %s AND step_no = %s",
                        (task_id, step_no),
                    )
                # Exiting the transaction context commits; no statements ran
                # beyond reads plus the retried flag update.
                return self._receipt(
                    task_id, step_no, existing_cutoff, step_deleted,
                    new_total, done, retried=True,
                )

            # No receipt: this is a genuinely new step.  New steps on a
            # completed task or out-of-order steps are rejected before any
            # deletion.
            if completed:
                raise RetentionTaskCompleted(
                    f"任务 {task_id!r} 已完成，后续步骤不再删除任何记录"
                )
            if step_no != next_step:
                raise RetentionStepGap(
                    f"步骤必须按序提交：期望 step_no={next_step}，"
                    f"收到 {step_no}"
                )

            # 3) Delete at most `limit` rows strictly older than the cutoff.
            #    The ctid self-join bounds the DELETE without needing DELETE
            #    privilege on any extra relation.
            await cur.execute(
                "DELETE FROM fault_logs WHERE ctid IN ("
                "SELECT ctid FROM fault_logs "
                "WHERE occurred_at < %s ORDER BY id LIMIT %s"
                ")",
                (cutoff_at, limit),
            )
            step_deleted = cur.rowcount or 0
            new_total = total_deleted + step_deleted
            # Fewer rows than the batch size means no more eligible rows
            # remain: the cleanup is complete.
            done = step_deleted < limit

            # 4) Persist progress and the receipt in the SAME transaction.
            await cur.execute(
                "UPDATE retention_tasks SET "
                "next_step_no = %s, total_deleted = %s, completed = %s, "
                "updated_at = now() WHERE task_id = %s",
                (step_no + 1, new_total, done, task_id),
            )
            await cur.execute(
                "INSERT INTO retention_step_receipts "
                "(task_id, step_no, cutoff_at, step_deleted, total_deleted, "
                "completed, retried) VALUES (%s, %s, %s, %s, %s, %s, FALSE)",
                (task_id, step_no, existing_cutoff, step_deleted,
                 new_total, done),
            )
            logger.info(
                "retention step committed task=%s step=%d deleted=%d "
                "total=%d completed=%s",
                task_id, step_no, step_deleted, new_total, done,
            )
            return self._receipt(
                task_id, step_no, existing_cutoff, step_deleted,
                new_total, done, retried=False,
            )

    @staticmethod
    def _receipt(
        task_id: str,
        step_no: int,
        cutoff_at: datetime,
        step_deleted: int,
        total_deleted: int,
        completed: bool,
        retried: bool,
    ) -> dict[str, Any]:
        return {
            "task_id": task_id,
            "step_no": step_no,
            "cutoff_at": cutoff_at,
            "step_deleted": step_deleted,
            "total_deleted": total_deleted,
            "completed": completed,
            "retried": retried,
        }
