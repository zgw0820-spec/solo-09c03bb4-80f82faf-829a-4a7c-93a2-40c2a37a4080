"""Typed application errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class QueryServiceError(Exception):
    """Base class for errors that carry a stable machine-readable code."""

    code = "query_service_error"
    http_status = 500

    def __init__(self, message: str = "", extra: dict | None = None) -> None:
        super().__init__(message)
        self.extra = extra or {}


class InvalidSqlError(QueryServiceError):
    code = "invalid_sql"
    http_status = 400


class InvalidDeadlineError(QueryServiceError):
    code = "invalid_deadline"
    http_status = 422


class PoolAcquireTimeout(QueryServiceError):
    """Raised when no connection became available within the total deadline."""

    code = "acquire_timeout"
    http_status = 503


class QueryTimeout(QueryServiceError):
    """Raised when the statement did not finish before the total deadline."""

    code = "query_timeout"
    http_status = 504


class RetentionConflictError(QueryServiceError):
    """Base class for rejected retention steps (nothing is deleted)."""

    code = "retention_conflict"
    http_status = 409


class RetentionTaskIdError(RetentionConflictError):
    """Task id is missing or malformed."""

    code = "invalid_task_id"
    http_status = 422


class RetentionCutoffConflict(RetentionConflictError):
    """A task already exists with a different fixed cutoff."""

    code = "cutoff_conflict"
    http_status = 409


class RetentionStepGap(RetentionConflictError):
    """Submitted step number is not the next expected one (skipping)."""

    code = "step_gap"
    http_status = 409


class RetentionTaskCompleted(RetentionConflictError):
    """Task already finished; later steps must not delete anything."""

    code = "task_completed"
    http_status = 409
