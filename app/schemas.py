"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，包含排队等连接的时间"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok", "timeout"]
    executed: bool = Field(
        description="语句是否真正发送到了数据库后端"
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    late_result_ignored: bool | None = None
    detail: str | None = None


class ErrorResponse(BaseModel):
    error: dict[str, str]


class RetentionStepRequest(BaseModel):
    task_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="清理任务 ID；同一任务的所有步骤共用同一 ID 且截止时间固定",
        examples=["cleanup-2026-11"],
    )
    cutoff_at: datetime = Field(
        ...,
        description=(
            "固定截止时间（ISO 8601、必须带时区）；仅删除 occurred_at 早于"
            "该时间的故障日志，同一任务重试时必须原样回传"
        ),
        examples=["2026-11-18T00:00:00Z"],
    )
    step_no: int = Field(
        ...,
        ge=1,
        description="步骤号，从 1 开始，同一任务必须按序提交、不得跳步",
        examples=[1],
    )

    @field_validator("cutoff_at")
    @classmethod
    def _cutoff_must_be_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("cutoff_at 必须携带时区信息（例如 2026-11-18T00:00:00Z）")
        return value


class RetentionStepResponse(BaseModel):
    task_id: str = Field(description="回显任务 ID")
    step_no: int = Field(description="本步步骤号（从 1 开始）")
    cutoff_at: datetime = Field(description="任务固定截止时间")
    step_deleted: int = Field(description="本步删除的故障日志条数（上限 50）")
    total_deleted: int = Field(description="该任务累计删除条数（含本步）")
    completed: bool = Field(
        description="是否已清理完毕：本步删除数不足 50 表示没有更多符合条件的记录"
    )
    retried: bool = Field(
        description="True 表示该步骤此前已提交，本次为重试，返回原始回执且未再删除"
    )


class HealthResponse(BaseModel):
    status: Literal["healthy"]
    pool: dict[str, Any]
    maintenance: dict[str, Any] | None = None
