"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class RecoveryPackagePayload(BaseModel):
    snapshot_version: str | int | None = None
    partitions: list[dict[str, Any]] = Field(default_factory=list)


class RecoveryStartPayload(BaseModel):
    publish_boundary: str
    package: dict[str, Any] | None = None
    package_digest: str | None = None
    preexisting_partitions: list[str] = Field(default_factory=list)


class RecoveryResendPayload(BaseModel):
    job_id: str
    partition: str
    obj: dict[str, Any]
