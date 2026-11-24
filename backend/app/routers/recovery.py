"""快照恢复数据包与异地恢复接口。

快照管理侧负责构建/下载恢复数据包（离线载体），表目录同步侧负责读取
数据包并发起带回执的异地恢复作业、补送修复对象以及查询可读分区。
"""
from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

from app.recovery import VALID_BOUNDARIES, recovery_registry
from app.schemas import (
    RecoveryPackagePayload,
    RecoveryResendPayload,
    RecoveryStartPayload,
)
from app.services import service_for

router = APIRouter(prefix="/api/recovery", tags=["快照异地恢复"])


def snapshot_service():
    service = service_for("snapshot_mgr")
    if service is None:
        raise HTTPException(404, "snapshot_mgr 服务未注册")
    return service


def catalog_service():
    service = service_for("table_catalog")
    if service is None:
        raise HTTPException(404, "table_catalog 服务未注册")
    return service


# ----------------------------------------------------------------------
# 快照管理：恢复数据包导出下载
# ----------------------------------------------------------------------

@router.post("/snapshots/{snapshot_id}/packages")
def build_package(snapshot_id: int, payload: RecoveryPackagePayload) -> dict[str, Any]:
    """构建指定版本快照的恢复数据包并登记，返回包清单与内容摘要。"""
    service = snapshot_service()
    partitions = payload.partitions or None
    package, message = service.build_recovery_package(
        snapshot_id,
        snapshot_version=payload.snapshot_version,
        partitions=partitions,
    )
    if package is None:
        raise HTTPException(status_code=404, detail=message)
    return {
        "ok": True,
        "message": message,
        "package_digest": package["package_digest"],
        "manifest": package["manifest"],
        "object_count": len(package["objects"]),
    }


@router.get("/snapshots/{snapshot_id}/packages/download")
def download_package(snapshot_id: int, snapshot_version: str | None = None) -> Response:
    """导出下载恢复数据包，作为异地作业恢复的离线载体。"""
    service = snapshot_service()
    exported = service.export_recovery_package(
        snapshot_id, snapshot_version=snapshot_version
    )
    if exported is None:
        raise HTTPException(status_code=404, detail=f"快照 {snapshot_id} 不存在")
    content, filename, digest = exported
    ascii_fallback = filename.encode("ascii", "ignore").decode() or "recovery_package.json"
    return Response(
        content=content,
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": (
                f"attachment; filename=\"{ascii_fallback}\"; "
                f"filename*=UTF-8''{quote(filename)}"
            ),
            "X-Package-Digest": digest,
        },
    )


@router.get("/packages")
def list_packages() -> dict[str, Any]:
    """列出已登记、可作为离线载体取用的恢复数据包。"""
    return {"ok": True, "items": recovery_registry.list_packages()}


# ----------------------------------------------------------------------
# 表目录同步：异地恢复作业（带回执）
# ----------------------------------------------------------------------

@router.post("/catalogs/{catalog_id}/jobs")
def start_recovery(catalog_id: int, payload: RecoveryStartPayload) -> dict[str, Any]:
    """在目标目录发起异地恢复，按选定发布边界创建可核验分区并回执。"""
    if payload.publish_boundary not in VALID_BOUNDARIES:
        raise HTTPException(
            status_code=400,
            detail=f"发布边界仅支持 {list(VALID_BOUNDARIES)}",
        )
    service = catalog_service()
    receipt = service.start_disaster_recovery(
        catalog_id,
        publish_boundary=payload.publish_boundary,
        package=payload.package,
        package_digest=payload.package_digest,
        preexisting_partitions=payload.preexisting_partitions,
    )
    if not receipt.get("ok", False) and "job_id" not in receipt:
        # 入参类错误（记录/包不存在），交由 404 表达；恢复本身失败仍返回回执。
        raise HTTPException(status_code=404, detail=receipt.get("message"))
    return receipt


@router.post("/catalogs/{catalog_id}/jobs/resend")
def resend_object(catalog_id: int, payload: RecoveryResendPayload) -> dict[str, Any]:
    """补送修复后的分区对象；重复提交同一分区对象只生效一次。"""
    service = catalog_service()
    receipt = service.resend_recovery_object(
        catalog_id,
        job_id=payload.job_id,
        partition=payload.partition,
        obj=payload.obj,
    )
    if not receipt.get("ok", False) and "job_id" not in receipt:
        raise HTTPException(status_code=404, detail=receipt.get("message"))
    return receipt


@router.get("/catalogs/{catalog_id}/jobs")
def list_jobs(catalog_id: int) -> dict[str, Any]:
    """查看目标目录下的恢复作业及各自生效/待补分区。"""
    service = catalog_service()
    return {"ok": True, "items": service.list_recovery_jobs()}


@router.get("/catalogs/{catalog_id}/jobs/{job_id}")
def get_job(catalog_id: int, job_id: str) -> dict[str, Any]:
    service = catalog_service()
    receipt = service.get_recovery_job(job_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"恢复作业 {job_id} 不存在")
    return receipt


@router.get("/catalogs/{catalog_id}/partitions")
def readable_partitions(catalog_id: int) -> dict[str, Any]:
    """重开目标表时核对当前实际可读的分区（含目的表原有无关分区）。"""
    service = catalog_service()
    return service.readable_partitions(catalog_id)
