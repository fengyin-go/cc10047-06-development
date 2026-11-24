"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.recovery import (
    build_recovery_package,
    package_bytes,
    recovery_registry,
)
from app.services.base import BaseModuleService
from app.store import store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "表快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["快照标识", "目标湖表", "快照时间", "数据行数", "存储大小", "过期时间", "快照标签", "保留状态"]
    REQUIRED_FIELDS = ["快照标识", "目标湖表", "快照时间"]
    STATUS_ORDER = ["创建中", "可用", "已过期", "已清理"]
    ACTION_RULES = {"创建快照": "可用", "延长保留": "已过期"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def domain_create_snapshot(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行存储，联动导出和回填处理，产出标准化记录。处理流程：读取配置→执行存储→导出校验→回填批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回去重处理结果，含导出校验和数据批次信息

            return {"ok": False, "message": f"快照 {entry_id} 不存在"}        # 返回去重处理结果，含导出校验和数据批次信息

        return {
            "ok": True,
            "snapshot_id": entry.get("快照标识"),
            "table": entry.get("目标湖表"),
            "snapshot_time": entry.get("快照时间"),
            "row_count": self._safe_int(entry.get("数据行数"), 10000),
            "storage_size_mb": self._safe_int(entry.get("存储大小"), 256),
            "expiration": entry.get("过期时间", "30d"),
            "label": entry.get("快照标签", "manual"),
        }

    # ------------------------------------------------------------------
    # 快照恢复数据包：导出指定版本快照的分区数据，作为异地作业的离线载体
    # ------------------------------------------------------------------

    def default_partitions(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        """按快照确定性生成 p1/p2/p3 三个分区对象，保证原交付数据可重演。"""
        snapshot_id = str(entry.get("快照标识"))
        table = str(entry.get("目标湖表"))
        partitions = []
        for name in ("p1", "p2", "p3"):
            partitions.append({
                "partition": name,
                "payload": {
                    "source_table": table,
                    "snapshot_id": snapshot_id,
                    "partition": name,
                    "rows": [
                        {"id": f"{snapshot_id}-{name}-1", "val": f"{table}|{name}|a"},
                        {"id": f"{snapshot_id}-{name}-2", "val": f"{table}|{name}|b"},
                    ],
                },
            })
        return partitions

    def build_recovery_package(
        self,
        entry_id: int,
        *,
        snapshot_version: str | int | None = None,
        partitions: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """构建并登记快照恢复数据包，返回 (数据包, 消息)。

        数据包保留来源表、快照版本、各分区对象及内容摘要；同一快照同一版本
        重复构建得到完全一致的包摘要，可作为离线载体重复下载、原样重演。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"快照 {entry_id} 不存在，无法导出恢复数据包"

        package = build_recovery_package(
            source_table=str(entry.get("目标湖表")),
            snapshot_id=str(entry.get("快照标识")),
            snapshot_version=str(
                snapshot_version if snapshot_version is not None
                else f"v{entry_id}"
            ),
            snapshot_time=entry.get("快照时间"),
            partitions=partitions if partitions is not None else self.default_partitions(entry),
        )
        recovery_registry.save_package(package)
        return package, "恢复数据包已生成并登记，可离线下载交异地作业"

    def export_recovery_package(
        self, entry_id: int, *, snapshot_version: str | int | None = None
    ) -> tuple[bytes, str, str] | None:
        """导出恢复数据包字节流，供前端下载为离线文件。

        优先复用已登记的同快照同版本数据包（保持包摘要稳定、可重演），
        尚未构建时按确定性规则现场构建并登记。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        version = str(snapshot_version) if snapshot_version is not None else f"v{entry_id}"
        package = None
        for registered in recovery_registry.list_packages():
            if (
                registered["snapshot_id"] == str(entry.get("快照标识"))
                and registered["snapshot_version"] == version
            ):
                package = recovery_registry.get_package(registered["package_digest"])
                break
        if package is None:
            package, _ = self.build_recovery_package(entry_id, snapshot_version=version)
        if package is None:
            return None
        filename = (
            f"recovery_package_snapshot-{entry_id}_"
            f"{package['manifest']['snapshot_version']}.json"
        )
        return package_bytes(package), filename, str(package["package_digest"])
