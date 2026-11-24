"""快照恢复数据包与异地恢复引擎。

本模块只依赖标准库，负责把指定版本快照的分区数据封装为可离线搬运的
恢复数据包，并在目标目录侧驱动一次带回执的恢复作业：

* 数据包保留来源表、快照版本、各分区对象以及每个对象的内容摘要（SHA-256），
  包整体也带一份摘要，原交付数据可以原样重演（构建过程确定、可重复）。
* 恢复作业支持两种发布边界：
  - ``partition`` 逐分区发布：某个对象核验通过即发布为可读分区；
  - ``batch``     整批确认：所有预期分区全部核验通过后才一起发布。
* 任何未通过核验的对象都不能单独发布，回执也不会把未生效范围记为成功；
  异常状态会回写待办并注明尚未生效的对象。
* 同一个分区对象重复补送只生效一次（写入幂等），目的表原有的无关分区
  继续可读，恢复作业不会触碰它们。
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

PACKAGE_KIND = "snapshot-recovery-package"
PACKAGE_VERSION = "1.0"
DIGEST_ALGO = "sha256"
BOUNDARY_PARTITION = "partition"
BOUNDARY_BATCH = "batch"
VALID_BOUNDARIES = (BOUNDARY_PARTITION, BOUNDARY_BATCH)

# 恢复作业 / 分区对象状态
OBJ_VERIFIED = "已核验"
OBJ_MISMATCH = "摘要不符"
OBJ_PUBLISHED = "已发布"
JOB_RUNNING = "待补齐"
JOB_PARTIAL = "部分生效"
JOB_COMPLETED = "已完成"


# ----------------------------------------------------------------------
# 摘要与确定性序列化
# ----------------------------------------------------------------------

def canonical_json(value: Any) -> str:
    """生成确定性 JSON 文本：键排序、无多余空白，保证同一数据摘要可重演。"""
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def digest_payload(payload: Any) -> str:
    """对分区对象的数据载荷计算内容摘要。"""
    return sha256_text(canonical_json(payload))


# ----------------------------------------------------------------------
# 数据包构建与校验
# ----------------------------------------------------------------------

def build_partition_object(
    partition: str, payload: Any, *, digest: str | None = None
) -> dict[str, Any]:
    """构造单个分区对象，缺省时由载荷计算内容摘要。"""
    actual_digest = digest if digest is not None else digest_payload(payload)
    return {
        "partition": str(partition),
        "digest_algo": DIGEST_ALGO,
        "digest": actual_digest,
        "bytes": len(canonical_json(payload).encode("utf-8")),
        "payload": copy.deepcopy(payload),
    }


def build_recovery_package(
    *,
    source_table: str,
    snapshot_id: str,
    snapshot_version: str | int,
    partitions: list[dict[str, Any]],
    snapshot_time: str | None = None,
) -> dict[str, Any]:
    """构建快照恢复数据包。

    ``partitions`` 每项形如 ``{"partition": "p1", "payload": [...]}``，
    也允许直接携带 ``digest``（用于测试中构造摘要不符的损坏包）。
    构建过程确定性：同一输入永远产出同一份包摘要，原交付数据可重演。
    """
    objects: list[dict[str, Any]] = []
    for item in partitions:
        objects.append(
            build_partition_object(
                item["partition"],
                item.get("payload", []),
                digest=item.get("digest"),
            )
        )
    objects.sort(key=lambda obj: obj["partition"])

    manifest = {
        "kind": PACKAGE_KIND,
        "package_version": PACKAGE_VERSION,
        "source_table": str(source_table),
        "snapshot_id": str(snapshot_id),
        "snapshot_version": str(snapshot_version),
        "snapshot_time": snapshot_time,
        "digest_algo": DIGEST_ALGO,
        "partitions": [
            {
                "partition": obj["partition"],
                "digest_algo": obj["digest_algo"],
                "digest": obj["digest"],
                "bytes": obj["bytes"],
            }
            for obj in objects
        ],
    }
    package = {
        "manifest": manifest,
        "objects": objects,
    }
    # 包摘要覆盖清单与全部对象载荷，离线载体可据此整体核验。
    package["package_digest"] = sha256_text(canonical_json(package))
    return package


def package_bytes(package: dict[str, Any]) -> bytes:
    """把数据包序列化为可下载/离线搬运的字节流（UTF-8、带缩进可读）。"""
    return json.dumps(package, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")


def verify_object(obj: dict[str, Any]) -> dict[str, Any]:
    """核验单个分区对象：重新计算载荷摘要并与包内摘要比对。"""
    expected = str(obj.get("digest", ""))
    actual = digest_payload(obj.get("payload"))
    return {
        "partition": obj.get("partition"),
        "expected_digest": expected,
        "actual_digest": actual,
        "ok": expected == actual,
    }


def verify_package(package: dict[str, Any]) -> dict[str, Any]:
    """核验整个数据包，返回包摘要与逐对象核验结果，不修改入参。"""
    material = {
        "manifest": package.get("manifest"),
        "objects": package.get("objects", []),
    }
    recomputed = sha256_text(canonical_json(material))
    package_ok = recomputed == package.get("package_digest")
    objects = [verify_object(obj) for obj in package.get("objects", [])]
    return {
        "package_digest": package.get("package_digest"),
        "recomputed_digest": recomputed,
        "package_ok": package_ok,
        "objects": objects,
        "all_objects_ok": all(item["ok"] for item in objects),
    }


# ----------------------------------------------------------------------
# 目标表与恢复作业
# ----------------------------------------------------------------------

@dataclass
class TargetTable:
    """目标目录中的一张表，维护当前可读分区（分区名 -> 发布来源）。"""

    catalog_id: str
    table_name: str
    readable_partitions: dict[str, dict[str, Any]] = field(default_factory=dict)

    def ensure_partition(self, partition: str, *, source: str, snapshot_id: str,
                         digest: str) -> bool:
        """幂等发布一个分区，返回本次是否真正写入（重复对象返回 False）。"""
        if partition in self.readable_partitions:
            return False
        self.readable_partitions[partition] = {
            "partition": partition,
            "source": source,
            "snapshot_id": snapshot_id,
            "digest": digest,
            "published": True,
        }
        return True

    def list_partitions(self) -> list[dict[str, Any]]:
        return [
            dict(meta)
            for _, meta in sorted(self.readable_partitions.items())
        ]


@dataclass
class RecoveryJob:
    job_id: str
    catalog_id: str
    target_table: str
    package_digest: str
    source_table: str
    snapshot_id: str
    snapshot_version: str
    publish_boundary: str
    expected_partitions: list[str]
    # 分区名 -> 状态记录
    object_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    status: str = JOB_RUNNING
    receipts: list[dict[str, Any]] = field(default_factory=list)

    def pending_objects(self) -> list[str]:
        """尚未生效（未发布）的预期分区，即待办。"""
        return [
            name for name in self.expected_partitions
            if not self.object_states.get(name, {}).get("published", False)
        ]

    def unverified_objects(self) -> list[str]:
        """仍未通过核验（摘要不符）的预期分区；整批发布只等这一项清零。"""
        return [
            name for name in self.expected_partitions
            if self.object_states.get(name, {}).get("state") == OBJ_MISMATCH
        ]

    def effective_partitions(self) -> list[str]:
        return [
            name for name in self.expected_partitions
            if self.object_states.get(name, {}).get("published", False)
        ]

    def receipt(self, *, phase: str, note: str = "") -> dict[str, Any]:
        """生成带回执的恢复结果；成功范围只包含实际生效对象。"""
        pending = self.pending_objects()
        effective = self.effective_partitions()
        ok = not pending
        return {
            "ok": ok,
            "job_id": self.job_id,
            "phase": phase,
            "catalog_id": self.catalog_id,
            "target_table": self.target_table,
            "source_table": self.source_table,
            "snapshot_id": self.snapshot_id,
            "snapshot_version": self.snapshot_version,
            "publish_boundary": self.publish_boundary,
            "package_digest": self.package_digest,
            "expected_partitions": list(self.expected_partitions),
            "effective_partitions": effective,
            "pending_partitions": pending,
            "object_states": copy.deepcopy(self.object_states),
            "job_status": self.status,
            "note": note or (
                "全部预期分区已核验并生效" if ok
                else f"以下对象尚未生效：{('、'.join(pending)) or '无'}"
            ),
        }


class RecoveryRegistry:
    """内存态恢复登记处：保存导出的数据包、目标表和恢复作业。"""

    def __init__(self) -> None:
        self._packages: dict[str, dict[str, Any]] = {}
        self._tables: dict[str, TargetTable] = {}
        self._jobs: dict[str, RecoveryJob] = {}
        self._job_seq = 0

    def reset(self) -> None:
        """清空作业与目标表状态；已导出的离线数据包默认保留。"""
        self.reset_runtime()

    def reset_runtime(self) -> None:
        """只清空恢复作业与目标表，保留已登记的恢复数据包。"""
        self._tables.clear()
        self._jobs.clear()
        self._job_seq = 0

    def reset_all(self) -> None:
        """连同离线数据包一起清空（进程级彻底复位）。"""
        self._packages.clear()
        self.reset_runtime()

    # ----- 数据包 -----
    def save_package(self, package: dict[str, Any]) -> str:
        digest = str(package["package_digest"])
        self._packages[digest] = copy.deepcopy(package)
        return digest

    def get_package(self, package_digest: str) -> dict[str, Any] | None:
        package = self._packages.get(package_digest)
        return copy.deepcopy(package) if package is not None else None

    def list_packages(self) -> list[dict[str, Any]]:
        return [
            {
                "package_digest": digest,
                "source_table": pkg["manifest"]["source_table"],
                "snapshot_id": pkg["manifest"]["snapshot_id"],
                "snapshot_version": pkg["manifest"]["snapshot_version"],
                "partitions": [obj["partition"] for obj in pkg["objects"]],
            }
            for digest, pkg in sorted(self._packages.items())
        ]

    # ----- 目标表 -----
    def ensure_table(self, catalog_id: str, table_name: str) -> TargetTable:
        key = f"{catalog_id}::{table_name}"
        table = self._tables.get(key)
        if table is None:
            table = TargetTable(catalog_id=catalog_id, table_name=table_name)
            self._tables[key] = table
        return table

    def seed_partition(self, catalog_id: str, table_name: str, partition: str,
                       *, digest: str | None = None) -> None:
        """预置目的表原有分区（与恢复无关，恢复过程必须保持其可读）。"""
        table = self.ensure_table(catalog_id, table_name)
        table.ensure_partition(
            partition,
            source="preexisting",
            snapshot_id="preexisting",
            digest=digest or sha256_text(f"{catalog_id}:{table_name}:{partition}"),
        )

    def readable_partitions(self, catalog_id: str, table_name: str) -> list[dict[str, Any]]:
        key = f"{catalog_id}::{table_name}"
        table = self._tables.get(key)
        return table.list_partitions() if table is not None else []

    # ----- 恢复作业 -----
    def _next_job_id(self) -> str:
        self._job_seq += 1
        return f"RJR-{self._job_seq:04d}"

    def start_recovery(
        self,
        *,
        catalog_id: str,
        target_table: str,
        package: dict[str, Any],
        publish_boundary: str,
    ) -> tuple[RecoveryJob, dict[str, Any]]:
        """从一次带回执的目标恢复作业创建实际可核验的分区。"""
        if publish_boundary not in VALID_BOUNDARIES:
            raise ValueError(
                f"发布边界仅支持 {VALID_BOUNDARIES}，收到：{publish_boundary}"
            )
        verification = verify_package(package)
        manifest = package["manifest"]
        expected = [obj["partition"] for obj in package["objects"]]

        table = self.ensure_table(catalog_id, target_table)
        job = RecoveryJob(
            job_id=self._next_job_id(),
            catalog_id=catalog_id,
            target_table=target_table,
            package_digest=package["package_digest"],
            source_table=manifest["source_table"],
            snapshot_id=manifest["snapshot_id"],
            snapshot_version=manifest["snapshot_version"],
            publish_boundary=publish_boundary,
            expected_partitions=expected,
        )

        for obj in package["objects"]:
            result = next(
                item for item in verification["objects"]
                if item["partition"] == obj["partition"]
            )
            if not result["ok"]:
                # 未通过核验的对象：记录待办，绝不发布。
                job.object_states[obj["partition"]] = {
                    "state": OBJ_MISMATCH,
                    "expected_digest": result["expected_digest"],
                    "actual_digest": result["actual_digest"],
                    "published": False,
                    "write_count": 0,
                }
                continue
            job.object_states[obj["partition"]] = {
                "state": OBJ_VERIFIED,
                "digest": result["actual_digest"],
                "published": False,
                "write_count": 0,
            }

        if publish_boundary == BOUNDARY_PARTITION:
            # 逐分区发布：核验通过的分区立即生效，不符的留在待办。
            for name in expected:
                state = job.object_states[name]
                if state["state"] == OBJ_VERIFIED:
                    written = table.ensure_partition(
                        name,
                        source=job.source_table,
                        snapshot_id=job.snapshot_id,
                        digest=state["digest"],
                    )
                    state["state"] = OBJ_PUBLISHED
                    state["published"] = True
                    state["write_count"] = 1 if written else 0
            job.status = JOB_COMPLETED if not job.pending_objects() else JOB_PARTIAL
            note = (
                "逐分区发布：核验通过的分区已生效"
                + ("" if not job.pending_objects()
                   else f"，摘要不符对象保留待办：{'、'.join(job.pending_objects())}")
            )
        else:
            # 整批确认：任一对象未核验通过则整批都不发布，p1 也保持不可见。
            if verification["all_objects_ok"]:
                for name in expected:
                    state = job.object_states[name]
                    written = table.ensure_partition(
                        name,
                        source=job.source_table,
                        snapshot_id=job.snapshot_id,
                        digest=state["digest"],
                    )
                    state["state"] = OBJ_PUBLISHED
                    state["published"] = True
                    state["write_count"] = 1 if written else 0
                job.status = JOB_COMPLETED
                note = "整批确认：全部预期分区核验通过后统一发布"
            else:
                job.status = JOB_PARTIAL
                bad = [
                    item["partition"] for item in verification["objects"]
                    if not item["ok"]
                ]
                note = (
                    "整批确认：存在摘要不符对象，整批暂缓发布；"
                    f"未生效对象：{'、'.join(bad)}"
                )

        self._jobs[job.job_id] = job
        receipt = job.receipt(phase="start", note=note)
        receipt["package_ok"] = verification["package_ok"]
        job.receipts.append(receipt)
        return job, receipt

    def resend_object(
        self,
        *,
        job_id: str,
        partition: str,
        obj: dict[str, Any],
    ) -> dict[str, Any]:
        """补送修好的分区对象；重复提交同一分区对象只生效一次。"""
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"恢复作业 {job_id} 不存在")
        if partition not in job.expected_partitions:
            raise ValueError(f"分区 {partition} 不属于作业 {job_id} 的预期范围")

        table = self.ensure_table(job.catalog_id, job.target_table)
        result = verify_object(obj)
        state = job.object_states[partition]

        if not result["ok"]:
            # 仍然摘要不符：继续挂待办，不能发布，回执不记成功。
            state["state"] = OBJ_MISMATCH
            state["expected_digest"] = result["expected_digest"]
            state["actual_digest"] = result["actual_digest"]
            state["published"] = False
            receipt = job.receipt(
                phase="resend",
                note=f"补送分区 {partition} 摘要仍不符，继续待办",
            )
            receipt["partition"] = partition
            receipt["duplicate"] = False
            job.receipts.append(receipt)
            return receipt

        if state.get("published", False):
            # 重复补送已生效对象：幂等跳过，只生效一次。
            receipt = job.receipt(
                phase="resend",
                note=f"分区 {partition} 已生效，重复补送仅核验不重复写入",
            )
            receipt["partition"] = partition
            receipt["duplicate"] = True
            receipt["written"] = False
            job.receipts.append(receipt)
            return receipt

        # 新的、核验通过的补送对象，先更新核验状态；是否立即发布由发布边界决定。
        state["state"] = OBJ_VERIFIED
        state["digest"] = result["actual_digest"]
        state["write_count"] = state.get("write_count", 0)

        if job.publish_boundary == BOUNDARY_BATCH:
            # 整批确认边界：单个对象先暂存，全部预期分区核验通过后一起发布。
            if not job.unverified_objects():
                for name in job.expected_partitions:
                    other = job.object_states[name]
                    if other["state"] != OBJ_PUBLISHED:
                        written = table.ensure_partition(
                            name,
                            source=job.source_table,
                            snapshot_id=job.snapshot_id,
                            digest=other["digest"],
                        )
                        other["state"] = OBJ_PUBLISHED
                        other["published"] = True
                        other["write_count"] += 1 if written else 0
                job.status = JOB_COMPLETED
                note = "整批确认：全部预期分区补齐核验通过，整批发布完成"
            else:
                job.status = JOB_PARTIAL
                note = (
                    f"分区 {partition} 已核验暂存，仍有待办："
                    f"{'、'.join(job.pending_objects())}"
                )
        else:
            # 逐分区发布：核验通过即生效。
            written = table.ensure_partition(
                partition,
                source=job.source_table,
                snapshot_id=job.snapshot_id,
                digest=result["actual_digest"],
            )
            state["state"] = OBJ_PUBLISHED
            state["published"] = True
            state["write_count"] += 1 if written else 0
            job.status = JOB_COMPLETED if not job.pending_objects() else JOB_PARTIAL
            note = f"逐分区发布：补送分区 {partition} 已生效"

        receipt = job.receipt(phase="resend", note=note)
        receipt["partition"] = partition
        newly_written = state.get("write_count", 0) > 0 and state["state"] == OBJ_PUBLISHED
        receipt["duplicate"] = not newly_written
        receipt["written"] = newly_written
        job.receipts.append(receipt)
        return receipt

    def get_job(self, job_id: str) -> RecoveryJob | None:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[dict[str, Any]]:
        return [
            {
                "job_id": job.job_id,
                "catalog_id": job.catalog_id,
                "target_table": job.target_table,
                "source_table": job.source_table,
                "snapshot_id": job.snapshot_id,
                "publish_boundary": job.publish_boundary,
                "status": job.status,
                "expected_partitions": job.expected_partitions,
                "effective_partitions": job.effective_partitions(),
                "pending_partitions": job.pending_objects(),
            }
            for job in sorted(self._jobs.values(), key=lambda item: item.job_id)
        ]


# 进程级单例：服务与路由共享同一份离线恢复登记状态。
recovery_registry = RecoveryRegistry()
