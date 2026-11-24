"""快照恢复数据包与异地恢复测试：复现 R3 的 p1 完整、p2 摘要不符场景。

覆盖需求：
1. 数据包保留来源表、快照版本、分区对象与内容摘要，原交付数据可重演；
2. 逐分区 / 整批确认两种发布边界下，摘要不符的 p2 对象都不可读；
3. 目的表原有无关分区 p3 继续可读，恢复作业不触碰它；
4. 补送两次修好的 p2：第一次生效，第二次重复提交只生效一次；
5. 未通过核验的对象不能单独发布，回执不会把未生效范围记为成功；
6. 未核验对象在整批边界下保持不可见，补齐核验后整批可见。
"""
from __future__ import annotations

import copy
import sys
sys.path.insert(0, '.')

from fastapi.testclient import TestClient

from app.main import app
from app.recovery import (
    BOUNDARY_BATCH,
    BOUNDARY_PARTITION,
    build_partition_object,
    build_recovery_package,
    digest_payload,
    package_bytes,
    recovery_registry,
    verify_package,
)
from app.services import service_for

client = TestClient(app)

SOURCE_TABLE = "orders_r3"
SNAPSHOT_ID = "snap-r3"
SNAPSHOT_VERSION = "v-r3"
TARGET_TABLE = "orders_r3_dr"
PREEXISTING = "p3"


def _payload(partition: str) -> dict:
    return {
        "source_table": SOURCE_TABLE,
        "snapshot_id": SNAPSHOT_ID,
        "partition": partition,
        "rows": [
            {"id": f"{SNAPSHOT_ID}-{partition}-1", "val": f"{SOURCE_TABLE}|{partition}|a"},
            {"id": f"{SNAPSHOT_ID}-{partition}-2", "val": f"{SOURCE_TABLE}|{partition}|b"},
        ],
    }


def _clean_package() -> dict:
    """R3 原始交付包：p1、p2 两个分区，摘要全部正确，可原样重演。"""
    return build_recovery_package(
        source_table=SOURCE_TABLE,
        snapshot_id=SNAPSHOT_ID,
        snapshot_version=SNAPSHOT_VERSION,
        snapshot_time="2026-11-24T08:00:00",
        partitions=[
            {"partition": "p1", "payload": _payload("p1")},
            {"partition": "p2", "payload": _payload("p2")},
        ],
    )


def _tamper_p2(package: dict) -> dict:
    """故意让 p2 对象摘要不符：载荷被改写但包内摘要保持原值。"""
    broken = copy.deepcopy(package)
    for obj in broken["objects"]:
        if obj["partition"] == "p2":
            obj["payload"]["rows"].append(
                {"id": "injected-row", "val": "tampered"}
            )
            # 清单与对象上的 digest 保持旧值 -> 重新核验必然不符
    for item in broken["manifest"]["partitions"]:
        if item["partition"] == "p2":
            item["digest"] = next(
                obj["digest"] for obj in package["objects"] if obj["partition"] == "p2"
            )
    return broken


def _fixed_p2_object() -> dict:
    """修好的 p2 对象：按修复后载荷重新计算正确摘要。"""
    return build_partition_object("p2", _payload("p2"))


def _partition_names(receipt: dict) -> set[str]:
    return {item["partition"] for item in receipt["readable_partitions"]}


class TestRecoveryPackageBuild:
    def test_package_keeps_provenance_and_digests(self):
        package = _clean_package()
        manifest = package["manifest"]
        assert manifest["source_table"] == SOURCE_TABLE
        assert manifest["snapshot_id"] == SNAPSHOT_ID
        assert manifest["snapshot_version"] == SNAPSHOT_VERSION
        assert [item["partition"] for item in manifest["partitions"]] == ["p1", "p2"]
        for obj in package["objects"]:
            assert obj["digest_algo"] == "sha256"
            assert obj["digest"] == digest_payload(obj["payload"])
        assert package["package_digest"]

    def test_package_is_replayable_byte_for_byte(self):
        first = package_bytes(_clean_package())
        second = package_bytes(_clean_package())
        # 原交付数据必须能重演：两次构建的离线载体逐字节一致。
        assert first == second

    def test_tampered_p2_fails_object_verification_but_p1_passes(self):
        verification = verify_package(_tamper_p2(_clean_package()))
        by_partition = {item["partition"]: item for item in verification["objects"]}
        assert by_partition["p1"]["ok"] is True
        assert by_partition["p2"]["ok"] is False
        assert by_partition["p2"]["expected_digest"] != by_partition["p2"]["actual_digest"]

    def test_snapshot_service_builds_and_downloads_package(self):
        snap_svc = service_for("snapshot_mgr")
        package, message = snap_svc.build_recovery_package(1, snapshot_version="v9")
        assert package is not None and package["manifest"]["snapshot_version"] == "v9"
        exported = snap_svc.export_recovery_package(1, snapshot_version="v9")
        assert exported is not None
        content, filename, digest = exported
        assert filename.endswith(".json")
        assert digest == package["package_digest"]
        assert content  # 离线载体非空


class TestPartitionBoundary:
    """发布边界一：逐分区发布，p1 在 p2 补齐前即可读，p2 不可读。"""

    def test_partition_boundary_p1_readable_p2_blocked_p3_kept(self):
        catalog_svc = service_for("table_catalog")
        receipt = catalog_svc.start_disaster_recovery(
            1,
            publish_boundary=BOUNDARY_PARTITION,
            package=_tamper_p2(_clean_package()),
            preexisting_partitions=[PREEXISTING],
        )
        # 记录本次采用的发布边界
        assert receipt["publish_boundary"] == BOUNDARY_PARTITION
        # 回执不能把未生效范围记为成功
        assert receipt["ok"] is False
        assert receipt["effective_partitions"] == ["p1"]
        assert receipt["pending_partitions"] == ["p2"]
        states = receipt["object_states"]
        assert states["p1"]["state"] == "已发布" and states["p1"]["published"] is True
        assert states["p2"]["state"] == "摘要不符" and states["p2"]["published"] is False

        readable = _partition_names(receipt)
        # p1 已可读；p2 部分对象不可读；目的表原有无关分区 p3 保留可读
        assert "p1" in readable
        assert "p2" not in readable
        assert PREEXISTING in readable

        view = catalog_svc.readable_partitions(1)
        names = {item["partition"] for item in view["partitions"]}
        assert names == {"p1", PREEXISTING}

    def test_two_redeliveries_write_p2_only_once(self):
        catalog_svc = service_for("table_catalog")
        receipt = catalog_svc.start_disaster_recovery(
            1,
            publish_boundary=BOUNDARY_PARTITION,
            package=_tamper_p2(_clean_package()),
            preexisting_partitions=[PREEXISTING],
        )
        job_id = receipt["job_id"]
        fixed = _fixed_p2_object()

        # 第一次补送修好的 p2：核验通过并生效
        first = catalog_svc.resend_recovery_object(
            1, job_id=job_id, partition="p2", obj=fixed
        )
        assert first["ok"] is True
        assert first["written"] is True and first["duplicate"] is False
        assert "p2" in _partition_names(first)

        # 第二次补送同一分区对象：只生效一次
        second = catalog_svc.resend_recovery_object(
            1, job_id=job_id, partition="p2", obj=copy.deepcopy(fixed)
        )
        assert second["duplicate"] is True
        assert second["written"] is False
        assert second["ok"] is True

        # 重开目标表核对：p1、p2 可用，p3 保留，且 p2 只写入一次
        view = catalog_svc.readable_partitions(1)
        names = [item["partition"] for item in view["partitions"]]
        assert sorted(names) == sorted(["p1", "p2", PREEXISTING])
        job = recovery_registry.get_job(job_id)
        assert job.object_states["p2"]["write_count"] == 1
        assert job.object_states["p1"]["write_count"] == 1


class TestBatchBoundary:
    """发布边界二：整批确认，p2 不符时 p1 也继续不可见，补齐后一起发布。"""

    def test_batch_boundary_holds_all_until_verified(self):
        catalog_svc = service_for("table_catalog")
        receipt = catalog_svc.start_disaster_recovery(
            2,
            publish_boundary=BOUNDARY_BATCH,
            package=_tamper_p2(_clean_package()),
            preexisting_partitions=[PREEXISTING],
        )
        assert receipt["publish_boundary"] == BOUNDARY_BATCH
        assert receipt["ok"] is False
        # 整批暂缓：p1、p2 都不在生效范围
        assert receipt["effective_partitions"] == []
        assert set(receipt["pending_partitions"]) == {"p1", "p2"}
        states = receipt["object_states"]
        assert states["p1"]["published"] is False
        assert states["p2"]["state"] == "摘要不符" and states["p2"]["published"] is False

        readable = _partition_names(receipt)
        # 无论哪种边界，p2 的部分对象都不可读；整批下 p1 同样不可见；p3 保留
        assert "p1" not in readable
        assert "p2" not in readable
        assert PREEXISTING in readable

    def test_batch_publishes_together_after_redelivery(self):
        catalog_svc = service_for("table_catalog")
        receipt = catalog_svc.start_disaster_recovery(
            2,
            publish_boundary=BOUNDARY_BATCH,
            package=_tamper_p2(_clean_package()),
            preexisting_partitions=[PREEXISTING],
        )
        job_id = receipt["job_id"]
        fixed = _fixed_p2_object()

        first = catalog_svc.resend_recovery_object(
            2, job_id=job_id, partition="p2", obj=fixed
        )
        # 全部预期分区核验通过 -> 整批一起发布
        assert first["ok"] is True
        assert sorted(first["effective_partitions"]) == ["p1", "p2"]
        assert first["pending_partitions"] == []
        names = _partition_names(first)
        assert {"p1", "p2", PREEXISTING}.issubset(names)

        second = catalog_svc.resend_recovery_object(
            2, job_id=job_id, partition="p2", obj=copy.deepcopy(fixed)
        )
        assert second["duplicate"] is True and second["written"] is False
        job = recovery_registry.get_job(job_id)
        assert job.object_states["p2"]["write_count"] == 1


class TestBoundaryEquivalenceAndReceiptSafety:
    def test_p2_unreadable_under_both_boundaries(self):
        catalog_svc = service_for("table_catalog")
        per_partition = catalog_svc.start_disaster_recovery(
            1,
            publish_boundary=BOUNDARY_PARTITION,
            package=_tamper_p2(_clean_package()),
            preexisting_partitions=[PREEXISTING],
        )
        whole_batch = catalog_svc.start_disaster_recovery(
            2,
            publish_boundary=BOUNDARY_BATCH,
            package=_tamper_p2(_clean_package()),
            preexisting_partitions=[PREEXISTING],
        )
        # 两种边界改变的是 p1 的可见性，但 p2 都不可读、p3 都保留
        assert "p2" not in _partition_names(per_partition)
        assert "p2" not in _partition_names(whole_batch)
        assert PREEXISTING in _partition_names(per_partition)
        assert PREEXISTING in _partition_names(whole_batch)
        assert "p1" in _partition_names(per_partition)
        assert "p1" not in _partition_names(whole_batch)
        # 任何回执的成功范围都不包含未生效的 p2
        assert "p2" not in per_partition["effective_partitions"]
        assert "p2" not in whole_batch["effective_partitions"]

    def test_bad_redelivery_keeps_object_pending(self):
        catalog_svc = service_for("table_catalog")
        receipt = catalog_svc.start_disaster_recovery(
            1,
            publish_boundary=BOUNDARY_PARTITION,
            package=_tamper_p2(_clean_package()),
        )
        still_bad = copy.deepcopy(_fixed_p2_object())
        still_bad["payload"]["rows"].append({"id": "x", "val": "corrupt"})
        result = catalog_svc.resend_recovery_object(
            1,
            job_id=receipt["job_id"],
            partition="p2",
            obj=still_bad,
        )
        assert result["ok"] is False
        assert result["pending_partitions"] == ["p2"]
        assert "p2" not in _partition_names(result)
        assert result["object_states"]["p2"]["state"] == "摘要不符"


class TestRecoveryApi:
    def test_full_flow_over_http(self):
        # 1) 快照侧构建并下载恢复数据包（显式指定 p1/p2 两个分区）
        resp = client.post(
            "/api/recovery/snapshots/1/packages",
            json={
                "snapshot_version": "v-r3",
                "partitions": [
                    {"partition": "p1", "payload": _payload("p1")},
                    {"partition": "p2", "payload": _payload("p2")},
                ],
            },
        )
        assert resp.status_code == 200
        digest = resp.json()["package_digest"]

        download = client.get(
            "/api/recovery/snapshots/1/packages/download",
            params={"snapshot_version": "v-r3"},
        )
        assert download.status_code == 200
        assert download.headers["X-Package-Digest"] == digest

        # 2) 目录侧取包并损坏 p2，按逐分区边界发起恢复
        package = recovery_registry.get_package(digest)
        broken = _tamper_p2(package)
        start = client.post(
            "/api/recovery/catalogs/1/jobs",
            json={
                "publish_boundary": BOUNDARY_PARTITION,
                "package": broken,
                "preexisting_partitions": [PREEXISTING],
            },
        )
        assert start.status_code == 200
        receipt = start.json()
        assert receipt["ok"] is False
        assert receipt["effective_partitions"] == ["p1"]
        job_id = receipt["job_id"]

        # 3) 补送两次修复后的 p2
        fixed = _fixed_p2_object()
        r1 = client.post(
            "/api/recovery/catalogs/1/jobs/resend",
            json={"job_id": job_id, "partition": "p2", "obj": fixed},
        )
        assert r1.json()["written"] is True
        r2 = client.post(
            "/api/recovery/catalogs/1/jobs/resend",
            json={"job_id": job_id, "partition": "p2", "obj": fixed},
        )
        body2 = r2.json()
        assert body2["duplicate"] is True and body2["written"] is False

        # 4) 重开目标表核对可读分区
        view = client.get("/api/recovery/catalogs/1/partitions")
        names = sorted(item["partition"] for item in view.json()["partitions"])
        assert names == sorted(["p1", "p2", PREEXISTING])

    def test_invalid_boundary_rejected(self):
        package = _clean_package()
        resp = client.post(
            "/api/recovery/catalogs/1/jobs",
            json={"publish_boundary": "mystery", "package": package},
        )
        assert resp.status_code == 400

    def test_recovery_routes_not_shadowed_by_resource_routes(self):
        # 回归：/api/recovery/packages 不能被通用 /api/{module}/{entry_id} 抢占成 422。
        resp = client.get("/api/recovery/packages")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_abnormal_state_written_back_to_catalog_record(self):
        package = _tamper_p2(_clean_package())
        resp = client.post(
            "/api/recovery/catalogs/1/jobs",
            json={
                "publish_boundary": BOUNDARY_PARTITION,
                "package": package,
                "preexisting_partitions": [PREEXISTING],
            },
        )
        assert resp.status_code == 200
        record = client.get("/api/table_catalog/1").json()
        # 异常状态回写补齐待办，并注明哪些对象尚未生效
        assert record["abnormal"] is True
        assert record["待补分区"] == "p2"
        assert record["生效分区"] == "p1"
        assert "p2" in record["异常说明"]
        assert record["同步状态"] == "恢复待补齐"
