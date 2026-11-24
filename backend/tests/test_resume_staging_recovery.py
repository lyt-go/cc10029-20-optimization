"""断点续传→暂存落地重复写入控制测试：已落地续传分片凭据与恢复核对。

覆盖场景：同一批次最后两行已写到暂存分区，索引未同步就异常退出、续传回执
也未持久化，值班人员再次点击恢复——续传进度必须与真正已落地的行对得上：
已写下的分片可确认、未落地的分片不被提前视为完成、暂存按凭据决定是否补写、
续传从实际已交付的批次分片继续。
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services.resume_point import ResumePointService
from app.services.staging_area import StagingAreaService
from app.store import store


@pytest.fixture(autouse=True)
def cleanup():
    """每个测试前重置store数据，确保测试隔离（产物文件按唯一批次编号隔离）。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _unique_batch(prefix: str = "TRAN-RECOV") -> str:
    """生成唯一批次编号，避免历史产物文件干扰本次断言。"""
    return f"{prefix}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"


def _write_batch_artifact(svc: StagingAreaService, batch_code: str, count: int) -> list[dict]:
    """直接写入批次传输产物文件，作为暂存落地与续传的源记录。"""
    records = [
        {"batch_key": batch_code, "line_no": i + 1, "field": f"val_{i + 1}"}
        for i in range(count)
    ]
    svc._write_jsonl(svc._data_dir("batches") / f"{batch_code}.jsonl", records)
    return records


def _staging_entry(batch_code: str) -> dict:
    return StagingAreaService()._find_active_entry(batch_code)


def _staging_id(batch_code: str) -> int:
    return _staging_entry(batch_code)["id"]


def _artifact_path(staging_code: str) -> Path:
    return DATA_ROOT / "staging" / f"{staging_code}.jsonl"


def _artifact_rows(staging_code: str) -> list[dict]:
    return StagingAreaService()._read_jsonl(_artifact_path(staging_code))


def _receipts(batch_code: str) -> list[dict]:
    return StagingAreaService()._read_chunk_receipts(batch_code)


def _drop_last_receipt(batch_code: str) -> None:
    """模拟异常退出：最后一张已落地续传分片凭据尚未持久化。"""
    receipt_file = StagingAreaService()._receipt_file(batch_code)
    lines = [l for l in receipt_file.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert lines, "凭据日志不能为空"
    receipt_file.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")


def _truncate_artifact(staging_code: str, keep: int) -> None:
    """模拟数据尚未落地：暂存产物只保留前 keep 行。"""
    path = _artifact_path(staging_code)
    lines = [l for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    path.write_text("\n".join(lines[:keep]) + "\n", encoding="utf-8")


def _append_fake_receipt(batch_code: str, staging_code: str, start: int, end: int) -> None:
    """模拟超前凭据：分片尚未落地却登记了完成回执。"""
    StagingAreaService()._append_chunk_receipt(batch_code, {
        "receipt_id": f"RCPT-{batch_code}-fake",
        "batch_code": batch_code,
        "staging_code": staging_code,
        "source": "staging",
        "partition": f"/staging/{batch_code}/",
        "artifact": f"staging/{staging_code}.jsonl",
        "chunk_index": start // 3,
        "offset_start": start,
        "offset_end": end,
        "record_count": end - start,
        "checksum": "deadbeefdeadbeef",
        "delivered_at": time.time(),
    })


class TestReceiptWrittenOnDelivery:
    """交付即登记：暂存分片与续传产物交付时写入已落地续传分片凭据。"""

    def test_staging_delivery_writes_chunk_receipts(self):
        svc = StagingAreaService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)

        result = svc.land_batch(batch, 8)
        assert result["records_landed"] == 8

        receipts = _receipts(batch)
        assert len(receipts) == 3  # 分片 3/3/2
        assert [(r["offset_start"], r["offset_end"]) for r in receipts] == [(0, 3), (3, 6), (6, 8)]
        # 同一批次最后两行单独成一片，凭据记录实际落地行数
        assert receipts[-1]["record_count"] == 2
        assert all(r["source"] == "staging" for r in receipts)
        assert all(r["partition"] == f"/staging/{batch}/" for r in receipts)
        # 凭据校验值与磁盘实际内容一致
        rows = _artifact_rows(result["staging_code"])
        for r in receipts:
            assert r["checksum"] == svc._checksum_rows(rows[r["offset_start"]:r["offset_end"]])
        assert _staging_entry(batch)["落地水位"] == "8"

    def test_resume_delivery_writes_receipts(self):
        svc = StagingAreaService()
        rp_svc = ResumePointService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)

        resume = rp_svc.create_resume_point(batch, 0, 8)
        result = rp_svc.resume_transfer(resume["resume_id"])
        assert result["records_resumed"] == 8

        receipts = _receipts(batch)
        assert len(receipts) == 3
        assert all(r["source"] == "resume" for r in receipts)
        assert _staging_entry(batch)["落地水位"] == "8"


class TestRecoverWhenReceiptMissing:
    """恢复请求①：数据已写但回执未更新（索引也未同步），恢复后进度对齐实际落地行。"""

    def test_recover_backfills_receipt_without_duplicate_write(self):
        svc = StagingAreaService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)
        landed = svc.land_batch(batch, 8)
        staging_code = landed["staging_code"]
        staging_id = _staging_id(batch)

        # 异常退出：最后两行已在暂存分区，但凭据未持久化、索引（落地水位）未同步
        _drop_last_receipt(batch)
        entry = _staging_entry(batch)
        entry["落地水位"] = "6"

        # 恢复前核对：实际数量与索引水位对不上
        pre = svc.verify_landing(staging_id)
        assert pre["consistent"] is False
        assert pre["actual_count"] == 8
        assert pre["confirmed_count"] == 6
        assert pre["untracked_rows"] == 2

        # 值班人员点击恢复
        report = svc.recover_landing(staging_id)
        assert report["confirmed_before"] == 6   # 已写下的分片被确认
        assert report["backfilled_receipts"] == 1  # 缺失回执按磁盘实际补登
        assert report["rewritten"] == 0          # 已落地行不重复写入
        assert report["records_landed"] == 8

        # 暂存产物仍是 8 行，没有重复写入
        assert len(_artifact_rows(staging_code)) == 8
        # 索引同步到实际落地进度
        assert _staging_entry(batch)["落地水位"] == "8"
        # 补登的回执与磁盘内容一致
        backfill = [r for r in _receipts(batch) if r["source"] == "recovery-backfill"]
        assert len(backfill) == 1
        assert (backfill[0]["offset_start"], backfill[0]["offset_end"]) == (6, 8)
        rows = _artifact_rows(staging_code)
        assert backfill[0]["checksum"] == svc._checksum_rows(rows[6:8])

        # 重新进入暂存页面核对：最后两行的实际数量、分区、断点位置一致
        verify = svc.verify_landing(staging_id)
        assert verify["consistent"] is True
        assert verify["actual_count"] == 8
        assert verify["breakpoint_position"] == 8
        assert verify["partition"] == f"/staging/{batch}/"
        assert [r["position"] for r in verify["last_rows"]] == [7, 8]
        assert [r["row"]["line_no"] for r in verify["last_rows"]] == [7, 8]

    def test_recover_via_generic_action(self):
        svc = StagingAreaService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)
        svc.land_batch(batch, 8)
        staging_id = _staging_id(batch)
        _drop_last_receipt(batch)
        _staging_entry(batch)["落地水位"] = "6"

        entry, message = svc.run_action(staging_id, "恢复落地")
        assert entry is not None
        assert entry["status"] == "已落地"
        assert "补登回执1张" in message
        assert "补写0行" in message
        assert svc.verify_landing(staging_id)["consistent"] is True


class TestRecoverWhenDataNotLanded:
    """恢复请求②：另一次请求时数据尚未落地，恢复必须真正补写缺失分片。"""

    def test_recover_rewrites_unlanded_chunks(self):
        svc = StagingAreaService()
        batch = _unique_batch()
        source = _write_batch_artifact(svc, batch, 8)
        landed = svc.land_batch(batch, 8)
        staging_code = landed["staging_code"]
        staging_id = _staging_id(batch)

        # 数据尚未落地：最后两行丢失，凭据与索引也只到第 6 行
        _truncate_artifact(staging_code, keep=6)
        _drop_last_receipt(batch)
        _staging_entry(batch)["落地水位"] = "6"

        report = svc.recover_landing(staging_id)
        assert report["confirmed_before"] == 6
        assert report["backfilled_receipts"] == 0  # 磁盘没有无凭据数据，无需补登
        assert report["rewritten"] == 2            # 补写尚未落地的最后两行
        assert report["records_landed"] == 8

        rows = _artifact_rows(staging_code)
        assert len(rows) == 8
        # 补写的最后两行与源记录一致
        assert rows[6:] == source[6:]
        assert _staging_entry(batch)["落地水位"] == "8"

        verify = svc.verify_landing(staging_id)
        assert verify["consistent"] is True
        assert [r["row"]["line_no"] for r in verify["last_rows"]] == [7, 8]

    def test_second_recover_is_idempotent(self):
        svc = StagingAreaService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)
        landed = svc.land_batch(batch, 8)
        staging_code = landed["staging_code"]
        staging_id = _staging_id(batch)

        _truncate_artifact(staging_code, keep=6)
        _drop_last_receipt(batch)
        _staging_entry(batch)["落地水位"] = "6"

        first = svc.recover_landing(staging_id)
        assert first["rewritten"] == 2
        # 再次点击恢复：已落地分片全部确认，零补写零补登
        second = svc.recover_landing(staging_id)
        assert second["confirmed_before"] == 8
        assert second["rewritten"] == 0
        assert second["backfilled_receipts"] == 0
        assert len(_artifact_rows(staging_code)) == 8
        assert svc.verify_landing(staging_id)["consistent"] is True


class TestUnlandedChunkNotTreatedComplete:
    """尚未落地的分片不能被提前视为完成：超前凭据不被承认。"""

    def test_fake_receipt_rejected_and_chunk_rewritten(self):
        svc = StagingAreaService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)
        landed = svc.land_batch(batch, 8)
        staging_code = landed["staging_code"]
        staging_id = _staging_id(batch)

        # 最后两行未落地，但凭据超前登记、索引也超前
        _truncate_artifact(staging_code, keep=6)
        _drop_last_receipt(batch)
        _append_fake_receipt(batch, staging_code, 6, 8)
        _staging_entry(batch)["落地水位"] = "8"

        # 凭据与磁盘共同确认：超前凭据不被承认，实际进度只有 6 行
        assert svc.confirmed_landed_count(batch) == 6
        # 索引被纠正回实际落地进度
        assert _staging_entry(batch)["落地水位"] == "6"

        report = svc.recover_landing(staging_id)
        assert report["confirmed_before"] == 6
        assert report["rewritten"] == 2
        assert len(_artifact_rows(staging_code)) == 8
        assert svc.verify_landing(staging_id)["consistent"] is True


class TestResumeFromActuallyDelivered:
    """续传从实际已交付的批次分片继续，进度与真正落地的行对得上。"""

    def test_resume_after_crash_does_not_redeliver_landed_rows(self):
        svc = StagingAreaService()
        rp_svc = ResumePointService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)
        landed = svc.land_batch(batch, 8)
        staging_code = landed["staging_code"]

        # 异常退出：最后两行已落地，但回执未持久化、断点索引停在第 6 行
        _drop_last_receipt(batch)
        _staging_entry(batch)["落地水位"] = "6"
        resume = rp_svc.create_resume_point(batch, 6, 2)

        # 值班人员再次点了恢复：续传按磁盘实际确认到 8 行，不再补传
        result = rp_svc.resume_transfer(resume["resume_id"])
        assert result["delivered_before"] == 8
        assert result["backfilled_receipts"] == 1
        assert result["records_resumed"] == 0
        assert result["new_position"] == 8

        entry = rp_svc.get_entry(resume["resume_id"])
        assert entry["续传水位"] == "8"
        assert entry["status"] == "已恢复"
        # 暂存分区没有重复行
        assert len(_artifact_rows(staging_code)) == 8
        assert svc.verify_landing(_staging_id(batch))["consistent"] is True

    def test_resume_delivers_when_nothing_landed(self):
        svc = StagingAreaService()
        rp_svc = ResumePointService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)

        resume = rp_svc.create_resume_point(batch, 0, 8)
        result = rp_svc.resume_transfer(resume["resume_id"])
        assert result["delivered_before"] == 0
        assert result["records_resumed"] == 8
        assert result["new_position"] == 8

        entry = rp_svc.get_entry(resume["resume_id"])
        assert entry["续传水位"] == "8"
        assert entry["status"] == "已恢复"
        assert len(_receipts(batch)) == 3

    def test_resume_via_generic_action(self):
        svc = StagingAreaService()
        rp_svc = ResumePointService()
        batch = _unique_batch()
        _write_batch_artifact(svc, batch, 8)
        svc.land_batch(batch, 8)
        _drop_last_receipt(batch)
        _staging_entry(batch)["落地水位"] = "6"
        resume = rp_svc.create_resume_point(batch, 6, 2)

        entry, message = rp_svc.run_action(resume["resume_id"], "启动续传")
        assert entry is not None
        assert "补传0行" in message
        assert entry["续传水位"] == "8"
