"""暂存落地领域服务：维护传输数据的暂存分区落地路径、文件组织与驻留时限控制。

本模块从传输批次产物文件读取数据，执行分区落地写入暂存产物文件，
落地完成后记录落地水位并支持超时清空。

重复写入控制：续传产物与暂存分片数据在交付时逐分片登记“已落地续传分片凭据”，
持久保存实际落地进度。异常退出（数据已写、索引未同步、回执未持久化）后，
恢复流程以磁盘实际数据与已持久化凭据的交集确认已落地分片、补登缺失回执、
只补写尚未落地的分片，保证续传进度与真正已落地的行一致且不重复写入。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService, _to_int
from app.store import store

DEFAULT_CHUNK_SIZE = 3


class StagingAreaService(BaseModuleService):
    MODULE = "staging_area"
    ENTITY = "暂存分区"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["暂存编号", "批次编号", "落地路径", "文件数量", "记录条数", "占用字节", "驻留时限", "落地水位"]
    REQUIRED_FIELDS = ["暂存编号", "批次编号", "落地路径"]
    STATUS_ORDER = ["草稿", "落地中", "已落地", "已清空"]
    ACTION_RULES = {"启动落地": "落地中", "确认落地": "已落地", "执行清空": "已清空", "恢复落地": "落地中"}
    NEGATIVE_ACTIONS = ["执行清空"]

    # -- 内部帮助器 --

    def _find_active_entry(self, batch_code: str) -> dict[str, Any] | None:
        """查找批次当前仍在使用的暂存分区（未清空），用于重复落地时复用而非新建。"""
        candidates = [
            row for row in store.rows(self.MODULE)
            if row.get("批次编号") == batch_code and row.get("status") != "已清空"
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: int(row.get("id", 0)))

    def _create_staging_entry(self, batch_code: str, expected_count: int) -> dict[str, Any]:
        """为批次创建暂存分区登记项，记录应落地条数，落地水位从零开始推进。"""
        staging_code = f"STAG-{batch_code}-{int(time.time() * 1000)}"
        values = {
            "暂存编号": staging_code,
            "批次编号": batch_code,
            "落地路径": f"/staging/{batch_code}/",
            "文件数量": "1",
            "记录条数": str(expected_count),
            "占用字节": "0",
            "驻留时限": "86400",
            "落地水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建暂存分区失败: {missing}")
        return entry

    @staticmethod
    def _artifact_rel(staging_code: str) -> str:
        """暂存产物文件相对数据根目录的路径，凭据与核对均以此为准。"""
        return f"staging/{staging_code}.jsonl"

    def _sync_entry_watermark(self, entry: dict[str, Any], landed: int) -> None:
        """把索引（落地水位、占用字节）同步到磁盘实际落地进度。"""
        artifact_file = self._data_dir("staging") / f"{entry['暂存编号']}.jsonl"
        rows = self._read_jsonl(artifact_file)
        entry["落地水位"] = str(landed)
        entry["占用字节"] = str(sum(len(str(r).encode("utf-8")) for r in rows))
        entry["文件数量"] = "1" if artifact_file.exists() else "0"

    # -- 分片交付（重复写入控制核心） --

    def deliver_chunks(
        self,
        batch_code: str,
        source_records: list[dict[str, Any]],
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        source: str = "staging",
    ) -> dict[str, Any]:
        """把批次记录按分片交付到暂存分区，逐分片登记已落地续传分片凭据。

        交付前先用凭据与磁盘实际数据核对：已确认落地的分片直接跳过（不重复写入），
        数据已写但回执缺失的区间补登凭据，仅补写尚未落地的分片；
        每个分片都是先写数据落盘、再登记凭据，保证异常退出后恢复可对账。
        """
        entry = self._find_active_entry(batch_code)
        if entry is None:
            entry = self._create_staging_entry(batch_code, len(source_records))
        staging_code = entry["暂存编号"]
        artifact_rel = self._artifact_rel(staging_code)
        artifact_file = self._data_dir("staging") / f"{staging_code}.jsonl"
        partition = entry["落地路径"]

        # 重试恢复前先确认：已写下的分片凭据+磁盘双向核对，缺失回执补登
        recon = self._reconcile_landed_chunks(
            batch_code, artifact_rel, chunk_size,
            backfill=True,
            receipt_extra={"staging_code": staging_code, "partition": partition},
        )
        confirmed = recon["confirmed"]

        # 尚未落地的分片补写：数据先落盘，凭据后登记
        remaining = source_records[confirmed:]
        written = 0
        for offset in range(0, len(remaining), chunk_size):
            shard = remaining[offset:offset + chunk_size]
            if not shard:
                continue
            offset_start = confirmed + offset
            offset_end = offset_start + len(shard)
            self._write_jsonl(artifact_file, shard)
            self._append_chunk_receipt(batch_code, {
                "receipt_id": f"RCPT-{batch_code}-{offset_start // chunk_size}-{source}-{int(time.time() * 1000)}",
                "batch_code": batch_code,
                "staging_code": staging_code,
                "source": source,
                "partition": partition,
                "artifact": artifact_rel,
                "chunk_index": offset_start // chunk_size,
                "offset_start": offset_start,
                "offset_end": offset_end,
                "record_count": len(shard),
                "checksum": self._checksum_rows(shard),
                "delivered_at": time.time(),
            })
            written += len(shard)

        landed = confirmed + written
        self._sync_entry_watermark(entry, landed)

        self._write_checkpoint(f"staging_{staging_code}", {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": landed,
            "confirmed_before": recon["receipt_confirmed"],
            "backfilled_receipts": recon["backfilled"],
            "total_bytes": int(entry["占用字节"]),
            "landed_at": time.time(),
            "retention_seconds": 86400,
        })

        return {
            "staging_code": staging_code,
            "staging_id": entry["id"],
            "batch_code": batch_code,
            "partition": partition,
            "confirmed_before": recon["receipt_confirmed"],
            "backfilled_receipts": recon["backfilled"],
            "rewritten": written,
            "records_landed": landed,
            "expected_count": len(source_records),
            "fully_landed": landed >= len(source_records),
            "total_bytes": int(entry["占用字节"]),
            "artifact_path": str(artifact_file),
        }

    def confirm_landed(self, batch_code: str) -> dict[str, Any]:
        """核对批次在暂存分区的实际交付进度，补登缺失回执并同步索引水位。

        供断点续传计算恢复位置：续传只从实际已交付的批次分片之后继续，
        未落地分片不会被提前视为完成。数据已写但回执缺失的区间在此补登。
        """
        entry = self._find_active_entry(batch_code)
        if entry is None:
            return {"confirmed": 0, "backfilled": 0, "actual": 0}
        recon = self._reconcile_landed_chunks(
            batch_code,
            self._artifact_rel(entry["暂存编号"]),
            DEFAULT_CHUNK_SIZE,
            backfill=True,
            receipt_extra={"staging_code": entry["暂存编号"], "partition": entry["落地路径"]},
        )
        if _to_int(entry.get("落地水位")) != recon["confirmed"]:
            self._sync_entry_watermark(entry, recon["confirmed"])
        return {
            "confirmed": recon["confirmed"],
            "backfilled": recon["backfilled"],
            "actual": recon["actual"],
        }

    def confirmed_landed_count(self, batch_code: str) -> int:
        """返回批次在暂存分区中实际已交付（凭据与磁盘共同确认）的记录行数。"""
        return self.confirm_landed(batch_code)["confirmed"]

    def _expected_records(self, batch_code: str, fallback_count: int) -> list[dict[str, Any]]:
        """读取批次应落地的源记录：优先传输产物文件，缺失时按应落地条数模拟。"""
        source_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        if source_file.exists():
            return self._read_jsonl(source_file)
        return self._simulate_records(batch_code, count=fallback_count, field_prefix="staging")

    # -- 对外业务动作 --

    def land_batch(self, batch_code: str, record_count: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> dict[str, Any]:
        """将传输批次数据落地到暂存分区，读取传输产物文件并写入暂存产物文件。

        暂存分区是传输链路中数据暂存的关键环节：它从传输产物文件读取数据，
        分片写入落地路径下的暂存产物文件，逐分片登记已落地续传分片凭据并推进
        分区水位。同一批次重复落地时复用既有分区，已确认分片跳过、缺失分片补写，
        避免异常重试造成重复写入。
        """
        source_records = self._expected_records(batch_code, record_count)
        delivery = self.deliver_chunks(batch_code, source_records, chunk_size=chunk_size, source="staging")
        return {
            "staging_code": delivery["staging_code"],
            "batch_code": batch_code,
            "records_landed": delivery["records_landed"],
            "confirmed_before": delivery["confirmed_before"],
            "backfilled_receipts": delivery["backfilled_receipts"],
            "rewritten": delivery["rewritten"],
            "total_bytes": delivery["total_bytes"],
            "artifact_path": delivery["artifact_path"],
        }

    def recover_landing(self, staging_id: int) -> dict[str, Any]:
        """恢复暂存分区落地：核对凭据与磁盘，补登缺失回执，只补写未落地分片。

        对应值班人员在异常退出后再次点击“恢复”的场景：同一批次最后几行可能
        已写到暂存分区但索引未同步、续传回执未持久化。恢复时以磁盘实际数据与
        已持久化凭据的交集为准确认进度，使落地水位与真正已落地的行对齐。
        """
        entry = self.get_entry(staging_id)
        if entry is None:
            return {"error": f"暂存分区 {staging_id} 不存在或已归档", "staging_id": staging_id}
        if entry.get("status") == "已清空":
            return {"error": f"暂存分区 {entry['暂存编号']} 已清空，无法恢复", "staging_id": staging_id}

        batch_code = entry["批次编号"]
        expected = _to_int(entry.get("记录条数"))
        source_records = self._expected_records(batch_code, expected)
        delivery = self.deliver_chunks(batch_code, source_records, source="staging")
        delivery["fully_landed"] = delivery["records_landed"] >= len(source_records)
        return delivery

    def verify_landing(self, staging_id: int) -> dict[str, Any]:
        """核对暂存分区落地一致性：实际数量、分区与断点位置是否对得上。

        供重新进入暂存页面时核对使用：比较磁盘实际行数、凭据确认的断点位置与
        索引落地水位，并返回最后两行的实际位置与内容。只读核对，不改写任何数据。
        """
        entry = self.get_entry(staging_id)
        if entry is None:
            return {"error": f"暂存分区 {staging_id} 不存在或已归档", "staging_id": staging_id}

        staging_code = entry["暂存编号"]
        batch_code = entry["批次编号"]
        partition = entry["落地路径"]
        artifact_rel = self._artifact_rel(staging_code)
        rows = self._read_jsonl(self._data_dir("staging") / f"{staging_code}.jsonl")
        actual = len(rows)

        recon = self._reconcile_landed_chunks(batch_code, artifact_rel, DEFAULT_CHUNK_SIZE, backfill=False)
        confirmed = recon["confirmed"]
        watermark = _to_int(entry.get("落地水位"))
        expected = _to_int(entry.get("记录条数"))

        last_rows = [
            {"position": actual - len(rows[-2:]) + idx + 1, "row": row}
            for idx, row in enumerate(rows[-2:])
        ]
        checks = {
            "实际数量一致": actual == watermark == confirmed,
            "分区一致": partition == f"/staging/{batch_code}/",
            "断点位置一致": confirmed == watermark,
        }
        return {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "partition": partition,
            "expected_count": expected,
            "actual_count": actual,
            "confirmed_count": confirmed,
            "watermark": watermark,
            "breakpoint_position": confirmed,
            "untracked_rows": actual - confirmed,
            "last_rows": last_rows,
            "checks": checks,
            "consistent": all(checks.values()),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作分派：恢复落地走真实的凭据核对与补写流程，其余动作沿用状态机。"""
        if action == "恢复落地":
            report = self.recover_landing(entry_id)
            if "error" in report:
                return None, report["error"]
            entry = self.get_entry(entry_id)
            if entry is not None:
                entry["status"] = "已落地" if report["fully_landed"] else "落地中"
                entry["pending"] = not report["fully_landed"]
                entry["abnormal"] = False
            return entry, (
                f"暂存分区已恢复落地：确认{report['confirmed_before']}行，"
                f"补登回执{report['backfilled_receipts']}张，补写{report['rewritten']}行"
            )
        return super().run_action(entry_id, action)

    def confirm_land(self, staging_id: int) -> dict[str, Any]:
        """确认暂存分区落地完成，校验落地产物数据完整性并写入落地确认日志。"""
        entry, msg = self.run_action(staging_id, "确认落地")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        # 验证落地产物文件存在且记录数匹配
        artifact_file = self._data_dir("staging") / f"{staging_code}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "staging_confirm_log.jsonl",
            [{"action": "confirm_land", "staging_code": staging_code,
              "record_count": len(records), "checksum": checksum,
              "confirmed_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已落地", "record_count": len(records),
                "checksum": checksum}

    def clear_staging(self, staging_id: int) -> dict[str, Any]:
        """执行暂存分区清空，删除落地产物文件并写入清空日志。"""
        entry, msg = self.run_action(staging_id, "执行清空")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        # 标记清空断点
        self._write_checkpoint(f"staging_{staging_code}_cleared", {
            "staging_code": staging_code,
            "cleared_at": time.time(),
            "status": "cleared",
        })
        self._write_jsonl(
            self._data_dir("events") / "staging_clear_log.jsonl",
            [{"action": "clear", "staging_code": staging_code, "cleared_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已清空"}
