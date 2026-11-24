"""断点续传领域服务：维护传输中断后已完成与未完成分片的断点登记与恢复位置。

本模块在传输批次因分片中断、重试超限等原因失败时，创建断点记录并支持从
恢复位置重新启动传输，保障数据传输的完整性。

恢复位置以“已落地续传分片凭据”为准：续传前先从暂存分区实际已交付的批次
分片确认进度（磁盘数据与凭据的交集），只补传尚未落地的分片，已写下的分片
不重复传输，未落地的分片不会被提前视为完成。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService, _to_int
from app.store import store


class ResumePointService(BaseModuleService):
    MODULE = "resume_point"
    ENTITY = "续传断点"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["断点编号", "批次编号", "已完成分片", "未完成分片", "恢复位置", "重试次数", "断点类型", "续传水位"]
    REQUIRED_FIELDS = ["断点编号", "批次编号", "已完成分片"]
    STATUS_ORDER = ["草稿", "待续传", "续传中", "已恢复"]
    ACTION_RULES = {"启动续传": "续传中", "确认恢复": "已恢复", "标记放弃": "已恢复"}
    NEGATIVE_ACTIONS = ["标记放弃"]

    def create_resume_point(self, batch_code: str, completed_chunks: int, pending_chunks: int) -> dict[str, Any]:
        """为传输中断批次创建断点续传记录，登记已完成分片、未完成分片和恢复位置。"""
        resume_code = f"RESU-{batch_code}-{int(time.time())}"
        values = {
            "断点编号": resume_code,
            "批次编号": batch_code,
            "已完成分片": str(completed_chunks),
            "未完成分片": str(pending_chunks),
            "恢复位置": str(completed_chunks),
            "重试次数": "0",
            "断点类型": "分片中断",
            "续传水位": str(completed_chunks),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建断点续传记录失败: {missing}")
        # 写入断点日志
        self._write_jsonl(
            self._data_dir("events") / "resume_point_log.jsonl",
            [{"action": "create_resume_point", "resume_code": resume_code, "batch_code": batch_code,
              "completed_chunks": completed_chunks, "pending_chunks": pending_chunks,
              "at": time.time()}]
        )
        return {"resume_code": resume_code, "resume_id": entry["id"], "batch_code": batch_code,
                "completed_chunks": completed_chunks, "pending_chunks": pending_chunks}

    def _source_records(self, batch_code: str, resume_entry: dict[str, Any]) -> list[dict[str, Any]]:
        """读取批次待传输源记录：优先批次产物文件，缺失时按断点登记的总分片数模拟。"""
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        if batch_file.exists():
            return self._read_jsonl(batch_file)
        try:
            total = int(resume_entry.get("已完成分片", 0)) + int(resume_entry.get("未完成分片", 0))
        except (TypeError, ValueError):
            total = 0
        return self._simulate_records(batch_code, count=total or 5, field_prefix="resume")

    def resume_transfer(self, resume_id: int) -> dict[str, Any]:
        """执行断点续传：从实际已交付的批次分片继续，把未落地分片补传到暂存分区。

        本方法实现数据传输的高可用保障与重复写入控制：先用暂存分区的已落地续传
        分片凭据与磁盘数据核对出实际交付进度（异常退出时索引与回执可能滞后，
        以真正落地的行为准），再从该位置继续传输；续传产物写入批次续传文件，
        交付到暂存分区的分片逐片登记凭据，最后把续传水位推进到实际落地进度。
        """
        resume_entry = self.get_entry(resume_id)
        if resume_entry is None:
            return {"error": f"续传断点 {resume_id} 不存在"}

        entry, msg = BaseModuleService.run_action(self, resume_id, "启动续传")
        if entry is None:
            return {"error": msg, "resume_id": resume_id}

        batch_code = resume_entry["批次编号"]
        retry_count = _to_int(resume_entry.get("重试次数")) + 1

        # 实际已交付进度：以暂存分区凭据+磁盘核对为准，而非仅信索引里的恢复位置
        staging_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
        confirm = staging_svc.confirm_landed(batch_code)
        delivered_before = confirm["confirmed"]

        source_records = self._source_records(batch_code, resume_entry)
        pending_records = source_records[delivered_before:]

        # 续传产物文件：记录本轮待补传分片，供链路追溯
        resume_file = self._data_dir("batches") / f"{batch_code}_resume_{retry_count}.jsonl"
        artifact_written = self._write_jsonl(resume_file, pending_records) if pending_records else 0

        # 交付到暂存分区：已确认分片跳过，未落地分片补写并逐片登记凭据
        delivery = staging_svc.deliver_chunks(batch_code, source_records, source="resume")
        new_position = delivery["records_landed"]

        # 续传水位推进到实际落地进度，恢复位置记录本轮起点
        entry["重试次数"] = str(retry_count)
        entry["恢复位置"] = str(delivered_before)
        entry["续传水位"] = str(new_position)

        # 写入续传产物断点
        self._write_checkpoint(f"resume_{batch_code}", {
            "batch_code": batch_code,
            "retry_count": retry_count,
            "resume_position": new_position,
            "delivered_before": delivered_before,
            "records_written": delivery["rewritten"],
            "last_resumed_at": time.time(),
        })

        fully_delivered = len(source_records) > 0 and new_position >= len(source_records)
        if fully_delivered:
            BaseModuleService.run_action(self, resume_id, "确认恢复")

        return {
            "resume_code": resume_entry["断点编号"],
            "batch_code": batch_code,
            "retry_count": retry_count,
            "delivered_before": delivered_before,
            "backfilled_receipts": confirm["backfilled"] + delivery["backfilled_receipts"],
            "records_resumed": delivery["rewritten"],
            "resume_artifact_records": artifact_written,
            "new_position": new_position,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作分派：启动续传执行真实的断点恢复传输，其余动作沿用状态机。"""
        if action == "启动续传":
            result = self.resume_transfer(entry_id)
            if "error" in result:
                return None, result["error"]
            entry = self.get_entry(entry_id)
            return entry, (
                f"续传断点已恢复：本轮起点{result['delivered_before']}行，"
                f"补传{result['records_resumed']}行，续传水位{result['new_position']}"
            )
        return super().run_action(entry_id, action)

    def get_pending_resume_points(self) -> list[dict[str, Any]]:
        """获取所有待续传的断点记录列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "待续传"]
