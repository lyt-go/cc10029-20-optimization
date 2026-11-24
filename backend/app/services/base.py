"""模块服务基类：共享查询、登记、动作、文件IO和跨模块数据流骨架，提供版本化产物读写与断点续做。

本基类为 18 个领域服务提供统一的文件持久化、校验值计算、跨模块自动触发和
断点恢复能力。所有写入操作通过 jsonl 文件实现版本化产物存储与水位推进。
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from app.store import store

DATA_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "data"


def _ensure_dir(path: Path) -> None:
    """创建目录及其父目录，已存在则静默跳过。"""
    path.mkdir(parents=True, exist_ok=True)


def _to_int(value: Any, default: int = 0) -> int:
    """把登记值安全解析为整数；示例数据中的占位文本按默认值处理。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # -- 基础 CRUD --

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，汇总当前数据集状态分布与异常记录数，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }

    # -- 文件 IO 持久化工具 --

    def _data_dir(self, sub: str) -> Path:
        """返回数据子目录路径并确保存在，用于存储版本化产物。"""
        p = DATA_ROOT / sub
        _ensure_dir(p)
        return p

    def _write_jsonl(self, filepath: Path, records: list[dict[str, Any]]) -> int:
        """将记录列表追加写入 jsonl 文件并落盘（flush+fsync），返回写入条数。

        数据先落盘是断点续传凭据模型的前提：只有写盘成功的分片才允许登记
        已落地续传分片凭据，异常退出后恢复流程才能以磁盘实际内容为准。
        """
        _ensure_dir(filepath.parent)
        written = 0
        with open(filepath, "a", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
                written += 1
            f.flush()
            os.fsync(f.fileno())
        return written

    def _read_jsonl(self, filepath: Path) -> list[dict[str, Any]]:
        """从磁盘 jsonl 文件中读取全部记录，解析为标准字典列表。"""
        if not filepath.exists():
            return []
        records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def _write_checkpoint(self, name: str, state: dict[str, Any]) -> Path:
        """将进度状态写入断点文件，支持跨模块水位推进与中断恢复。"""
        p = self._data_dir("checkpoints")
        cp_file = p / f"{name}.json"
        state["_updated_at"] = time.time()
        with open(cp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, default=str)
        return cp_file

    def _read_checkpoint(self, name: str) -> dict[str, Any] | None:
        """读取指定断点文件的状态快照，文件不存在返回 None。"""
        cp_file = self._data_dir("checkpoints") / f"{name}.json"
        if not cp_file.exists():
            return None
        with open(cp_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _compute_checksum(self, content: str) -> str:
        """计算字符串内容的 SHA256 校验值并返回前 16 位十六进制摘要。"""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    def _checksum_rows(self, rows: list[dict[str, Any]]) -> str:
        """对一组数据行计算内容校验值，键序无关，用于分片凭据与磁盘数据核对。"""
        canonical = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str)
        return self._compute_checksum(canonical)

    # -- 已落地续传分片凭据（断点续传→暂存落地的重复写入控制） --

    def _receipt_file(self, batch_code: str) -> Path:
        """返回指定批次的已落地续传分片凭据日志文件路径（每批次一份 append-only 日志）。"""
        return self._data_dir("receipts") / f"landed_chunks_{batch_code}.jsonl"

    def _append_chunk_receipt(self, batch_code: str, receipt: dict[str, Any]) -> Path:
        """追加一条已落地续传分片凭据并立即落盘。

        凭据必须在对应分片数据写盘成功之后登记（先数据后凭据）；恢复流程以
        “磁盘实际数据 + 已持久化凭据”的交集为准，凭据缺失的数据会被补登，
        凭据超前于数据的分片不会被当作已完成。
        """
        receipt_file = self._receipt_file(batch_code)
        _ensure_dir(receipt_file.parent)
        line = json.dumps(receipt, ensure_ascii=False, default=str) + "\n"
        with open(receipt_file, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        return receipt_file

    def _read_chunk_receipts(self, batch_code: str, artifact: str | None = None) -> list[dict[str, Any]]:
        """读取指定批次的已落地续传分片凭据，可按落地产物文件过滤。"""
        receipts = self._read_jsonl(self._receipt_file(batch_code))
        if artifact is not None:
            receipts = [r for r in receipts if r.get("artifact") == artifact]
        return receipts

    def _reconcile_landed_chunks(
        self,
        batch_code: str,
        artifact_rel: str,
        chunk_size: int,
        *,
        backfill: bool,
        receipt_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """核对批次落地产物：返回凭据与磁盘共同确认的实际落地进度。

        确认规则（续传进度必须和真正已落地的行对得上）：
        1. 凭据按落盘区间连续校验（偏移衔接、区间不超磁盘实际行数、内容校验值一致），
           只承认连续有效前缀，尚未落地的分片不会被提前视为完成；
        2. 磁盘上已有数据但凭据缺失（异常退出前回执未持久化）的区间，
           当 backfill=True 时按磁盘实际内容补登凭据，使续传回执与落地行对齐。
        """
        rows = self._read_jsonl(DATA_ROOT / artifact_rel)
        actual = len(rows)
        receipts = self._read_chunk_receipts(batch_code, artifact_rel)
        receipts.sort(key=lambda r: (int(r.get("offset_start", 0)), int(r.get("offset_end", 0))))

        confirmed = 0
        valid_receipts: list[dict[str, Any]] = []
        while True:
            nxt = next(
                (
                    r for r in receipts
                    if int(r.get("offset_start", -1)) == confirmed
                    and int(r.get("offset_end", 0)) <= actual
                    and r.get("checksum") == self._checksum_rows(
                        rows[confirmed:int(r.get("offset_end", 0))]
                    )
                ),
                None,
            )
            if nxt is None:
                break
            confirmed = int(nxt["offset_end"])
            valid_receipts.append(nxt)

        receipt_confirmed = confirmed
        backfilled = 0
        if backfill and actual > confirmed and chunk_size > 0:
            start = confirmed
            while start < actual:
                end = min(start + chunk_size, actual)
                shard = rows[start:end]
                self._append_chunk_receipt(batch_code, {
                    "receipt_id": f"RCPT-{batch_code}-{start // chunk_size}-backfill-{int(time.time() * 1000)}",
                    "batch_code": batch_code,
                    "source": "recovery-backfill",
                    "artifact": artifact_rel,
                    "chunk_index": start // chunk_size,
                    "offset_start": start,
                    "offset_end": end,
                    "record_count": len(shard),
                    "checksum": self._checksum_rows(shard),
                    "delivered_at": time.time(),
                    **(receipt_extra or {}),
                })
                backfilled += 1
                start = end
            confirmed = actual

        return {
            "confirmed": confirmed,
            "receipt_confirmed": receipt_confirmed,
            "actual": actual,
            "untracked": actual - receipt_confirmed,
            "valid_receipts": valid_receipts,
            "backfilled": backfilled,
        }

    # -- 跨模块自动触发帮助器 --

    @staticmethod
    def _safe_import(module_path: str, class_name: str):
        """延迟导入避免循环依赖，动态加载指定模块中的类。"""
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)

    def _auto_create_seed(self, module: str, overrides: dict[str, Any]) -> dict[str, Any] | None:
        """跨模块自动创建配套配置条目，写入 store 数据表并返回已生效的记录。"""
        from app.store import store
        rows = store.rows(module)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        entry = {"id": new_id}
        for field, default_val in overrides.items():
            entry[field] = default_val
        entry["status"] = "已生效"
        entry["pending"] = False
        entry["abnormal"] = False
        rows.append(entry)
        return entry

    # -- 批次数据模拟生成工具 --

    def _simulate_records(self, batch_key: str, count: int = 10, field_prefix: str = "field") -> list[dict[str, Any]]:
        """模拟生成一批数据记录作为测试输入，每条记录携带批次编号、行号和模拟字段取值。"""
        records = []
        for i in range(count):
            records.append({
                "batch_key": batch_key,
                "line_no": i + 1,
                f"{field_prefix}_a": f"val_{i}_a",
                f"{field_prefix}_b": f"val_{i}_b",
                f"{field_prefix}_c": i * 10,
                "created_at": time.time(),
            })
        return records
