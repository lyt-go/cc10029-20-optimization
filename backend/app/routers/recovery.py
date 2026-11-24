"""断点恢复接口：暂存落地恢复、落地一致性核对与断点续传恢复。

供值班人员在异常退出后再次发起恢复，以及重新进入暂存页面时核对
最后落盘行的实际数量、分区与断点位置是否一致。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import ActionResult
from app.services import service_for
from app.services.resume_point import ResumePointService
from app.services.staging_area import StagingAreaService

router = APIRouter(prefix="/api", tags=["断点恢复"])


def _staging_service() -> StagingAreaService:
    service = service_for("staging_area")
    assert isinstance(service, StagingAreaService)
    return service


def _resume_service() -> ResumePointService:
    service = service_for("resume_point")
    assert isinstance(service, ResumePointService)
    return service


@router.post("/staging_area/{staging_id}/recover", response_model=ActionResult)
def recover_staging(staging_id: int) -> ActionResult:
    """恢复暂存分区落地：凭据+磁盘核对确认已落地分片，补登回执，补写未落地分片。"""
    report = _staging_service().recover_landing(staging_id)
    if "error" in report:
        return ActionResult(ok=False, message=report["error"], entry=report)
    return ActionResult(
        ok=True,
        message=(
            f"暂存分区已恢复：确认{report['confirmed_before']}行，"
            f"补登回执{report['backfilled_receipts']}张，补写{report['rewritten']}行，"
            f"落地水位{report['records_landed']}"
        ),
        entry=report,
    )


@router.get("/staging_area/{staging_id}/verify", response_model=dict[str, Any])
def verify_staging(staging_id: int) -> dict[str, Any]:
    """核对暂存分区：实际数量、分区与断点位置是否一致（只读，不改写数据）。"""
    report = _staging_service().verify_landing(staging_id)
    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])
    return report


@router.post("/resume_point/{resume_id}/resume", response_model=ActionResult)
def resume_transfer(resume_id: int) -> ActionResult:
    """恢复断点续传：从实际已交付的批次分片继续，补传尚未落地的分片。"""
    report = _resume_service().resume_transfer(resume_id)
    if "error" in report:
        return ActionResult(ok=False, message=report["error"], entry=report)
    return ActionResult(
        ok=True,
        message=(
            f"断点续传已恢复：本轮起点{report['delivered_before']}行，"
            f"补传{report['records_resumed']}行，续传水位{report['new_position']}"
        ),
        entry=report,
    )
