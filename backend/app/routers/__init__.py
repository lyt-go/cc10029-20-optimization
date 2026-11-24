"""业务路由汇总：断点恢复等专用路由先于通用资源路由注册，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import recovery, resource

ROUTERS = [recovery, resource]
