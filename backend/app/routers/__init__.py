"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import recovery, resource, chains

# recovery 必须排在 resource 之前：/api/recovery/... 是固定前缀，
# 否则三段式 GET（如 /api/recovery/packages）会被 /api/{module}/{entry_id} 抢先匹配。
ROUTERS = [recovery, resource, chains]
