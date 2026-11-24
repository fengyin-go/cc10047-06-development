"""pytest 公共夹具：每个测试前清空异地恢复登记处，保证用例相互隔离。"""
from __future__ import annotations

import sys
sys.path.insert(0, '.')

import pytest

from app.recovery import recovery_registry


@pytest.fixture(autouse=True)
def reset_recovery_registry():
    # 彻底复位：恢复作业、目标表与离线数据包均按用例隔离，避免包摘要撞键。
    recovery_registry.reset_all()
    yield
    recovery_registry.reset_all()
