"""API 级功能开关与守卫工具。

用于集中管理通过 API 触发的数据写入类操作（如清洗、物化）的开关。
默认禁止通过 API 触发数据任务，设置环境变量 ENABLE_API_DATA_TASKS 为 1/true/on/yes 可整体开启。
可选地，通过 API_DATA_TASKS_DISABLED_MESSAGE 覆盖禁止时的提示文案。
"""

from __future__ import annotations

import os
from typing import Optional

from fastapi import HTTPException, status

_FALSE_VALUES = {"0", "false", "no", "off"}


def _is_disabled(flag_value: Optional[str]) -> bool:
    if flag_value is None:
        return True
    return flag_value.strip().lower() in _FALSE_VALUES


def ensure_data_task_trigger_enabled(action: str) -> None:
    """确保允许通过 API 触发数据任务。

    Args:
        action: 当前操作的描述，用于错误提示。

    Raises:
        HTTPException: 当开关关闭时，以 403 拒绝请求。
    """

    env_value = os.getenv("ENABLE_API_DATA_TASKS")
    if not _is_disabled(env_value):
        return

    message = os.getenv("API_DATA_TASKS_DISABLED_MESSAGE", "数据任务触发已禁用")
    if action:
        message = f"{message}：{action}"
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=message)
