from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional, Type

from sqlalchemy.types import TypeDecorator, String, Text


class JSONText(TypeDecorator):
    impl = Text
    cache_ok = True

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    def process_result_value(self, value: Any, dialect) -> Any:
        if value is None:
            return {}
        if isinstance(value, (dict, list)):
            return value
        value_str = str(value).strip()
        if not value_str:
            return {}
        try:
            return json.loads(value_str)
        except json.JSONDecodeError:
            return value


class IntString(TypeDecorator):
    impl = String(255)
    cache_ok = True

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        if value is None:
            return None
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return None

    def process_result_value(self, value: Any, dialect) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0


class FloatString(TypeDecorator):
    impl = String(255)
    cache_ok = True

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        if value is None:
            return None
        try:
            return str(float(value))
        except (TypeError, ValueError):
            return None

    def process_result_value(self, value: Any, dialect) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


class DateTimeString(TypeDecorator):
    impl = String(255)
    cache_ok = True
    _formats = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        raise TypeError(f"Unsupported datetime value: {value!r}")

    def process_result_value(self, value: Any, dialect) -> Optional[datetime]:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value
        for fmt in self._formats:
            try:
                return datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None


class EnumString(TypeDecorator):
    impl = String(255)
    cache_ok = True

    def __init__(self, enum_cls: Type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enum_cls = enum_cls

    def process_bind_param(self, value: Any, dialect) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, self._enum_cls):
            return value.value
        return str(value).lower()

    def process_result_value(self, value: Any, dialect) -> Optional[Any]:
        if value in (None, ""):
            return None
        if isinstance(value, self._enum_cls):
            return value
        str_value = str(value)
        try:
            return self._enum_cls(str_value)
        except ValueError:
            try:
                return self._enum_cls(str_value.lower())
            except ValueError:
                return None
