"""时间工具：统一带时区的 ISO 8601 字符串，默认时区 Asia/Shanghai。"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def now() -> datetime:
    return datetime.now(LOCAL_TZ)


def parse(value: str | datetime | None, *, default: datetime | None = None) -> datetime | None:
    if value is None:
        return default
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).strip())
    if dt.tzinfo is None:
        # 业务约定时区为 Asia/Shanghai，裸时间按本地时区解释
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt


def format(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ).isoformat(timespec="seconds")


def utc_stamp(dt: datetime) -> float:
    return dt.astimezone(timezone.utc).timestamp()


def compact(dt: datetime) -> str:
    """事件/工单号使用的 YYYYMMDDHHMM。"""
    return dt.astimezone(LOCAL_TZ).strftime("%Y%m%d%H%M")
