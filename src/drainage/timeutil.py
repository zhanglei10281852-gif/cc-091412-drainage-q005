"""时间工具：统一带时区 ISO 8601，班次按 Asia/Shanghai 判断。"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def now() -> datetime:
    return datetime.now(timezone.utc).astimezone(SHANGHAI)


def now_iso() -> str:
    return now().isoformat(timespec="seconds")


def parse(value: str | None) -> datetime:
    """解析 ISO 8601；缺省时取当前时间。无时区按上海时区处理。"""
    if not value:
        return now()
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SHANGHAI)
    return dt.astimezone(SHANGHAI)


def iso(dt: datetime) -> str:
    return dt.astimezone(SHANGHAI).isoformat(timespec="seconds")


def _parse_hhmm(value: str) -> time:
    if value == "24:00":
        return time.max
    hour, minute = value.split(":")
    return time(int(hour), int(minute))


def on_shift(shift: dict, moment: datetime) -> bool:
    local = moment.astimezone(SHANGHAI).time()
    start = _parse_hhmm(shift["start"])
    end = _parse_hhmm(shift["end"])
    if start <= end:
        return start <= local < end
    # 跨夜班次
    return local >= start or local < end


def eta_window(departure: datetime, distance_m: int, speed_m_per_min: int,
               slack: tuple[int, int]) -> tuple[str, str]:
    travel = timedelta(minutes=distance_m / speed_m_per_min)
    return iso(departure + travel + timedelta(minutes=slack[0])), iso(
        departure + travel + timedelta(minutes=slack[1])
    )
