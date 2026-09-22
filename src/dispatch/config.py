"""运行时配置：落盘位置与调度阈值全部来自环境变量。"""
from __future__ import annotations

import os
from pathlib import Path


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


class Config:
    def __init__(self) -> None:
        self.data_dir = Path(os.environ.get("DISPATCH_DATA_DIR", ".runtime/data")).resolve()
        self.reference_dir = Path(
            os.environ.get("DISPATCH_REFERENCE_DIR", "reference/dispatch")
        )
        # 未显式指定参考目录时，相对仓库根定位，避免依赖启动目录
        if not os.environ.get("DISPATCH_REFERENCE_DIR") and not self.reference_dir.exists():
            here = Path(__file__).resolve()
            candidate = here.parents[2] / "reference" / "dispatch"
            if candidate.exists():
                self.reference_dir = candidate
        self.tick_seconds = _int("DISPATCH_TICK_SECONDS", 15)
        self.ack_timeout_seconds = _int("DISPATCH_ACK_TIMEOUT_SECONDS", 300)
        self.restore_timeout_seconds = _int("DISPATCH_RESTORE_TIMEOUT_SECONDS", 5400)
        self.seed_sample = os.environ.get("DISPATCH_SEED_SAMPLE", "") == "1"

    def ensure(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
