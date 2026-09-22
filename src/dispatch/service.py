"""服务装配：参考数据 + 日志 + 引擎 + 后台调度节拍。"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from .config import Config
from .engine import Engine
from .journal import Journal
from .reference import ReferenceData
from .timeutil import now


class Service:
    def __init__(self, config: Config | None = None, *, start_scheduler: bool = False) -> None:
        self.config = config or Config()
        self.config.ensure()
        self.ref = ReferenceData(Path(self.config.reference_dir))
        self.journal = Journal(self.config.data_dir)
        self.engine = Engine(self.ref, self.journal, self.config)
        self._scheduler_started = False
        if self.config.seed_sample:
            self.load_sample_receipts()
        if start_scheduler:
            self.start_scheduler()

    def load_sample_receipts(self) -> list[str]:
        """将 reference 中的离线回执样例按发生时间补录（工单不存在则跳过）。"""
        path = Path(self.config.reference_dir) / "sample_receipts.jsonl"
        loaded = []
        if not path.exists():
            return loaded
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if payload.get("workOrderId") not in self.engine.orders:
                continue
            result = self.engine.ingest_receipt(payload)
            if result.get("created"):
                loaded.append(payload["receiptId"])
        return loaded

    def start_scheduler(self) -> None:
        if self._scheduler_started:
            return
        self._scheduler_started = True
        thread = threading.Thread(
            target=self._run_scheduler, name="dispatch-scheduler", daemon=True
        )
        thread.start()

    def _run_scheduler(self) -> None:
        import time
        while True:
            time.sleep(self.config.tick_seconds)
            try:
                self.engine.tick()
            except Exception:  # 后台节拍异常不能拖垮进程
                continue


def build_service(*, data_dir: str | None = None, reference_dir: str | None = None,
                  start_scheduler: bool = False, env: dict | None = None) -> Service:
    import os
    old = {}
    for key, value in {
        "DISPATCH_DATA_DIR": data_dir,
        "DISPATCH_REFERENCE_DIR": reference_dir,
    }.items():
        old[key] = os.environ.get(key)
        if value is not None:
            os.environ[key] = value
        elif key in os.environ:
            del os.environ[key]
    if env:
        for key, value in env.items():
            old.setdefault(key, os.environ.get(key))
            os.environ[key] = value
    try:
        return Service(start_scheduler=start_scheduler)
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
