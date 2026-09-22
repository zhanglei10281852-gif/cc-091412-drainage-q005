"""事件溯源存储：JSONL 原子追加（fsync），重启重放。

所有状态变更先落盘再应用到内存；测试使用独立 DATA_DIR。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path


class EventStore:
    def __init__(self, data_dir: str | Path | None = None):
        base = Path(data_dir) if data_dir else Path(os.environ.get("DATA_DIR", ".data"))
        base.mkdir(parents=True, exist_ok=True)
        self.path = base / "events.log"
        self._lock = threading.Lock()
        self._fh = None

    def append(self, event: dict) -> dict:
        """落盘并返回事件；同一把锁保证并发写入顺序与可见性。"""
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        return event

    def replay(self):
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"事件日志损坏: {self.path}:{lineno}") from exc
