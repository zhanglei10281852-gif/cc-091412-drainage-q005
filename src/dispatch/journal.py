"""仅追加日志（journal）。

所有处置事实按条目追加到 journal.jsonl，重启时重放即可恢复全部状态，
因此未签收工单与已产生的超时提醒不会丢失。写入带 fsync，并按键去重保证幂等。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any


class Journal:
    def __init__(self, data_dir) -> None:
        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
        self.path = data_dir / "journal.jsonl"
        self._lock = threading.Lock()
        self.seen: set[str] = set()
        self.entries: list[dict[str, Any]] = []
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    entry = json.loads(line)
                    self.entries.append(entry)
                    self.seen.add(entry["id"])

    def append_many(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """原子性地追加一批因果相关条目，返回真正写入的条目（去重后的丢弃）。"""
        written: list[dict[str, Any]] = []
        with self._lock, self.path.open("a", encoding="utf-8") as fh:
            for entry in entries:
                if entry["id"] in self.seen:
                    continue
                self.seen.add(entry["id"])
                self.entries.append(entry)
                fh.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
                written.append(entry)
            fh.flush()
            os.fsync(fh.fileno())
        return written

    def append(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        rows = self.append_many([entry])
        return rows[0] if rows else None
