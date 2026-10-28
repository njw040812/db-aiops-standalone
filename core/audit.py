"""审计日志：哈希链 + 按天追加写。

为什么不用普通日志？
运维变更审计的合规要求是「不可静默篡改」。这里每条记录都带
`prev_hash` 和 `hash = SHA256(prev_hash + 规范化JSON(payload+seq+ts))`，
删掉/改动任意一条，后续链条都会校验失败。

存储：`var/audit/audit-YYYYMMDD.jsonl`（UTC 日期轮转），只追加不覆盖。
校验：`verify_dir()` 跨文件顺序校验整条链。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

GENESIS_HASH = "0" * 64


def canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def compute_hash(prev_hash: str, payload: Any) -> str:
    return hashlib.sha256((prev_hash + canonical_json(payload)).encode("utf-8")).hexdigest()


class AuditSink:
    """订阅 aiops.audit，把变更事件写入哈希链文件。"""

    def __init__(self, audit_dir: str | Path = "var/audit", filename_prefix: str = "audit"):
        self._dir = Path(audit_dir)
        self._prefix = filename_prefix
        self._seq = 0
        self._last_hash = GENESIS_HASH
        self._recent: deque[dict[str, Any]] = deque(maxlen=200)
        self._lock = asyncio.Lock()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._restore_tail()

    def _restore_tail(self) -> None:
        """重启后续上最后一条记录的 seq / hash，保证链条连续。"""
        files = sorted(self._dir.glob(f"{self._prefix}-*.jsonl"))
        if not files:
            return
        try:
            with files[-1].open("r", encoding="utf-8") as fh:
                last_line = None
                count = 0
                for line in fh:
                    if line.strip():
                        last_line = line
                        count += 1
                if last_line:
                    record = json.loads(last_line)
                    self._seq = int(record.get("seq", count))
                    self._last_hash = record.get("hash", GENESIS_HASH)
        except (OSError, ValueError) as exc:
            logger.warning("[Audit] cannot restore tail: %s", exc)

    def _path_for(self, ts: datetime) -> Path:
        return self._dir / f"{self._prefix}-{ts.astimezone(timezone.utc):%Y%m%d}.jsonl"

    async def handle(self, event: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            ts = datetime.now(timezone.utc)
            self._seq += 1
            payload = {
                "seq": self._seq,
                "ts": ts.isoformat(),
                "kind": event.get("event_type", "unknown"),
                "payload": event,
                "prev_hash": self._last_hash,
            }
            record = {**payload, "hash": compute_hash(self._last_hash, payload)}
            self._last_hash = record["hash"]

            path = self._path_for(ts)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            self._recent.append(record)
            logger.info("[Audit] #%d %s -> %s", self._seq, record["kind"], path.name)
            return record

    @property
    def records(self) -> list[dict[str, Any]]:
        return list(self._recent)

    def tail(self, n: int = 5) -> list[dict[str, Any]]:
        return list(self._recent)[-n:]

    # ------------------------------------------------------------------ verify
    @staticmethod
    def verify_dir(path: str | Path) -> dict[str, Any]:
        """按文件顺序校验整条哈希链。"""
        directory = Path(path)
        files = sorted(directory.glob("audit-*.jsonl"))
        if not files:
            return {"ok": True, "entries": 0, "files": 0, "broken_at": None}

        expected_prev = GENESIS_HASH
        entries = 0
        for file in files:
            with file.open("r", encoding="utf-8") as fh:
                for lineno, line in enumerate(fh, start=1):
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    payload = {k: record[k] for k in ("seq", "ts", "kind", "payload", "prev_hash")}
                    if record.get("prev_hash") != expected_prev:
                        return {
                            "ok": False,
                            "entries": entries,
                            "files": len(files),
                            "broken_at": {"file": file.name, "line": lineno, "reason": "prev_hash mismatch"},
                        }
                    if compute_hash(expected_prev, payload) != record.get("hash"):
                        return {
                            "ok": False,
                            "entries": entries,
                            "files": len(files),
                            "broken_at": {"file": file.name, "line": lineno, "reason": "hash mismatch"},
                        }
                    expected_prev = record["hash"]
                    entries += 1

        return {"ok": True, "entries": entries, "files": len(files), "broken_at": None}

    @staticmethod
    def verify_file(path: str | Path, expected_prev: Optional[str] = None) -> dict[str, Any]:
        expected = expected_prev or GENESIS_HASH
        entries = 0
        with Path(path).open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                record = json.loads(line)
                payload = {k: record[k] for k in ("seq", "ts", "kind", "payload", "prev_hash")}
                if record.get("prev_hash") != expected:
                    return {"ok": False, "entries": entries, "broken_at": lineno, "reason": "prev_hash mismatch"}
                if compute_hash(expected, payload) != record.get("hash"):
                    return {"ok": False, "entries": entries, "broken_at": lineno, "reason": "hash mismatch"}
                expected = record["hash"]
                entries += 1
        return {"ok": True, "entries": entries, "broken_at": None}
