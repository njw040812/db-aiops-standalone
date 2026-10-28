"""审计链：追加写、跨文件连续、篡改检测。"""

from __future__ import annotations

import json
from pathlib import Path

from core.audit import AuditSink, GENESIS_HASH


async def _write_three(sink: AuditSink) -> Path:
    for index in range(3):
        await sink.handle({"event_type": "change.approved", "correlation_id": f"c-{index}"})
    files = sorted(Path(sink._dir).glob("audit-*.jsonl"))
    assert len(files) == 1
    return files[0]


async def test_chain_is_valid(tmp_path):
    sink = AuditSink(tmp_path)
    await _write_three(sink)

    result = AuditSink.verify_dir(tmp_path)
    assert result["ok"] is True
    assert result["entries"] == 3
    assert result["files"] == 1


async def test_first_record_starts_from_genesis(tmp_path):
    sink = AuditSink(tmp_path)
    path = await _write_three(sink)

    first = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert first["prev_hash"] == GENESIS_HASH
    assert first["seq"] == 1


async def test_tampering_is_detected(tmp_path):
    sink = AuditSink(tmp_path)
    path = await _write_three(sink)

    lines = path.read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[1])
    record["payload"]["event_type"] = "change.rejected"
    lines[1] = json.dumps(record, ensure_ascii=False, default=str)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = AuditSink.verify_dir(tmp_path)
    assert result["ok"] is False
    assert result["broken_at"]["reason"] in {"hash mismatch", "prev_hash mismatch"}


async def test_deleting_a_record_breaks_chain(tmp_path):
    sink = AuditSink(tmp_path)
    path = await _write_three(sink)

    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("\n".join([lines[0], lines[2]]) + "\n", encoding="utf-8")

    result = AuditSink.verify_dir(tmp_path)
    assert result["ok"] is False
    assert result["broken_at"]["reason"] == "prev_hash mismatch"


async def test_chain_continues_after_restart(tmp_path):
    first = AuditSink(tmp_path)
    await _write_three(first)

    second = AuditSink(tmp_path)
    assert second._seq == 3
    await second.handle({"event_type": "heal.completed"})

    result = AuditSink.verify_dir(tmp_path)
    assert result["ok"] is True
    assert result["entries"] == 4


async def test_tail_returns_recent_records(tmp_path):
    sink = AuditSink(tmp_path)
    await _write_three(sink)

    tail = sink.tail(2)
    assert [record["seq"] for record in tail] == [2, 3]


async def test_verify_empty_dir_is_ok(tmp_path):
    assert AuditSink.verify_dir(tmp_path) == {
        "ok": True,
        "entries": 0,
        "files": 0,
        "broken_at": None,
    }
