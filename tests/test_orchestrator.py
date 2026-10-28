"""编排器：完整链路、失败即停、重试隔离、审批中断与恢复。"""

from __future__ import annotations

import pytest

from core.orchestrator import NodeSpec, NodeStatus, Orchestrator


def _status_map(orchestrator: Orchestrator, incident_id: str) -> dict[str, str]:
    return {item["name"]: item["status"] for item in orchestrator.get_workflow_status(incident_id)}


async def test_full_pipeline_resolves(bus):
    orchestrator = Orchestrator(bus)
    state = await orchestrator.run()

    assert state.status == "resolved"
    statuses = _status_map(orchestrator, state.incident_id)
    assert statuses == {
        "monitor": "completed",
        "rca": "completed",
        "heal": "completed",
        "change": "completed",
        "execute": "completed",
    }
    assert state.alert_event and state.rca_event and state.heal_event and state.change_event
    assert state.heal_event.status == "succeeded"
    assert state.metadata["duration_ms"] >= 0
    assert all(item["attempts"] == 1 for item in orchestrator.get_workflow_status())


async def test_second_run_does_not_inherit_retry_counts(bus):
    orchestrator = Orchestrator(bus)
    first = await orchestrator.run()
    assert _status_map(orchestrator, first.incident_id)["monitor"] == "completed"

    second = await orchestrator.run()

    monitor = orchestrator.get_workflow_status(second.incident_id)[0]
    assert monitor["attempts"] == 1
    assert second.incident_id != first.incident_id


async def test_failure_halts_workflow(bus):
    calls: list[str] = []

    async def bad(state):
        calls.append("bad")
        raise RuntimeError("boom")

    async def after(state):
        calls.append("after")
        return state

    orchestrator = Orchestrator(
        bus,
        nodes=[
            NodeSpec("bad", bad, max_retries=1),
            NodeSpec("after", after),
        ],
    )
    state = await orchestrator.run()

    assert calls == ["bad", "bad"]
    assert state.status == "failed"
    runs = orchestrator.get_workflow_status()
    assert runs[0]["attempts"] == 2
    assert runs[0]["status"] == "failed"
    assert "after" not in [run["name"] for run in runs]


async def test_retry_then_success(bus):
    attempts = {"count": 0}

    async def flaky(state):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("temporary")
        return state

    orchestrator = Orchestrator(bus, nodes=[NodeSpec("flaky", flaky, max_retries=2)])
    state = await orchestrator.run()

    assert state.status == "open"
    assert attempts["count"] == 3
    assert orchestrator.get_workflow_status()[0]["status"] == "completed"


async def test_low_confidence_skips_heal_and_execute(bus):
    orchestrator = Orchestrator(bus)
    orchestrator.rca._min_confidence = 0.99

    state = await orchestrator.run()

    statuses = _status_map(orchestrator, state.incident_id)
    assert state.status == "escalated"
    assert statuses["rca"] == "completed"
    assert statuses["heal"] == "skipped"
    assert statuses["change"] == "skipped"
    assert statuses["execute"] == "skipped"
    assert state.heal_event is None


async def test_pending_approval_stops_before_execute(bus):
    orchestrator = Orchestrator(bus)
    orchestrator.change._auto_threshold = -1.0
    orchestrator.change._manual_threshold = -1.0

    state = await orchestrator.run()

    assert state.status == "pending_approval"
    assert state.change_event.approval_status == "pending"
    assert state.heal_event.execution_result is None
    assert _status_map(orchestrator, state.incident_id)["execute"] == "skipped"


async def test_resume_approve_executes_only_execute_node(bus):
    orchestrator = Orchestrator(bus)
    orchestrator.change._auto_threshold = -1.0
    orchestrator.change._manual_threshold = -1.0
    state = await orchestrator.run()
    incident_id = state.incident_id

    resumed = await orchestrator.resume(incident_id, approve=True, approver="dba-tech-lead")

    assert resumed.status == "resolved"
    assert resumed.change_event.approver == "dba-tech-lead"
    assert resumed.heal_event.status == "succeeded"
    statuses = _status_map(orchestrator, incident_id)
    assert statuses["execute"] == "completed"


async def test_resume_reject_does_not_execute(bus):
    orchestrator = Orchestrator(bus)
    orchestrator.change._auto_threshold = -1.0
    orchestrator.change._manual_threshold = -1.0
    state = await orchestrator.run()

    resumed = await orchestrator.resume(state.incident_id, approve=False, approver="dba-tech-lead", reason="拒绝")

    assert resumed.status == "rejected"
    assert resumed.heal_event.status == "rejected"
    assert resumed.heal_event.execution_result is None


async def test_resume_unknown_incident_raises(bus):
    orchestrator = Orchestrator(bus)
    with pytest.raises(KeyError):
        await orchestrator.resume("not-exist")


async def test_resume_without_pending_change_raises(bus):
    orchestrator = Orchestrator(bus)
    state = await orchestrator.run()
    with pytest.raises(ValueError):
        await orchestrator.resume(state.incident_id)


async def test_checkpoint_is_deep_copy(bus):
    orchestrator = Orchestrator(bus)
    state = await orchestrator.run()
    checkpoint = orchestrator.get_checkpoint(state.incident_id)
    assert checkpoint is not state
    assert checkpoint.incident_id == state.incident_id
    checkpoint.metadata["mutated"] = True
    assert "mutated" not in orchestrator.get_checkpoint(state.incident_id).metadata


async def test_workflow_status_includes_agent_metrics(bus):
    orchestrator = Orchestrator(bus)
    await orchestrator.run()

    statuses = orchestrator.get_workflow_status()
    monitor = next(item for item in statuses if item["name"] == "monitor")
    assert monitor["agent"]["processed_count"] == 1
    assert monitor["agent"]["error_count"] == 0
    assert monitor["agent"]["avg_latency_ms"] >= 0


async def test_custom_node_condition_can_skip(bus):
    called: list[str] = []

    async def handler(state):
        called.append("x")
        return state

    orchestrator = Orchestrator(
        bus,
        nodes=[NodeSpec("never", handler, condition=lambda _: False)],
    )
    state = await orchestrator.run()

    assert called == []
    assert orchestrator.get_workflow_status()[0]["status"] == NodeStatus.SKIPPED.value
