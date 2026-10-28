"""自愈 Agent：提案与执行分离、审批门禁、dry-run、熔断、爆炸半径。"""

from __future__ import annotations

import pytest

from agents.heal_agent import (
    CircuitBreaker,
    HealAgent,
    Playbook,
    SimulatedExecutor,
)
from agents.rca_agent import RCAAgent
from models.events import ChangeEvent, HealEvent, HealLevel, IncidentState

from tests.conftest import make_alert, make_state


class TrackingExecutor(SimulatedExecutor):
    def __init__(self) -> None:
        self.runs: list[str] = []

    async def run(self, command: str) -> str:
        self.runs.append(command)
        return await super().run(command)


class FailingExecutor(SimulatedExecutor):
    async def run(self, command: str) -> str:
        raise RuntimeError("模拟执行失败")


def make_heal_event(
    action: str = "kill_ddl",
    level: HealLevel = HealLevel.L1_SEMI,
    blast: float = 0.08,
    params: dict | None = None,
) -> HealEvent:
    return HealEvent(
        rca_event_id="rca-1",
        heal_level=level,
        action_type=action,
        playbook_id=action,
        command="mysql -h db-node-3 -e \"KILL QUERY 1;\"",
        action_params=params
        or {"risk_weight": 0.45, "requires_window": False, "touches_primary": False},
        target_service="mysql-replica-2",
        estimated_impact="测试剧本",
        blast_radius=blast,
        correlation_id="incident-1",
    )


def approved_state(heal: HealEvent) -> IncidentState:
    state = IncidentState()
    state.heal_event = heal
    state.change_event = ChangeEvent(
        heal_event_id=heal.event_id,
        risk_score=0.2,
        approval_status="approved",
        approver="dba-oncall",
        correlation_id=state.incident_id,
    )
    return state


async def _proposed_state(bus, kg) -> IncidentState:
    rca = RCAAgent(bus, kg)
    state = await rca.handle(make_state(make_alert()))
    heal = HealAgent(bus)
    return await heal.handle(state)


async def test_proposal_only_does_not_execute(bus, kg):
    rca = RCAAgent(bus, kg)
    state = await rca.handle(make_state(make_alert()))
    executor = TrackingExecutor()
    heal = HealAgent(bus, executor=executor)

    state = await heal.handle(state)

    event = state.heal_event
    assert event is not None
    assert event.status == "proposed"
    assert event.execution_result is None
    assert event.dry_run_result.startswith("DRY-RUN OK")
    assert executor.runs == []
    assert state.status == "pending_approval"


async def test_execute_without_approval_is_rejected(bus):
    heal = HealAgent(bus)
    state = IncidentState()
    state.heal_event = make_heal_event()
    state.change_event = ChangeEvent(
        heal_event_id=state.heal_event.event_id,
        risk_score=0.2,
        approval_status="pending",
    )

    with pytest.raises(PermissionError):
        await heal.execute(state)


async def test_execute_after_approval(bus):
    executor = TrackingExecutor()
    heal = HealAgent(bus, executor=executor)
    state = approved_state(make_heal_event())

    state = await heal.execute(state)

    assert state.heal_event.status == "succeeded"
    assert state.status == "resolved"
    assert len(executor.runs) == 1
    assert "SUCCESS" in state.heal_event.execution_result


async def test_execute_is_idempotent(bus):
    executor = TrackingExecutor()
    heal = HealAgent(bus, executor=executor)
    state = approved_state(make_heal_event())

    state = await heal.execute(state)
    state = await heal.execute(state)

    assert len(executor.runs) == 1
    assert any(entry.action == "idempotent_skip" for entry in state.timeline)


async def test_blast_radius_escalates_to_l2(bus, kg):
    rca = RCAAgent(bus, kg)
    state = await rca.handle(make_state(make_alert()))
    big = Playbook(
        id="big_change",
        action_type="big_change",
        level=HealLevel.L1_SEMI,
        description="高风险大范围变更",
        command_template="db-config rollback --service {service}",
        blast_radius=0.9,
        risk_weight=0.9,
        estimated_duration_sec=10,
    )
    heal = HealAgent(bus, playbooks={"big_change": big})
    state.rca_event.suggested_actions = ["big_change"]

    state = await heal.handle(state)

    assert state.heal_event.heal_level == HealLevel.L2_MANUAL
    assert "强制升级 L2" in state.heal_event.metadata["escalation_reason"]


async def test_missing_playbook_escalates(bus, kg):
    rca = RCAAgent(bus, kg)
    state = await rca.handle(make_state(make_alert()))
    state.rca_event.suggested_actions = ["not_a_playbook"]

    state = await HealAgent(bus).handle(state)

    assert state.status == "escalated"
    assert state.heal_event is None


async def test_preflight_blocks_dangerous_command():
    executor = SimulatedExecutor()
    with pytest.raises(ValueError):
        executor.preflight('mysql -e "DROP TABLE t_order;"')
    with pytest.raises(ValueError):
        executor.preflight("unknown-binary --do-something")
    with pytest.raises(ValueError):
        executor.preflight("")


async def test_circuit_breaker_blocks_proposal(bus, kg):
    rca = RCAAgent(bus, kg)
    state = await rca.handle(make_state(make_alert()))
    breaker = CircuitBreaker(threshold=1, timeout_sec=60)
    breaker.record_failure()
    heal = HealAgent(bus, circuit_breaker=breaker)

    state = await heal.handle(state)

    assert state.status == "blocked"
    assert state.heal_event is None


async def test_failure_marks_incident_failed(bus):
    heal = HealAgent(bus, executor=FailingExecutor())

    state = await heal.execute(approved_state(make_heal_event()))

    assert state.heal_event.status == "failed"
    assert state.status == "failed"
    assert heal.circuit_breaker.failures == 1
    assert "FAILED" in state.heal_event.execution_result


async def test_rollback_attempted_on_failure(bus):
    heal = HealAgent(bus, executor=FailingExecutor())
    event = make_heal_event(action="add_index_online", level=HealLevel.L2_MANUAL, blast=0.25)
    event.rollback_command = "gh-ost --panic"
    state = await heal.execute(approved_state(event))

    assert "ROLLBACK" in state.heal_event.execution_result


async def test_preflight_failure_escalates(bus, kg):
    rca = RCAAgent(bus, kg)
    state = await rca.handle(make_state(make_alert()))
    bad = Playbook(
        id="bad",
        action_type="bad",
        level=HealLevel.L0_AUTO,
        description="危险命令",
        command_template="rm -rf {service}",
        blast_radius=0.1,
        risk_weight=0.1,
        estimated_duration_sec=1,
    )
    heal = HealAgent(bus, playbooks={"bad": bad})
    state.rca_event.suggested_actions = ["bad"]

    state = await heal.handle(state)

    assert state.status == "escalated"
    assert state.heal_event is None
