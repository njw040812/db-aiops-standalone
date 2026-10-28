"""变更审批：6 因子风险模型、分级策略、工单、审计。"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from agents.change_agent import RISK_WEIGHTS, ChangeAgent, RiskScorer
from models.events import HealEvent, HealLevel, IncidentState


def make_heal(
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
        command="mysql -e \"KILL QUERY 1;\"",
        action_params=params
        or {"risk_weight": 0.45, "requires_window": False, "touches_primary": False},
        target_service="mysql-replica-2",
        estimated_impact="测试",
        blast_radius=blast,
        correlation_id="incident-1",
    )


async def _review(bus, heal: HealEvent, now: datetime | None = None) -> IncidentState:
    agent = ChangeAgent(bus, now=now)
    state = IncidentState()
    state.heal_event = heal
    return await agent.handle(state)


def test_risk_weights_sum_to_one():
    assert sum(RISK_WEIGHTS.values()) == pytest.approx(1.0)
    assert set(RISK_WEIGHTS) == {"blast_radius", "operation", "window", "criticality", "history", "data_loss"}


def test_scorer_is_monotonic_in_blast_radius():
    scorer = RiskScorer()
    small, _ = scorer.compute("kill_ddl", 0.05, "mysql-replica-2", HealLevel.L1_SEMI, {"risk_weight": 0.45})
    large, _ = scorer.compute("kill_ddl", 0.25, "mysql-replica-2", HealLevel.L1_SEMI, {"risk_weight": 0.45})
    assert large > small


def test_scorer_penalizes_business_hours_window_operations():
    scorer = RiskScorer()
    business, _ = scorer.compute(
        "add_index_online",
        0.25,
        "mysql-primary",
        HealLevel.L2_MANUAL,
        {"risk_weight": 0.8, "requires_window": True, "touches_primary": True},
        hour=14,
    )
    night, _ = scorer.compute(
        "add_index_online",
        0.25,
        "mysql-primary",
        HealLevel.L2_MANUAL,
        {"risk_weight": 0.8, "requires_window": True, "touches_primary": True},
        hour=3,
    )
    assert business > night


async def test_l0_low_risk_auto_approved(bus):
    state = await _review(
        bus,
        make_heal(action="recycle_connection_pool", level=HealLevel.L0_AUTO, blast=0.02, params={"risk_weight": 0.1}),
    )

    assert state.change_event.approval_status == "approved"
    assert state.change_event.approver == "auto-system"
    assert state.change_event.policy == "L0_AUTO_APPROVED"
    assert state.heal_event.status == "approved"


async def test_l1_oncall_approved_at_night(bus):
    night = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    state = await _review(bus, make_heal(), now=night)

    assert state.change_event.approval_status == "approved"
    assert state.change_event.approver == "dba-oncall"
    assert state.change_event.policy == "L1_ONCALL_APPROVED"


async def test_l2_generates_ticket_and_waits(bus):
    night = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    state = await _review(
        bus,
        make_heal(
            action="add_index_online",
            level=HealLevel.L2_MANUAL,
            blast=0.25,
            params={"risk_weight": 0.8, "requires_window": True, "touches_primary": True},
        ),
        now=night,
    )

    change = state.change_event
    assert change.approval_status == "pending"
    assert change.policy == "L2_TICKET_REQUIRED"
    assert change.ticket_id.startswith("CHG-")
    assert state.status == "pending_approval"
    assert state.heal_event.status == "proposed"


async def test_ddl_waits_for_change_window_in_business_hours(bus):
    business = datetime(2026, 10, 28, 14, 0, tzinfo=timezone.utc)
    state = await _review(
        bus,
        make_heal(
            action="add_index_online",
            level=HealLevel.L2_MANUAL,
            blast=0.25,
            params={"risk_weight": 0.8, "requires_window": True, "touches_primary": True},
        ),
        now=business,
    )

    assert state.change_event.approval_status == "pending"
    assert state.change_event.policy == "CHANGE_WINDOW_REQUIRED"


async def test_high_risk_l1_waits(bus):
    night = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    state = await _review(
        bus,
        make_heal(action="rollback_config", level=HealLevel.L1_SEMI, blast=0.9, params={"risk_weight": 0.9}),
        now=night,
    )

    assert state.change_event.approval_status == "pending"
    assert state.change_event.policy == "L1_PENDING"


async def test_audit_entry_contains_risk_factors(bus):
    state = await _review(bus, make_heal())
    entry = state.change_event.audit_log[0]

    assert entry["action"] == "change_approved"
    assert entry["metadata"]["risk_score"] == state.change_event.risk_score
    assert set(entry["metadata"]["risk_factors"]) == set(RISK_WEIGHTS)
    assert entry["target"] == "mysql-replica-2"


async def test_manual_approve_appends_audit(bus):
    night = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    agent = ChangeAgent(bus, now=night)
    state = IncidentState()
    state.heal_event = make_heal(
        action="add_index_online",
        level=HealLevel.L2_MANUAL,
        blast=0.25,
        params={"risk_weight": 0.8, "requires_window": True, "touches_primary": True},
    )
    state = await agent.handle(state)
    assert state.change_event.approval_status == "pending"

    state = await agent.manual_approve(state, approver="dba-tech-lead", reason="窗口确认")

    assert state.change_event.approval_status == "approved"
    assert state.change_event.approver == "dba-tech-lead"
    assert len(state.change_event.audit_log) == 2
    assert state.change_event.audit_log[-1]["action"] == "change_manual_approved"
    assert state.heal_event.status == "approved"


async def test_manual_reject(bus):
    night = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    agent = ChangeAgent(bus, now=night)
    state = IncidentState()
    state.heal_event = make_heal(
        action="add_index_online",
        level=HealLevel.L2_MANUAL,
        blast=0.25,
        params={"risk_weight": 0.8, "requires_window": True, "touches_primary": True},
    )
    state = await agent.handle(state)

    state = await agent.manual_reject(state, approver="dba-tech-lead", reason="窗口不合适")

    assert state.change_event.approval_status == "rejected"
    assert state.heal_event.status == "rejected"
    assert state.status == "rejected"


async def test_failure_raises_history_risk(bus):
    night = datetime(2026, 10, 28, 2, 0, tzinfo=timezone.utc)
    agent = ChangeAgent(bus, now=night)

    async def score() -> float:
        state = IncidentState()
        state.heal_event = make_heal()
        state = await agent.handle(state)
        return state.change_event.risk_score

    before = await score()
    agent.record_outcome(success=False)
    after = await score()
    assert after > before


async def test_change_publishes_audit_event(bus):
    received: list[dict] = []

    async def handler(event: dict) -> None:
        received.append(event)

    await bus.subscribe("aiops.audit", "test-audit", handler)
    await _review(bus, make_heal())

    assert received
    assert received[0]["event_type"] in {"change.approved", "change.requested"}
