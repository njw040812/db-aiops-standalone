"""根因分析：图谱驱动、贝叶斯归一化、置信度不足转人工。"""

from __future__ import annotations

import pytest

from agents.rca_agent import RCAAgent
from models.events import IncidentState, Severity

from tests.conftest import make_alert, make_state


async def test_replica_ddl_root_cause(bus, kg):
    agent = RCAAgent(bus, kg)
    state = await agent.handle(make_state(make_alert()))

    rca = state.rca_event
    assert rca is not None
    assert rca.root_cause_type == "replica_ddl"
    assert rca.confidence >= 0.30
    assert rca.needs_human is False
    assert "kill_ddl" in rca.suggested_actions
    assert rca.affected_services == ["mysql-replica-2", "mysql-primary"]
    assert rca.impact_score == pytest.approx(0.4)
    assert state.status == "proposing"


async def test_posteriors_are_normalized(bus, kg):
    agent = RCAAgent(bus, kg)
    state = await agent.handle(make_state(make_alert()))

    hypotheses = state.rca_event.metadata["hypotheses"]
    assert hypotheses
    assert sum(item["posterior"] for item in hypotheses) == pytest.approx(1.0, abs=1e-3)
    assert hypotheses[0]["type"] == state.rca_event.root_cause_type


async def test_unknown_metric_escalates(bus, kg):
    agent = RCAAgent(bus, kg)
    alert = make_alert(alert_name="mystery_metric", metric_name="mystery_metric")
    state = await agent.handle(make_state(alert))

    assert state.rca_event.needs_human is True
    assert state.rca_event.root_cause_type == "unknown"
    assert state.rca_event.suggested_actions == []
    assert state.status == "escalated"


async def test_low_confidence_escalates(bus, kg):
    agent = RCAAgent(bus, kg, min_confidence=0.99)
    state = await agent.handle(make_state(make_alert()))

    assert state.rca_event.needs_human is True
    assert state.status == "escalated"


async def test_narrow_margin_escalates(bus, kg):
    agent = RCAAgent(bus, kg, min_margin=0.99)
    state = await agent.handle(make_state(make_alert()))

    assert state.rca_event.needs_human is True


async def test_works_without_knowledge_graph(bus):
    agent = RCAAgent(bus, knowledge_graph=None)
    state = await agent.handle(make_state(make_alert()))

    rca = state.rca_event
    assert rca is not None
    assert rca.affected_services == ["mysql-replica-2"]
    assert rca.impact_score == 0.0


async def test_long_transaction_evidence_changes_ranking(bus, kg):
    agent = RCAAgent(bus, kg)
    alert = make_alert()
    alert.metadata["long_transactions"] = 3
    state = await agent.handle(make_state(alert))

    types = [item["type"] for item in state.rca_event.metadata["hypotheses"]]
    assert "long_transaction" in types
    assert any(entry["type"] == "long_transaction" for entry in state.rca_event.evidence)


async def test_high_severity_keeps_metric_evidence(bus, kg):
    agent = RCAAgent(bus, kg)
    state = await agent.handle(make_state(make_alert(severity=Severity.CRITICAL)))
    evidence = {item["type"]: item for item in state.rca_event.evidence}
    assert "metric_anomaly" in evidence
    assert evidence["metric_anomaly"]["weight"] == 0.8


async def test_publishes_to_event_bus(bus, kg):
    received: list[dict] = []

    async def handler(event: dict) -> None:
        received.append(event)

    await bus.subscribe("aiops.events", "test-group", handler)
    agent = RCAAgent(bus, kg)
    await agent.handle(make_state(make_alert()))

    assert len(received) == 1
    assert received[0]["root_cause_type"] == "replica_ddl"
