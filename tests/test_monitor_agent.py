"""监控告警 Agent：检测、分级、去重。"""

from __future__ import annotations

import pytest

from agents.monitor_agent import MonitorAgent
from models.events import IncidentState, Severity
from models.time_series import generate_db_series


def _baseline() -> list[float]:
    return generate_db_series("mysql_replication_lag_sec", n_points=120, inject_faults=False)[0]


def _sample(value: float, service: str = "mysql-replica-2", metric: str = "mysql_replication_lag_sec"):
    return {
        "metric_name": metric,
        "value": value,
        "service": service,
        "labels": {"host": "db-node-3"},
        "baseline": _baseline(),
    }


async def test_normal_value_does_not_alert(bus):
    agent = MonitorAgent(bus)
    state = IncidentState(metadata={"metric_data": _sample(1.4)})

    state = await agent.handle(state)

    assert state.alert_event is None
    assert any(entry.action == "no_anomaly" for entry in state.timeline)


async def test_spike_fires_critical_alert(bus):
    agent = MonitorAgent(bus)
    state = IncidentState(metadata={"metric_data": _sample(46.2)})

    state = await agent.handle(state)

    alert = state.alert_event
    assert alert is not None
    assert alert.severity == Severity.CRITICAL
    assert alert.metric_name == "mysql_replication_lag_sec"
    active = [vote for vote in alert.algorithm_votes if not vote["abstained"]]
    assert len(active) >= 2
    assert sum(1 for vote in active if vote["is_anomaly"]) >= 2
    assert alert.dedup_key


async def test_duplicate_alert_is_suppressed(bus):
    agent = MonitorAgent(bus, dedup_window_sec=300)

    first = await agent.handle(IncidentState(metadata={"metric_data": _sample(46.2)}))
    assert first.alert_event is not None

    second = await agent.handle(IncidentState(metadata={"metric_data": _sample(46.2)}))
    assert second.alert_event is None
    assert second.metadata.get("suppressed") == 1


async def test_cold_start_without_baseline_waits(bus):
    agent = MonitorAgent(bus)
    state = IncidentState(
        metadata={
            "metric_data": {
                "metric_name": "mysql_replication_lag_sec",
                "value": 99.0,
                "service": "mysql-replica-1",
                "labels": {},
            }
        }
    )

    state = await agent.handle(state)

    assert state.alert_event is None
    assert any(entry.action == "warmup" for entry in state.timeline)
    assert len(agent.get_history("mysql-replica-1", "mysql_replication_lag_sec")) == 1


async def test_alert_is_published_to_event_bus(bus):
    received: list[dict] = []

    async def handler(event: dict) -> None:
        received.append(event)

    await bus.subscribe("aiops.alerts", "test-group", handler)
    agent = MonitorAgent(bus)
    await agent.handle(IncidentState(metadata={"metric_data": _sample(46.2)}))

    assert len(received) == 1
    assert received[0]["event_type"] == "alert.fired"
    assert received[0]["metric_value"] == 46.2


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.95, Severity.CRITICAL), (0.80, Severity.HIGH), (0.70, Severity.LOW)],
)
async def test_connection_ratio_severity(bus, value, expected):
    agent = MonitorAgent(bus)
    baseline = generate_db_series("mysql_connections_used_ratio", 120, False)[0]
    state = IncidentState(
        metadata={
            "metric_data": {
                "metric_name": "mysql_connections_used_ratio",
                "value": value,
                "service": "mysql-primary",
                "labels": {},
                "baseline": baseline,
            }
        }
    )

    state = await agent.handle(state)

    assert state.alert_event is not None
    assert state.alert_event.severity == expected
