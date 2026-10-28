"""pytest 公共夹具：把项目根加入 sys.path，并提供构造事件的辅助函数。"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.event_bus import InMemoryEventBus  # noqa: E402
from core.knowledge_graph import create_demo_knowledge_graph  # noqa: E402
from models.events import AlertEvent, IncidentState, Severity  # noqa: E402


@pytest.fixture
def bus() -> InMemoryEventBus:
    return InMemoryEventBus()


@pytest.fixture
def kg():
    return create_demo_knowledge_graph()


@pytest.fixture
def now() -> datetime:
    return datetime.now(timezone.utc)


def make_alert(**overrides) -> AlertEvent:
    payload = {
        "alert_name": "mysql_replication_lag_sec",
        "severity": Severity.CRITICAL,
        "source": "prometheus",
        "target_service": "mysql-replica-2",
        "metric_name": "mysql_replication_lag_sec",
        "metric_value": 46.2,
        "threshold": 5.0,
        "labels": {"host": "db-node-3"},
    }
    payload.update(overrides)
    return AlertEvent(**payload)


def make_state(alert: AlertEvent | None = None) -> IncidentState:
    state = IncidentState()
    state.alert_event = alert or make_alert()
    state.status = "investigating"
    return state
