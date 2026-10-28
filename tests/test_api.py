"""API 层：端到端触发、审批、拓扑、检测、审计、指标。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.main import create_app
from models.time_series import generate_db_series


@pytest.fixture
def client(tmp_path):
    app = create_app(audit_dir=str(tmp_path / "audit"))
    with TestClient(app) as test_client:
        yield test_client


def test_root_and_health(client):
    root = client.get("/").json()
    assert root["service"] == "DB-AIOps"
    assert root["pipeline"] == ["monitor", "rca", "heal", "change", "execute"]

    health = client.get("/health").json()
    assert health["status"] == "healthy"


def test_trigger_demo_pipeline(client):
    response = client.post("/api/v1/incidents/trigger", json={})
    assert response.status_code == 200

    body = response.json()
    assert body["status"] == "resolved"
    assert body["alert"]["metric"] == "mysql_replication_lag_sec"
    assert body["rca"]["type"] == "replica_ddl"
    assert body["heal"]["playbook"] == "kill_ddl"
    assert body["heal"]["status"] == "succeeded"
    assert body["change"]["status"] == "approved"
    assert body["change"]["risk_score"] < 0.55
    assert len(body["workflow"]) == 5
    assert all(node["status"] == "completed" for node in body["workflow"])


def test_trigger_custom_metric(client):
    response = client.post(
        "/api/v1/incidents/trigger",
        json={
            "metric_name": "mysql_connections_used_ratio",
            "metric_value": 0.95,
            "target_service": "mysql-primary",
            "long_transactions": 2,
        },
    )
    body = response.json()
    assert response.status_code == 200
    assert body["alert"]["severity"] in {"critical", "high"}
    assert body["rca"] is not None
    assert body["status"] in {"resolved", "pending_approval"}


def test_trigger_without_baseline_has_no_alert(client):
    response = client.post(
        "/api/v1/incidents/trigger",
        json={
            "metric_name": "mysql_replication_lag_sec",
            "metric_value": 99.0,
            "target_service": "mysql-replica-1",
            "auto_baseline": False,
        },
    )
    body = response.json()
    assert body["alert"] is None
    assert body["status"] in {"open", "investigating"}


def test_incident_list_and_detail(client):
    trigger = client.post("/api/v1/incidents/trigger", json={}).json()
    incident_id = trigger["incident_id"]

    listing = client.get("/api/v1/incidents").json()
    assert listing["total"] >= 1
    assert listing["incidents"][0]["incident_id"] == incident_id

    detail = client.get(f"/api/v1/incidents/{incident_id}").json()
    assert detail["incident_id"] == incident_id
    assert detail["timeline"]


def test_incident_detail_404(client):
    assert client.get("/api/v1/incidents/does-not-exist").status_code == 404


def test_approve_pending_incident(client):
    orchestrator = client.app.state.orchestrator
    orchestrator.change._auto_threshold = -1.0
    orchestrator.change._manual_threshold = -1.0

    pending = client.post("/api/v1/incidents/trigger", json={}).json()
    assert pending["status"] == "pending_approval"
    assert pending["change"]["ticket"].startswith("CHG-") or pending["change"]["status"] == "pending"

    approved = client.post(
        f"/api/v1/incidents/{pending['incident_id']}/approve",
        json={"approver": "dba-tech-lead", "reason": "同意执行"},
    )
    assert approved.status_code == 200
    body = approved.json()
    assert body["status"] == "resolved"
    assert body["change"]["approver"] == "dba-tech-lead"
    assert body["heal"]["status"] == "succeeded"


def test_reject_pending_incident(client):
    orchestrator = client.app.state.orchestrator
    orchestrator.change._auto_threshold = -1.0
    orchestrator.change._manual_threshold = -1.0

    pending = client.post("/api/v1/incidents/trigger", json={}).json()
    rejected = client.post(
        f"/api/v1/incidents/{pending['incident_id']}/reject",
        json={"approver": "dba-tech-lead", "reason": "窗口不合适"},
    )
    assert rejected.status_code == 200
    body = rejected.json()
    assert body["status"] == "rejected"
    assert body["heal"]["execution_result"] is None


def test_approve_unknown_incident_404(client):
    response = client.post("/api/v1/incidents/nope/approve", json={})
    assert response.status_code == 404


def test_approve_non_pending_conflict(client):
    resolved = client.post("/api/v1/incidents/trigger", json={}).json()
    response = client.post(f"/api/v1/incidents/{resolved['incident_id']}/approve", json={})
    assert response.status_code == 409


def test_topology(client):
    summary = client.get("/api/v1/topology").json()
    assert summary["total_nodes"] == 13

    deps = client.get("/api/v1/topology/mysql-replica-2/dependencies").json()
    assert deps["direct_dependencies"] == ["mysql-primary"]
    assert deps["impact_score"] == pytest.approx(0.4)
    assert deps["recent_changes"]

    assert client.get("/api/v1/topology/not-a-service/dependencies").status_code == 404


def test_anomaly_detect(client):
    baseline = generate_db_series("mysql_replication_lag_sec", 200, inject_faults=False)[0]
    response = client.post(
        "/api/v1/anomaly/detect",
        json={
            "values": baseline,
            "current_value": 99.0,
            "metric_name": "mysql_replication_lag_sec",
        },
    )
    body = response.json()
    assert body["is_anomaly"] is True
    assert body["severity"] in {"critical", "high"}
    assert len(body["algorithms"]) >= 2

    normal = client.post(
        "/api/v1/anomaly/detect",
        json={
            "values": baseline,
            "current_value": 1.3,
            "metric_name": "mysql_replication_lag_sec",
        },
    ).json()
    assert normal["is_anomaly"] is False


def test_anomaly_demo(client):
    body = client.get("/api/v1/anomaly/demo?points=600").json()
    assert body["points"] == 600
    assert body["fault_windows"]
    assert body["alarm_count"] >= 1
    assert body["true_positive_points"] >= 1


def test_playbooks_and_risk_model(client):
    playbooks = client.get("/api/v1/playbooks").json()
    assert playbooks["total"] == 14
    ids = {item["id"] for item in playbooks["playbooks"]}
    assert {"kill_ddl", "add_index_online", "promote_replica", "recycle_connection_pool"} <= ids

    model = client.get("/api/v1/risk-model").json()
    assert sum(model["weights"].values()) == pytest.approx(1.0)


def test_agents_status(client):
    client.post("/api/v1/incidents/trigger", json={})
    body = client.get("/api/v1/agents/status").json()
    assert len(body["agents"]) == 4
    monitor = next(item for item in body["agents"] if item["type"] == "monitor")
    assert monitor["processed_count"] == 1


def test_audit_chain_after_incident(client):
    client.post("/api/v1/incidents/trigger", json={})
    result = client.get("/api/v1/audit/verify").json()
    assert result["ok"] is True
    assert result["entries"] >= 2

    tail = client.get("/api/v1/audit/tail?limit=5").json()
    assert tail["records"]


def test_metrics_endpoint(client):
    client.post("/api/v1/incidents/trigger", json={})
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "dbaiops_incidents_total 1" in response.text
    assert 'dbaiops_agent_processed_total{agent="MonitorAgent"}' in response.text
