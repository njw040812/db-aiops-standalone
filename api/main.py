"""FastAPI 入口：把 5 节点工作流暴露成可操作的运维 API。

接口分四组：
- 故障：触发 / 列表 / 详情 / 审批 / 拒绝
- 知识图谱：拓扑摘要 / 服务依赖链
- 检测：单点检测 / 回放 demo
- 运维：Agent 指标 / Playbook / 风险模型 / 审计链校验 / Prometheus 指标

所有状态都放在 app.state 里，方便测试时用 create_app() 构造隔离实例。
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field

from agents.heal_agent import PLAYBOOKS
from agents.change_agent import RISK_WEIGHTS
from config.settings import settings
from core.audit import AuditSink
from core.event_bus import InMemoryEventBus
from core.knowledge_graph import create_demo_knowledge_graph
from core.orchestrator import Orchestrator
from models.events import IncidentState
from models.time_series import EnsembleDetector, generate_db_series, policy_for

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MAX_INCIDENTS = 200


class TriggerRequest(BaseModel):
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    target_service: Optional[str] = None
    labels: dict[str, str] = Field(default_factory=dict)
    long_transactions: int = 0
    slow_queries: int = 0
    error_patterns: list[str] = Field(default_factory=list)
    auto_baseline: bool = True


class DetectionRequest(BaseModel):
    values: list[float]
    current_value: float
    metric_name: str = "mysql_replication_lag_sec"


class ApprovalRequest(BaseModel):
    approver: str = "dba-oncall"
    reason: str = "人工审批通过"


class RejectionRequest(BaseModel):
    approver: str = "dba-oncall"
    reason: str = "人工审批拒绝"


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialize_alert(state: IncidentState) -> Optional[dict[str, Any]]:
    alert = state.alert_event
    if alert is None:
        return None
    return {
        "name": alert.alert_name,
        "severity": alert.severity.value,
        "service": alert.target_service,
        "metric": alert.metric_name,
        "value": alert.metric_value,
        "threshold": alert.threshold,
        "score": alert.anomaly_score,
        "description": alert.description,
        "votes": [
            {
                "algorithm": vote["algorithm"],
                "is_anomaly": vote["is_anomaly"],
                "abstained": vote.get("abstained", False),
                "ratio": vote.get("ratio", 0),
            }
            for vote in alert.algorithm_votes
        ],
    }


def _serialize_rca(state: IncidentState) -> Optional[dict[str, Any]]:
    rca = state.rca_event
    if rca is None:
        return None
    return {
        "root_cause": rca.root_cause,
        "type": rca.root_cause_type,
        "service": rca.root_cause_service,
        "confidence": rca.confidence,
        "needs_human": rca.needs_human,
        "impact_score": rca.impact_score,
        "affected_services": rca.affected_services,
        "evidence": rca.evidence,
        "suggested_actions": rca.suggested_actions,
        "hypotheses": rca.metadata.get("hypotheses", []),
        "margin": rca.metadata.get("posterior_margin"),
    }


def _serialize_heal(state: IncidentState) -> Optional[dict[str, Any]]:
    heal = state.heal_event
    if heal is None:
        return None
    return {
        "playbook": heal.playbook_id,
        "action": heal.action_type,
        "level": heal.heal_level.value,
        "status": heal.status,
        "target": heal.target_service,
        "command": heal.command,
        "blast_radius": heal.blast_radius,
        "dry_run": heal.dry_run_result,
        "execution_result": heal.execution_result,
        "rollback_command": heal.rollback_command,
        "executor": heal.executor,
        "executed_at": heal.executed_at.isoformat() if heal.executed_at else None,
    }


def _serialize_change(state: IncidentState) -> Optional[dict[str, Any]]:
    change = state.change_event
    if change is None:
        return None
    return {
        "status": change.approval_status,
        "risk_score": change.risk_score,
        "risk_factors": change.risk_factors,
        "policy": change.policy,
        "approver": change.approver,
        "reason": change.reason,
        "ticket": change.ticket_id,
        "audit": change.audit_log,
    }


def serialize_incident(state: IncidentState, orchestrator: Orchestrator) -> dict[str, Any]:
    return {
        "incident_id": state.incident_id,
        "status": state.status,
        "created_at": state.created_at.isoformat(),
        "updated_at": state.updated_at.isoformat(),
        "duration_ms": state.metadata.get("duration_ms"),
        "alert": _serialize_alert(state),
        "rca": _serialize_rca(state),
        "heal": _serialize_heal(state),
        "change": _serialize_change(state),
        "timeline": [
            {
                "at": entry.at.isoformat(),
                "actor": entry.actor,
                "action": entry.action,
                "detail": entry.detail,
                "level": entry.level,
            }
            for entry in state.timeline
        ],
        "workflow": orchestrator.get_workflow_status(state.incident_id),
    }


def create_app(
    audit_dir: Optional[str] = None,
    use_kafka: bool = False,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from core.event_bus import create_event_bus

        bus = create_event_bus(use_kafka)
        await bus.start()
        audit = AuditSink(audit_dir=audit_dir or settings.audit_dir)
        await bus.subscribe(settings.topic_audit, "audit-writer", audit.handle)

        orchestrator = Orchestrator(bus)

        app.state.bus = bus
        app.state.audit = audit
        app.state.orchestrator = orchestrator
        app.state.incidents: OrderedDict[str, IncidentState] = OrderedDict()
        app.state.counters = {"triggered": 0, "approved": 0, "rejected": 0, "executed": 0}

        logger.info("DB-AIOps started (audit_dir=%s)", audit._dir)
        yield

        await bus.stop()
        logger.info("DB-AIOps stopped")

    app = FastAPI(
        title="DB-AIOps · 数据库智能运维",
        description="监控告警 → 根因分析 → 自愈提案 → 变更审批 → 审批后执行",
        version="2.0.0",
        lifespan=lifespan,
    )

    def _remember(app: FastAPI, state: IncidentState) -> None:
        store: OrderedDict[str, IncidentState] = app.state.incidents
        store[state.incident_id] = state
        while len(store) > MAX_INCIDENTS:
            store.popitem(last=False)

    # ------------------------------------------------------------------ basic
    @app.get("/")
    async def root():
        return {
            "service": "DB-AIOps",
            "version": "2.0.0",
            "pipeline": ["monitor", "rca", "heal", "change", "execute"],
            "status": "running",
        }

    @app.get("/health")
    async def health():
        return {"status": "healthy", "timestamp": _iso()}

    # --------------------------------------------------------------- incidents
    @app.post("/api/v1/incidents/trigger")
    async def trigger(request: TriggerRequest):
        app_state = app.state
        metadata: dict[str, Any] = {}
        if request.metric_name:
            assert request.metric_value is not None, "metric_value 不能为空"
            metric_data: dict[str, Any] = {
                "metric_name": request.metric_name,
                "value": request.metric_value,
                "service": request.target_service or "mysql-replica-2",
                "labels": request.labels,
            }
            if request.auto_baseline:
                metric_data["baseline"] = generate_db_series(
                    request.metric_name, n_points=120, inject_faults=False
                )[0]
            metadata["metric_data"] = metric_data
        if request.long_transactions:
            metadata["long_transactions"] = request.long_transactions
        if request.slow_queries:
            metadata["slow_queries"] = request.slow_queries
        if request.error_patterns:
            metadata["error_patterns"] = request.error_patterns

        state = await app_state.orchestrator.run(metadata=metadata)
        app_state.counters["triggered"] += 1
        _remember(app, state)
        return serialize_incident(state, app_state.orchestrator)

    @app.get("/api/v1/incidents")
    async def list_incidents(limit: int = 20):
        store: OrderedDict[str, IncidentState] = app.state.incidents
        items = list(store.values())[-min(limit, MAX_INCIDENTS) :]
        return {
            "total": len(store),
            "incidents": [
                {
                    "incident_id": state.incident_id,
                    "status": state.status,
                    "metric": state.alert_event.metric_name if state.alert_event else None,
                    "service": state.alert_event.target_service if state.alert_event else None,
                    "risk_score": state.change_event.risk_score if state.change_event else None,
                    "approval": state.change_event.approval_status if state.change_event else None,
                }
                for state in reversed(items)
            ],
        }

    @app.get("/api/v1/incidents/{incident_id}")
    async def get_incident(incident_id: str):
        state = app.state.incidents.get(incident_id) or app.state.orchestrator.get_incident(incident_id)
        if state is None:
            raise HTTPException(status_code=404, detail="incident not found")
        return serialize_incident(state, app.state.orchestrator)

    @app.post("/api/v1/incidents/{incident_id}/approve")
    async def approve(incident_id: str, request: ApprovalRequest):
        try:
            state = await app.state.orchestrator.resume(
                incident_id, approve=True, approver=request.approver, reason=request.reason
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="incident not found")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        app.state.counters["approved"] += 1
        _remember(app, state)
        return serialize_incident(state, app.state.orchestrator)

    @app.post("/api/v1/incidents/{incident_id}/reject")
    async def reject(incident_id: str, request: RejectionRequest):
        try:
            state = await app.state.orchestrator.resume(
                incident_id, approve=False, approver=request.approver, reason=request.reason
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="incident not found")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        app.state.counters["rejected"] += 1
        _remember(app, state)
        return serialize_incident(state, app.state.orchestrator)

    # ---------------------------------------------------------------- topology
    @app.get("/api/v1/topology")
    async def topology():
        return app.state.orchestrator.knowledge_graph.summary()

    @app.get("/api/v1/topology/{service}/dependencies")
    async def dependencies(service: str):
        kg = app.state.orchestrator.knowledge_graph
        if kg.get_node(service) is None:
            raise HTTPException(status_code=404, detail="service not found in graph")
        return {
            "service": service,
            "type": kg.node_type(service),
            "direct_dependencies": kg.get_dependencies(service),
            "depended_by": kg.get_dependents(service),
            "impact_score": kg.compute_impact_score(service),
            "dependency_paths": kg.paths_to(service, "mysql-primary")[:5],
            "root_candidates": kg.find_root_candidates(service)[:5],
            "recent_changes": kg.find_recent_changes([service], within_hours=24),
        }

    # ---------------------------------------------------------------- detection
    @app.post("/api/v1/anomaly/detect")
    async def detect(request: DetectionRequest):
        policy = policy_for(request.metric_name)
        detector = EnsembleDetector(policy=policy, min_votes=2)
        is_anomaly, ratio, results = detector.detect(request.values, request.current_value)
        return {
            "metric": request.metric_name,
            "direction": policy.direction,
            "is_anomaly": is_anomaly,
            "ratio": ratio,
            "severity": policy.severity_for(request.current_value, ratio) if is_anomaly else "normal",
            "algorithms": [result.to_dict() for result in results],
        }

    @app.get("/api/v1/anomaly/demo")
    async def anomaly_demo(metric: str = "mysql_replication_lag_sec", points: int = 600):
        values, labels, windows = generate_db_series(metric, n_points=points, inject_faults=True)
        policy = policy_for(metric)
        detector = EnsembleDetector(policy=policy, min_votes=2)
        hits: list[dict[str, Any]] = []
        for index in range(policy.min_history, len(values)):
            is_anomaly, ratio, _ = detector.detect(values[:index], values[index])
            if is_anomaly:
                hits.append(
                    {
                        "index": index,
                        "value": round(values[index], 3),
                        "ratio": ratio,
                        "label": labels[index],
                    }
                )
        return {
            "metric": metric,
            "points": len(values),
            "fault_windows": windows,
            "alarms": hits,
            "alarm_count": len(hits),
            "true_positive_points": sum(1 for hit in hits if hit["label"] == 1),
            "false_positive_points": sum(1 for hit in hits if hit["label"] == 0),
        }

    # ----------------------------------------------------------------- ops api
    @app.get("/api/v1/agents/status")
    async def agents_status():
        return {
            "agents": app.state.orchestrator.get_agents_metrics(),
            "workflow": app.state.orchestrator.get_workflow_status(),
            "event_log_size": len(getattr(app.state.bus, "get_event_log", list)()),
        }

    @app.get("/api/v1/playbooks")
    async def playbooks():
        return {
            "total": len(PLAYBOOKS),
            "playbooks": [
                {
                    "id": item.id,
                    "level": item.level.value,
                    "description": item.description,
                    "blast_radius": item.blast_radius,
                    "risk_weight": item.risk_weight,
                    "ddl": item.ddl,
                    "touches_primary": item.touches_primary,
                    "requires_window": item.requires_window,
                    "estimated_duration_sec": item.estimated_duration_sec,
                }
                for item in PLAYBOOKS.values()
            ],
        }

    @app.get("/api/v1/risk-model")
    async def risk_model():
        return {
            "weights": RISK_WEIGHTS,
            "auto_approve_threshold": settings.change_auto_approve_threshold,
            "manual_threshold": settings.change_manual_threshold,
            "max_blast_radius": settings.heal_max_blast_radius,
        }

    @app.get("/api/v1/audit/verify")
    async def audit_verify():
        return AuditSink.verify_dir(app.state.audit._dir)

    @app.get("/api/v1/audit/tail")
    async def audit_tail(limit: int = 10):
        return {"records": app.state.audit.tail(limit)}

    @app.get("/metrics")
    async def metrics():
        counters = app.state.counters
        lines = [
            "# HELP dbaiops_incidents_total 触发过的故障总数",
            "# TYPE dbaiops_incidents_total counter",
            f"dbaiops_incidents_total {counters['triggered']}",
            "# HELP dbaiops_approvals_total 人工审批次数",
            "# TYPE dbaiops_approvals_total counter",
            f'dbaiops_approvals_total{{result="approved"}} {counters["approved"]}',
            f'dbaiops_approvals_total{{result="rejected"}} {counters["rejected"]}',
        ]
        for agent in app.state.orchestrator.get_agents_metrics():
            name = agent["agent"]
            lines += [
                f'dbaiops_agent_processed_total{{agent="{name}"}} {agent["processed_count"]}',
                f'dbaiops_agent_errors_total{{agent="{name}"}} {agent["error_count"]}',
                f'dbaiops_agent_latency_ms{{agent="{name}",quantile="0.95"}} {agent["p95_latency_ms"]}',
            ]
        return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    return app


app = create_app()
