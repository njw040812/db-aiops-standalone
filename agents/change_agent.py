"""变更审批 Agent：风险评分 + 审批策略 + 审计。

风险模型（6 个因子，权重和为 1）：

    blast_radius   0.30   影响面，来自图谱
    operation      0.25   Playbook 固有风险（DDL / 主从切换高）
    window         0.15   是否在业务高峰执行需要窗口的操作
    criticality    0.10   目标实例关键度
    history        0.10   同类操作近期成功率（失败后用 EWMA 抬升）
    data_loss      0.10   是否有数据删除/不可逆风险

审批策略：
    L0 + score<=0.30                → 自动批准
    L0 + score>0.30                 → 转当班
    L1 + score<=0.55                → 当班批准（可执行）
    L1 + score>0.55                 → 转当班挂起
    L2                              → 生成变更工单，必须 DBA TL 批准
    需要变更窗口的 DDL 在业务时段    → 强制挂起，等窗口

审批通过只代表「可以执行」，真正的执行在编排器的 execute 节点，
由 HealAgent 再次校验审批状态。
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from agents.base_agent import BaseAgent
from config.settings import settings
from core.event_bus import EventBus
from models.events import (
    AgentType,
    ChangeEvent,
    EventType,
    HealLevel,
    IncidentState,
)

logger = logging.getLogger(__name__)

RISK_WEIGHTS = {
    "blast_radius": 0.30,
    "operation": 0.25,
    "window": 0.15,
    "criticality": 0.10,
    "history": 0.10,
    "data_loss": 0.10,
}

SERVICE_CRITICALITY = {
    "mysql-primary": 1.0,
    "payment-service": 0.95,
    "api-gateway": 0.90,
    "order-service": 0.80,
    "user-service": 0.70,
    "mysql-replica-1": 0.60,
    "mysql-replica-2": 0.60,
    "report-service": 0.40,
    "redis-cache": 0.65,
}

DATA_LOSS_RISK = {
    "archive_table": 1.0,
    "promote_replica": 0.8,
    "add_index_online": 0.3,
    "kill_long_transaction": 0.3,
    "kill_ddl": 0.2,
    "purge_binlog": 0.2,
    "rollback_config": 0.2,
}


class RiskScorer:
    def __init__(self, history_risk: float = 0.10):
        self._history_risk = history_risk

    def compute(
        self,
        action_type: str,
        blast_radius: float,
        target_service: str,
        level: HealLevel,
        params: Optional[dict[str, Any]] = None,
        hour: Optional[int] = None,
        now: Optional[datetime] = None,
    ) -> tuple[float, dict[str, float]]:
        params = params or {}
        now = now or datetime.now(timezone.utc)
        hour = hour if hour is not None else now.hour
        business_hours = settings.business_hours_start <= hour < settings.business_hours_end

        requires_window = bool(params.get("requires_window"))
        if requires_window:
            window_factor = 0.9 if business_hours else 0.2
        else:
            window_factor = 0.1 if business_hours else 0.3

        criticality = SERVICE_CRITICALITY.get(target_service, 0.5)
        if params.get("touches_primary"):
            criticality = max(criticality, 0.9)

        factors = {
            "blast_radius": round(min(max(blast_radius, 0.0), 1.0), 4),
            "operation": round(float(params.get("risk_weight", 0.5)), 4),
            "window": round(window_factor, 4),
            "criticality": round(criticality, 4),
            "history": round(self._history_risk, 4),
            "data_loss": round(DATA_LOSS_RISK.get(action_type, 0.0), 4),
        }
        score = sum(weight * factors[name] for name, weight in RISK_WEIGHTS.items())
        return round(min(score, 1.0), 4), factors

    def record_outcome(self, success: bool) -> None:
        """失败一次就把历史风险抬高，连续失败会逐步收紧审批阈值。"""
        self._history_risk = round(0.8 * self._history_risk + 0.2 * (0.0 if success else 1.0), 4)


class ChangeAgent(BaseAgent):
    def __init__(
        self,
        event_bus: EventBus,
        auto_approve_threshold: float = settings.change_auto_approve_threshold,
        manual_threshold: float = settings.change_manual_threshold,
        now: Optional[datetime] = None,
    ):
        super().__init__(AgentType.CHANGE, event_bus, "ChangeAgent")
        self._scorer = RiskScorer()
        self._auto_threshold = auto_approve_threshold
        self._manual_threshold = manual_threshold
        self._now = now

    def _clock(self) -> datetime:
        return self._now or datetime.now(timezone.utc)

    # ------------------------------------------------------------------ review
    async def process(self, state: IncidentState) -> IncidentState:
        heal = state.heal_event
        if heal is None:
            raise ValueError("ChangeAgent 需要 heal_event")

        now = self._clock()
        risk_score, factors = self._scorer.compute(
            action_type=heal.action_type,
            blast_radius=heal.blast_radius,
            target_service=heal.target_service,
            level=heal.heal_level,
            params=heal.action_params,
            now=now,
        )

        status, approver, reason, policy = self._decide(heal.heal_level, risk_score, heal.action_params)
        ticket_id = ""
        if status == "pending":
            ticket_id = f"CHG-{now:%Y%m%d}-{uuid.uuid4().hex[:4].upper()}"

        audit_entry = self._audit_entry(
            action=f"change_{status}",
            actor=approver,
            target=heal.target_service,
            reason=reason,
            metadata={
                "risk_score": risk_score,
                "risk_factors": factors,
                "policy": policy,
                "action_type": heal.action_type,
                "level": heal.heal_level.value,
                "incident_id": state.incident_id,
                "ticket_id": ticket_id,
            },
            now=now,
        )

        change_event = ChangeEvent(
            event_type=EventType.CHANGE_APPROVED if status == "approved" else EventType.CHANGE_REQUESTED,
            heal_event_id=heal.event_id,
            risk_score=risk_score,
            approval_status=status,
            approver=approver if status == "approved" else "",
            reason=reason,
            ticket_id=ticket_id,
            policy=policy,
            risk_factors=factors,
            audit_log=[audit_entry],
            correlation_id=state.incident_id,
            metadata={"owner": approver} if status == "pending" else {},
        )

        state.change_event = change_event
        if status == "approved":
            heal.status = "approved"
            heal.event_type = EventType.HEAL_APPROVED
            state.status = "approved"
            state.touch("ChangeAgent", "approved", f"{policy}：{reason}")
        else:
            state.status = "pending_approval"
            state.touch(
                "ChangeAgent",
                "pending",
                f"{policy}：{reason}（工单 {ticket_id}）",
                level="warning",
            )

        await self._publish("aiops.audit", change_event)
        logger.info(
            "[ChangeAgent] %s action=%s risk=%.3f policy=%s",
            status,
            heal.action_type,
            risk_score,
            policy,
        )
        return state

    def _decide(
        self,
        level: HealLevel,
        score: float,
        params: Optional[dict[str, Any]],
    ) -> tuple[str, str, str, str]:
        params = params or {}
        if params.get("requires_window") and settings.business_hours_start <= self._clock().hour < settings.business_hours_end:
            return (
                "pending",
                "dba-oncall",
                f"DDL/切换操作需要变更窗口，业务时段不执行（risk={score:.3f}）",
                "CHANGE_WINDOW_REQUIRED",
            )
        if level == HealLevel.L2_MANUAL:
            return (
                "pending",
                "dba-tech-lead",
                f"L2 高风险变更，需 DBA TL 审批（risk={score:.3f}）",
                "L2_TICKET_REQUIRED",
            )
        if level == HealLevel.L1_SEMI:
            if score <= self._manual_threshold:
                return (
                    "approved",
                    "dba-oncall",
                    f"当班 DBA 批准（risk={score:.3f} <= {self._manual_threshold}）",
                    "L1_ONCALL_APPROVED",
                )
            return (
                "pending",
                "dba-oncall",
                f"L1 风险偏高，挂起等待人工确认（risk={score:.3f}）",
                "L1_PENDING",
            )
        # L0
        if score <= self._auto_threshold:
            return (
                "approved",
                "auto-system",
                f"低风险自动放行（risk={score:.3f} <= {self._auto_threshold}）",
                "L0_AUTO_APPROVED",
            )
        return (
            "pending",
            "dba-oncall",
            f"L0 风险超出自动阈值，转当班确认（risk={score:.3f}）",
            "L0_PENDING",
        )

    # ------------------------------------------------------- manual operations
    async def manual_approve(
        self,
        state: IncidentState,
        approver: str = "dba-oncall",
        reason: str = "人工审批通过",
    ) -> IncidentState:
        change = state.change_event
        if change is None:
            raise ValueError("没有待审批的变更单")
        if change.approval_status != "pending":
            return state

        now = self._clock()
        change.approval_status = "approved"
        change.event_type = EventType.CHANGE_APPROVED
        change.approver = approver
        change.reason = reason
        change.audit_log.append(
            self._audit_entry(
                action="change_manual_approved",
                actor=approver,
                target=state.heal_event.target_service if state.heal_event else "unknown",
                reason=reason,
                metadata={"ticket_id": change.ticket_id, "risk_score": change.risk_score, "incident_id": state.incident_id},
                now=now,
            )
        )
        if state.heal_event:
            state.heal_event.status = "approved"
            state.heal_event.event_type = EventType.HEAL_APPROVED
        state.status = "approved"
        state.touch("ChangeAgent", "manual_approved", f"{approver}：{reason}")
        await self._publish("aiops.audit", change)
        return state

    async def manual_reject(
        self,
        state: IncidentState,
        approver: str = "dba-oncall",
        reason: str = "人工审批拒绝",
    ) -> IncidentState:
        change = state.change_event
        if change is None:
            raise ValueError("没有待审批的变更单")

        change.approval_status = "rejected"
        change.event_type = EventType.CHANGE_REJECTED
        change.approver = approver
        change.reason = reason
        change.audit_log.append(
            self._audit_entry(
                action="change_manual_rejected",
                actor=approver,
                target=state.heal_event.target_service if state.heal_event else "unknown",
                reason=reason,
                metadata={"ticket_id": change.ticket_id, "risk_score": change.risk_score},
                now=self._clock(),
            )
        )
        if state.heal_event:
            state.heal_event.status = "rejected"
            state.heal_event.event_type = EventType.HEAL_REJECTED
        state.status = "rejected"
        state.touch("ChangeAgent", "manual_rejected", f"{approver}：{reason}", level="warning")
        await self._publish("aiops.audit", change)
        return state

    def record_outcome(self, success: bool) -> None:
        self._scorer.record_outcome(success)

    @staticmethod
    def _audit_entry(
        action: str,
        actor: str,
        target: str,
        reason: str,
        metadata: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        return {
            "timestamp": now.isoformat(),
            "action": action,
            "actor": actor,
            "target": target,
            "reason": reason,
            "metadata": metadata,
        }

    def get_risk_weights(self) -> dict[str, float]:
        return dict(RISK_WEIGHTS)
