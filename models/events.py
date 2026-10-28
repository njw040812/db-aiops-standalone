"""事件驱动架构的核心数据模型。

本模块定义 Agent 之间唯一的通信契约。一次数据库故障的完整生命周期是：

    AlertEvent  →  RCAEvent  →  HealEvent(proposed)  →  ChangeEvent(approved)
                                      ↑                         │
                                      └──── HealEvent(executed) ←┘

设计要点（面试可讲）：
1. 每个事件都带 `correlation_id`，等于 `IncidentState.incident_id`，
   这样跨 Kafka topic 也能把一次故障的所有事件串起来。
2. 自愈动作有 `proposed / approved / rejected / succeeded / failed` 五个状态，
   没有 ChangeEvent 的批准，HealAgent 不允许执行（two-phase change）。
3. `IncidentState.timeline` 是内存中的审计轨迹，落盘由 AuditSink 负责。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    """统一使用带时区的 UTC 时间，避免 naive/aware 混用导致比较报错。"""
    return datetime.now(timezone.utc)


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

    @property
    def rank(self) -> int:
        return {
            Severity.CRITICAL: 4,
            Severity.HIGH: 3,
            Severity.MEDIUM: 2,
            Severity.LOW: 1,
            Severity.INFO: 0,
        }[self]


class AlertStatus(str, Enum):
    FIRING = "firing"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class AgentType(str, Enum):
    MONITOR = "monitor"
    RCA = "rca"
    HEAL = "heal"
    CHANGE = "change"
    ORCHESTRATOR = "orchestrator"


class EventType(str, Enum):
    ALERT_FIRED = "alert.fired"
    RCA_STARTED = "rca.started"
    RCA_COMPLETED = "rca.completed"
    RCA_ESCALATED = "rca.escalated"
    HEAL_PROPOSED = "heal.proposed"
    HEAL_APPROVED = "heal.approved"
    HEAL_REJECTED = "heal.rejected"
    HEAL_EXECUTING = "heal.executing"
    HEAL_COMPLETED = "heal.completed"
    HEAL_FAILED = "heal.failed"
    CHANGE_REQUESTED = "change.requested"
    CHANGE_APPROVED = "change.approved"
    CHANGE_REJECTED = "change.rejected"


class HealLevel(str, Enum):
    """自愈分级。级别越高，需要的审批层级越高。"""

    L0_AUTO = "L0"       # 低风险、可逆：自动审批 + 自动执行
    L1_SEMI = "L1"       # 中风险：当班 DBA 确认
    L2_MANUAL = "L2"     # 高风险（DDL / 主从切换）：变更评审 + 人工工单


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=utcnow)
    source_agent: AgentType
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict[str, Any] = Field(default_factory=dict)


class AlertEvent(BaseEvent):
    """监控告警 Agent 产出的告警。

    `algorithm_votes` 保留每个算法的判定，方便事后解释「为什么报警」，
    也是排查误报时唯一可用的证据。
    """

    event_type: EventType = EventType.ALERT_FIRED
    source_agent: AgentType = AgentType.MONITOR

    alert_name: str
    severity: Severity
    status: AlertStatus = AlertStatus.FIRING
    source: str
    target_service: str
    metric_name: Optional[str] = None
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    description: str = ""
    labels: dict[str, str] = Field(default_factory=dict)

    anomaly_score: float = 0.0
    algorithm_votes: list[dict[str, Any]] = Field(default_factory=list)
    dedup_key: str = ""
    suppressed_count: int = 0


class RCAEvent(BaseEvent):
    """根因分析结果。

    `needs_human` 为 True 时，编排器会跳过自愈节点，避免在证据不足时
    「自信地做错事」——这是自愈系统最重要的一个安全阀。
    """

    event_type: EventType = EventType.RCA_COMPLETED
    source_agent: AgentType = AgentType.RCA

    alert_event_id: str
    root_cause: str
    root_cause_service: str
    root_cause_type: str
    confidence: float
    affected_services: list[str]
    evidence: list[dict[str, Any]]
    suggested_actions: list[str]
    impact_score: float = 0.0
    needs_human: bool = False


class HealEvent(BaseEvent):
    """自愈提案 / 执行结果。

    同一个对象贯穿「提案 → 审批 → 执行」，状态字段 `status` 是唯一的
    事实来源；执行前必须存在一条 approval_status=approved 的 ChangeEvent。
    """

    event_type: EventType = EventType.HEAL_PROPOSED
    source_agent: AgentType = AgentType.HEAL

    rca_event_id: str
    heal_level: HealLevel
    action_type: str
    playbook_id: str = ""
    command: str = ""
    action_params: dict[str, Any] = Field(default_factory=dict)
    target_service: str
    estimated_impact: str
    blast_radius: float
    rollback_command: Optional[str] = None
    dry_run_result: Optional[str] = None
    execution_result: Optional[str] = None
    status: str = "proposed"          # proposed / approved / rejected / succeeded / failed
    executor: str = "simulated"
    executed_at: Optional[datetime] = None


class ChangeEvent(BaseEvent):
    """变更审批结果（等价于一张变更工单）。"""

    event_type: EventType = EventType.CHANGE_REQUESTED
    source_agent: AgentType = AgentType.CHANGE

    heal_event_id: str
    risk_score: float
    approval_status: str = "pending"   # pending / approved / rejected
    approver: str = ""
    reason: str = ""
    ticket_id: str = ""
    policy: str = ""
    risk_factors: dict[str, float] = Field(default_factory=dict)
    audit_log: list[dict[str, Any]] = Field(default_factory=list)


class TimelineEntry(BaseModel):
    at: datetime = Field(default_factory=utcnow)
    actor: str
    action: str
    detail: str = ""
    level: str = "info"


class IncidentState(BaseModel):
    """一次故障处理的全局状态，流经编排器的所有节点。"""

    incident_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    status: str = "open"  # open / investigating / proposing / pending_approval / executing / resolved / failed / escalated

    alert_event: Optional[AlertEvent] = None
    rca_event: Optional[RCAEvent] = None
    heal_event: Optional[HealEvent] = None
    change_event: Optional[ChangeEvent] = None

    current_agent: AgentType = AgentType.MONITOR
    error_message: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    timeline: list[TimelineEntry] = Field(default_factory=list)

    def touch(self, actor: str, action: str, detail: str = "", level: str = "info") -> None:
        """追加一条时间线记录，同时刷新 updated_at。"""
        self.timeline.append(
            TimelineEntry(actor=actor, action=action, detail=detail, level=level)
        )
        self.updated_at = utcnow()
