"""Agent 编排器：状态机 + 条件路由 + 重试 + 检查点 + 审批中断点。

默认工作流（5 个节点）：

    monitor → rca → heal(提案) → change(审批) → execute(执行)
                │        │            │
                │        └─ 无 Playbook → 转人工（跳过后续）
                └─ 证据不足/置信度低 → escalated（跳过自愈）

与原实现的关键差异：
1. **节点状态按 incident 隔离**：原来 retry_count 和 status 挂在共享的
   WorkflowNode 上，第二次跑同一个 Orchestrator 时重试计数是累加的；
2. **失败即停**：根因分析失败后不会继续执行自愈，避免「在错误信息上动手」；
3. **异常向上抛**：BaseAgent 不再吞异常，重试才有意义；
4. **审批是中断点**：pending 的变更单停在 change 节点，通过 `resume()` 人工
   审批后只执行 execute 节点，不会重跑监控和根因；
5. **检查点**：每步完成后深拷贝状态，resume 从断点继续。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

from agents.base_agent import BaseAgent
from agents.change_agent import ChangeAgent
from agents.heal_agent import HealAgent
from agents.monitor_agent import MonitorAgent
from agents.rca_agent import RCAAgent
from config.settings import settings
from core.event_bus import InMemoryEventBus
from core.knowledge_graph import InMemoryKnowledgeGraph, create_demo_knowledge_graph
from models.events import IncidentState

logger = logging.getLogger(__name__)

NodeHandler = Callable[[IncidentState], Awaitable[IncidentState]]
Condition = Callable[[IncidentState], bool]


class NodeStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class NodeSpec:
    name: str
    handler: NodeHandler
    condition: Optional[Condition] = None
    max_retries: int = 1
    agent: Optional[BaseAgent] = None
    description: str = ""

    def should_run(self, state: IncidentState) -> bool:
        return self.condition is None or self.condition(state)


@dataclass
class NodeRun:
    name: str
    status: NodeStatus = NodeStatus.PENDING
    attempts: int = 0
    error: str = ""
    duration_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "attempts": self.attempts,
            "error": self.error,
            "duration_ms": round(self.duration_ms, 2),
        }


class Orchestrator:
    def __init__(
        self,
        event_bus,
        knowledge_graph: Optional[InMemoryKnowledgeGraph] = None,
        nodes: Optional[list[NodeSpec]] = None,
        max_retries: int = 1,
    ):
        self.event_bus = event_bus
        self.knowledge_graph = knowledge_graph or create_demo_knowledge_graph()
        self._checkpoints: dict[str, IncidentState] = {}
        self._node_runs: dict[str, list[NodeRun]] = {}
        self._node_states: dict[str, dict[str, NodeStatus]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._max_retries = max_retries

        self.monitor = MonitorAgent(event_bus)
        self.rca = RCAAgent(event_bus, self.knowledge_graph)
        self.heal = HealAgent(event_bus)
        self.change = ChangeAgent(event_bus)

        self._nodes = nodes or self._build_default_workflow()

    # ----------------------------------------------------------------- workflow
    def _build_default_workflow(self) -> list[NodeSpec]:
        return [
            NodeSpec(
                "monitor",
                self.monitor.handle,
                condition=None,
                agent=self.monitor,
                description="异常检测 + 告警分级 + 告警收敛",
            ),
            NodeSpec(
                "rca",
                self.rca.handle,
                condition=lambda s: s.alert_event is not None,
                agent=self.rca,
                description="图谱依赖链 + 变更关联 + 归一化贝叶斯根因",
            ),
            NodeSpec(
                "heal",
                self.heal.handle,
                condition=lambda s: s.rca_event is not None and not s.rca_event.needs_human,
                agent=self.heal,
                description="Playbook 匹配 + dry-run 预检（只提案，不执行）",
            ),
            NodeSpec(
                "change",
                self.change.handle,
                condition=lambda s: s.heal_event is not None and s.heal_event.status == "proposed",
                agent=self.change,
                description="6 因子风险评分 + 审批策略 + 审计",
            ),
            NodeSpec(
                "execute",
                self._execute_node,
                condition=lambda s: (
                    s.heal_event is not None
                    and s.change_event is not None
                    and s.change_event.approval_status == "approved"
                ),
                description="校验审批后执行，失败自动回滚 + 熔断",
            ),
        ]

    async def run(
        self,
        initial_state: Optional[IncidentState] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> IncidentState:
        state = initial_state or IncidentState()
        if metadata:
            state.metadata = {**state.metadata, **metadata}

        started = time.perf_counter()
        state.touch("Orchestrator", "start", f"故障 {state.incident_id} 进入处理流程")
        runs: list[NodeRun] = []
        self._node_runs[state.incident_id] = runs
        self._node_states[state.incident_id] = {}

        for spec in self._nodes:
            if not spec.should_run(state):
                run = NodeRun(spec.name, NodeStatus.SKIPPED)
                runs.append(run)
                self._node_states[state.incident_id][spec.name] = NodeStatus.SKIPPED
                state.touch("Orchestrator", "skip", f"{spec.name} 条件不满足，跳过")
                continue

            run = NodeRun(spec.name, NodeStatus.RUNNING)
            runs.append(run)
            ok = await self._run_node(spec, state, run)
            if not ok:
                state.status = "failed"
                state.touch(
                    "Orchestrator",
                    "halt",
                    f"{spec.name} 执行失败，工作流终止（不在失败分析上继续执行变更）",
                    level="error",
                )
                break

        state.metadata["duration_ms"] = round((time.perf_counter() - started) * 1000, 2)
        state.touch("Orchestrator", "finish", f"状态 {state.status}，用时 {state.metadata['duration_ms']}ms")
        state.updated_at = datetime.now(timezone.utc)
        self._save_checkpoint(state)
        return state

    async def _run_node(self, spec: NodeSpec, state: IncidentState, run: NodeRun) -> bool:
        for attempt in range(1, spec.max_retries + 2):
            run.attempts = attempt
            started = time.perf_counter()
            try:
                state = await spec.handler(state)
                run.status = NodeStatus.COMPLETED
                run.duration_ms += (time.perf_counter() - started) * 1000
                self._node_states[state.incident_id][spec.name] = NodeStatus.COMPLETED
                return True
            except Exception as exc:
                run.duration_ms += (time.perf_counter() - started) * 1000
                run.error = str(exc)
                logger.warning("[Orchestrator] %s 第 %d 次尝试失败: %s", spec.name, attempt, exc)
                if attempt <= spec.max_retries:
                    state.touch(
                        "Orchestrator",
                        "retry",
                        f"{spec.name} 第 {attempt} 次失败，准备重试：{exc}",
                        level="warning",
                    )
                    await asyncio.sleep(0)
                    continue
                run.status = NodeStatus.FAILED
                self._node_states[state.incident_id][spec.name] = NodeStatus.FAILED
                return False
        return False

    async def _execute_node(self, state: IncidentState) -> IncidentState:
        state = await self.heal.execute(state)
        if state.heal_event is not None:
            self.change.record_outcome(state.heal_event.status == "succeeded")
        return state

    # -------------------------------------------------------------- checkpoint
    def _save_checkpoint(self, state: IncidentState) -> None:
        self._checkpoints[state.incident_id] = state.model_copy(deep=True)

    def get_checkpoint(self, incident_id: str) -> Optional[IncidentState]:
        """返回深拷贝，避免调用方改坏内部断点。"""
        state = self._checkpoints.get(incident_id)
        return state.model_copy(deep=True) if state is not None else None

    def get_incident(self, incident_id: str) -> Optional[IncidentState]:
        return self.get_checkpoint(incident_id)

    async def resume(
        self,
        incident_id: str,
        approve: bool = True,
        approver: str = "dba-oncall",
        reason: str = "人工审批",
    ) -> IncidentState:
        """人工审批后从断点继续：只跑 execute 节点，不重跑监控/根因。"""
        state = self._checkpoints.get(incident_id)
        if state is None:
            raise KeyError(f"incident {incident_id} 不存在")

        lock = self._locks.setdefault(incident_id, asyncio.Lock())
        async with lock:
            if state.change_event is None or state.change_event.approval_status != "pending":
                raise ValueError("该故障没有待审批的变更单")

            if approve:
                state = await self.change.manual_approve(state, approver, reason)
            else:
                state = await self.change.manual_reject(state, approver, reason)
                self._save_checkpoint(state)
                return state

            execute_spec = next((n for n in self._nodes if n.name == "execute"), None)
            if execute_spec is None:
                raise RuntimeError("工作流中没有 execute 节点")

            run = NodeRun("execute", NodeStatus.RUNNING)
            self._node_runs.setdefault(incident_id, []).append(run)
            ok = await self._run_node(execute_spec, state, run)
            if not ok:
                state.status = "failed"
            self._save_checkpoint(state)
            return state

    def get_workflow_status(self, incident_id: Optional[str] = None) -> list[dict[str, Any]]:
        if incident_id is None:
            if not self._node_runs:
                return []
            incident_id = next(reversed(self._node_runs))
        runs = self._node_runs.get(incident_id, [])
        statuses: list[dict[str, Any]] = []
        for run in runs:
            item = run.to_dict()
            spec = next((n for n in self._nodes if n.name == run.name), None)
            if spec and spec.agent is not None:
                item["agent"] = spec.agent.get_metrics()
            statuses.append(item)
        return statuses

    def get_agents_metrics(self) -> list[dict[str, Any]]:
        return [agent.get_metrics() for agent in (self.monitor, self.rca, self.heal, self.change)]


# ------------------------------------------------------------------------- demo
def _fmt_chain(services: list[str]) -> str:
    return " → ".join(services)


async def run_demo(verbose: bool = True) -> IncidentState:
    from core.audit import AuditSink

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")

    bus = InMemoryEventBus()
    await bus.start()
    audit = AuditSink(audit_dir=settings.audit_dir)
    await bus.subscribe(settings.topic_audit, "audit-writer", audit.handle)

    orchestrator = Orchestrator(bus)
    state = await orchestrator.run()

    if verbose:
        print("\n" + "=" * 72)
        print("  DB-AIOps 故障处理报告")
        print("=" * 72)
        print(f"  incident_id : {state.incident_id}")
        print(f"  最终状态    : {state.status}")
        print(f"  全链路耗时  : {state.metadata.get('duration_ms')} ms")

        if state.alert_event:
            alert = state.alert_event
            print("\n  [1] 监控告警")
            print(f"      指标      : {alert.metric_name} = {alert.metric_value}")
            print(f"      服务      : {alert.target_service} / 严重度 {alert.severity.value}")
            votes = [v for v in alert.algorithm_votes if not v.get("abstained")]
            print("      算法投票  : " + ", ".join(f"{v['algorithm']}={'异常' if v['is_anomaly'] else '正常'}" for v in votes))

        if state.rca_event:
            rca = state.rca_event
            print("\n  [2] 根因分析")
            print(f"      根因      : {rca.root_cause}")
            print(f"      类型/置信度: {rca.root_cause_type} / {rca.confidence:.2f}")
            print(f"      影响链    : {_fmt_chain(rca.affected_services)}")
            print(f"      影响面    : {rca.impact_score:.2f}    证据 {len(rca.evidence)} 条")
            print(f"      建议动作  : {', '.join(rca.suggested_actions)}")
            if rca.needs_human:
                print("      ⚠ 置信度不足，已转人工研判")

        if state.heal_event:
            heal = state.heal_event
            print("\n  [3] 自愈提案（未执行）")
            print(f"      Playbook  : {heal.playbook_id} [{heal.heal_level.value}]")
            print(f"      命令      : {heal.command}")
            print(f"      爆炸半径  : {heal.blast_radius:.2f}")
            print(f"      dry-run   : {heal.dry_run_result}")

        if state.change_event:
            change = state.change_event
            print("\n  [4] 变更审批")
            print(f"      结果      : {change.approval_status} / 风险分 {change.risk_score:.3f}")
            print(f"      策略      : {change.policy}")
            print(f"      审批人    : {change.approver or '（待审批）'}")
            print(f"      工单      : {change.ticket_id or '—'}")
            factors = ", ".join(f"{k}={v:.2f}" for k, v in change.risk_factors.items())
            print(f"      风险因子  : {factors}")

        if state.heal_event and state.heal_event.execution_result:
            print("\n  [5] 执行结果")
            print(f"      状态      : {state.heal_event.status}")
            print(f"      结果      : {state.heal_event.execution_result}")

        print("\n  [时间线]（本地时间）")
        for entry in state.timeline:
            print(f"      {entry.at.astimezone():%H:%M:%S} {entry.actor:12s} {entry.action:16s} {entry.detail}")

        print("\n  [工作流节点]")
        for item in orchestrator.get_workflow_status():
            print(f"      {item['name']:10s} {item['status']:10s} attempts={item['attempts']}")

        print("\n  [审计链]")
        print(f"      {AuditSink.verify_dir(settings.audit_dir)}")
        print("=" * 72)

    await bus.stop()
    return state


if __name__ == "__main__":
    asyncio.run(run_demo())
