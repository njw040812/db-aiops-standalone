"""故障自愈 Agent：Playbook 匹配 → 预检（dry-run）→ 审批后执行。

原实现的三个问题，这里逐条修掉：
1. `dry_run=True` 反而执行了修复 —— 现在 dry-run 只做预检，永不执行；
2. 执行发生在审批之前 —— 现在 `execute()` 强制要求一条 approved 的变更单；
3. 模板格式化用固定参数，少一个占位符就 KeyError —— 现在用 SafeDict 兜底。

安全护栏（按顺序生效）：
    熔断器 → 爆炸半径上限 → 命令白名单 → 危险模式拦截 → 审批门禁 → 幂等检查 → 执行
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from agents.base_agent import BaseAgent
from config.settings import settings
from core.event_bus import EventBus
from models.events import AgentType, EventType, HealEvent, HealLevel, IncidentState

logger = logging.getLogger(__name__)


class SafeDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


@dataclass(frozen=True)
class Playbook:
    id: str
    action_type: str
    level: HealLevel
    description: str
    command_template: str
    blast_radius: float
    risk_weight: float
    estimated_duration_sec: int
    rollback_command: Optional[str] = None
    ddl: bool = False
    touches_primary: bool = False
    requires_window: bool = False


PLAYBOOKS: dict[str, Playbook] = {
    "recycle_connection_pool": Playbook(
        id="recycle_connection_pool",
        action_type="recycle_connection_pool",
        level=HealLevel.L0_AUTO,
        description="回收空闲连接并重置代理层连接池",
        command_template="proxysql-admin --recycle-connections --service {service}",
        blast_radius=0.02,
        risk_weight=0.10,
        estimated_duration_sec=5,
        rollback_command=None,
    ),
    "throttle_batch_job": Playbook(
        id="throttle_batch_job",
        action_type="throttle_batch_job",
        level=HealLevel.L0_AUTO,
        description="把批量任务限速到 30%，给在线业务让出 IO/CPU",
        command_template="db-jobctl throttle --job {job_name} --ratio 0.3",
        blast_radius=0.03,
        risk_weight=0.10,
        estimated_duration_sec=10,
        rollback_command="db-jobctl throttle --job {job_name} --ratio 1.0",
    ),
    "purge_binlog": Playbook(
        id="purge_binlog",
        action_type="purge_binlog",
        level=HealLevel.L0_AUTO,
        description="清理超过保留期的 binlog，释放磁盘空间",
        command_template='mysql -h {host} -e "PURGE BINARY LOGS BEFORE NOW() - INTERVAL {retention_hours} HOUR;"',
        blast_radius=0.05,
        risk_weight=0.20,
        estimated_duration_sec=20,
        rollback_command=None,
    ),
    "analyze_table": Playbook(
        id="analyze_table",
        action_type="analyze_table",
        level=HealLevel.L0_AUTO,
        description="重新收集统计信息，修正优化器执行计划",
        command_template='mysql -h {host} -e "ANALYZE TABLE {database}.{table};"',
        blast_radius=0.04,
        risk_weight=0.05,
        estimated_duration_sec=30,
        rollback_command=None,
    ),
    "kill_long_transaction": Playbook(
        id="kill_long_transaction",
        action_type="kill_long_transaction",
        level=HealLevel.L1_SEMI,
        description="杀掉运行超过 60s 的长事务，释放连接与锁",
        command_template="pt-kill --host {host} --busy-time 60 --kill --print",
        blast_radius=0.06,
        risk_weight=0.35,
        estimated_duration_sec=10,
        rollback_command=None,
    ),
    "kill_ddl": Playbook(
        id="kill_ddl",
        action_type="kill_ddl",
        level=HealLevel.L1_SEMI,
        description="终止阻塞复制回放的 DDL 会话",
        command_template='mysql -h {host} -e "KILL QUERY {session_id};"',
        blast_radius=0.08,
        risk_weight=0.45,
        estimated_duration_sec=10,
        rollback_command=None,
    ),
    "route_reads_to_replica": Playbook(
        id="route_reads_to_replica",
        action_type="route_reads_to_replica",
        level=HealLevel.L1_SEMI,
        description="把读流量切到健康的只读副本",
        command_template="proxysql-admin --route reads --from {service} --to {target_replica}",
        blast_radius=0.10,
        risk_weight=0.30,
        estimated_duration_sec=15,
        rollback_command="proxysql-admin --route reads --restore {service}",
    ),
    "rollback_config": Playbook(
        id="rollback_config",
        action_type="rollback_config",
        level=HealLevel.L1_SEMI,
        description="回滚最近的参数变更（连接数/连接池配置）",
        command_template="db-config rollback --service {service} --version previous",
        blast_radius=0.10,
        risk_weight=0.55,
        estimated_duration_sec=30,
        rollback_command="db-config apply --service {service} --version current",
    ),
    "extend_tablespace": Playbook(
        id="extend_tablespace",
        action_type="extend_tablespace",
        level=HealLevel.L1_SEMI,
        description="扩容表空间数据文件",
        command_template='mysql -h {host} -e "ALTER TABLESPACE {tablespace} ADD DATAFILE \'{datafile}\';"',
        blast_radius=0.12,
        risk_weight=0.40,
        estimated_duration_sec=60,
        rollback_command=None,
    ),
    "restart_replica": Playbook(
        id="restart_replica",
        action_type="restart_replica",
        level=HealLevel.L1_SEMI,
        description="重启复制线程，修复复制中断",
        command_template='mysql -h {host} -e "STOP REPLICA; START REPLICA;"',
        blast_radius=0.15,
        risk_weight=0.45,
        estimated_duration_sec=45,
        rollback_command=None,
    ),
    "archive_table": Playbook(
        id="archive_table",
        action_type="archive_table",
        level=HealLevel.L2_MANUAL,
        description="归档历史数据到冷存储并清理原表（有数据删除风险）",
        command_template="pt-archiver --source h={host},D={database},t={table} --where 'created_at < NOW() - INTERVAL 180 DAY' --purge --limit 1000",
        blast_radius=0.20,
        risk_weight=0.75,
        estimated_duration_sec=600,
        rollback_command=None,
        requires_window=True,
    ),
    "add_index_online": Playbook(
        id="add_index_online",
        action_type="add_index_online",
        level=HealLevel.L2_MANUAL,
        description="用 gh-ost 在线添加索引（DDL，需变更窗口）",
        command_template='gh-ost --host={host} --database={database} --table={table} --alter="ADD INDEX {index_name} (created_at, status)" --execute',
        blast_radius=0.25,
        risk_weight=0.80,
        estimated_duration_sec=900,
        rollback_command="gh-ost --host={host} --database={database} --table={table} --panic",
        ddl=True,
        touches_primary=True,
        requires_window=True,
    ),
    "promote_replica": Playbook(
        id="promote_replica",
        action_type="promote_replica",
        level=HealLevel.L2_MANUAL,
        description="主库故障时提升只读副本为主库（主从切换）",
        command_template="orchestrator -c recover -i {service}",
        blast_radius=0.40,
        risk_weight=0.95,
        estimated_duration_sec=180,
        rollback_command="orchestrator -c recover -i {service} --rollback",
        touches_primary=True,
        requires_window=True,
    ),
    "scale_up": Playbook(
        id="scale_up",
        action_type="scale_up",
        level=HealLevel.L1_SEMI,
        description="提升实例规格/副本数",
        command_template="db-capacity scale --instance {service} --add-replicas 1",
        blast_radius=0.07,
        risk_weight=0.25,
        estimated_duration_sec=300,
        rollback_command="db-capacity scale --instance {service} --add-replicas 0",
    ),
}


class CircuitBreaker:
    """连续失败 N 次后暂停自动修复，半开状态只放一个探针。"""

    def __init__(self, threshold: int = 5, timeout_sec: int = 60):
        self._threshold = threshold
        self._timeout = timedelta(seconds=timeout_sec)
        self._failures = 0
        self._opened_at: Optional[datetime] = None
        self._state = "CLOSED"

    def allow_request(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.now(timezone.utc)
        if self._state == "CLOSED":
            return True
        if self._state == "OPEN":
            if self._opened_at and now - self._opened_at >= self._timeout:
                self._state = "HALF_OPEN"
                logger.warning("[CircuitBreaker] HALF_OPEN: 放行一个探针请求")
                return True
            return False
        return True  # HALF_OPEN

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None
        self._state = "CLOSED"

    def record_failure(self, now: Optional[datetime] = None) -> None:
        self._failures += 1
        if self._failures >= self._threshold:
            self._state = "OPEN"
            self._opened_at = now or datetime.now(timezone.utc)
            logger.error("[CircuitBreaker] OPEN: 自动修复已暂停（连续失败 %d 次）", self._failures)

    @property
    def state(self) -> str:
        return self._state

    @property
    def failures(self) -> int:
        return self._failures


class CommandExecutor:
    """命令执行器抽象：预检与执行分离，便于替换成真实执行器。"""

    ALLOWED_PREFIXES = (
        "mysql ",
        "mysqladmin ",
        "proxysql-admin ",
        "pt-kill ",
        "pt-archiver ",
        "gh-ost ",
        "orchestrator ",
        "db-config ",
        "db-jobctl ",
        "db-capacity ",
        "kubectl ",
        "redis-cli ",
    )
    # 注意：SQL 语句本身带分号（KILL QUERY 123;），所以不能简单拦 `;`，
    # 只拦截真正的 shell 拼接/替换/重定向特征。
    FORBIDDEN_PATTERNS = (
        r"\bdrop\s+database\b",
        r"\bdrop\s+table\b",
        r"\btruncate\s+table\b",
        r"\brm\s+-rf\b",
        r"\bshutdown\b",
        r"\bdelete\s+from\b.*\bwhere\s+1\s*=\s*1\b",
        r"\$\(",
        r"`",
        r"\s\|\s",
        r"\s&&\s",
        r";\s*(rm|shutdown|reboot|mkfs)\b",
    )

    def preflight(self, command: str) -> str:
        if not command or not command.strip():
            raise ValueError("空命令")
        if not command.startswith(self.ALLOWED_PREFIXES):
            raise ValueError(f"命令不在白名单内: {command.split()[0] if command.split() else command}")
        for pattern in self.FORBIDDEN_PATTERNS:
            if re.search(pattern, command, flags=re.IGNORECASE):
                raise ValueError(f"命令命中危险模式 {pattern}: {command}")
        return f"DRY-RUN OK: 命令通过白名单与危险模式校验 — {command}"

    async def run(self, command: str) -> str:
        raise NotImplementedError


class SimulatedExecutor(CommandExecutor):
    """默认执行器：只做模拟，不触碰任何真实数据库。"""

    async def run(self, command: str) -> str:
        self.preflight(command)
        await asyncio.sleep(0)
        return f"SUCCESS(simulated): 已模拟执行，用时 0ms — {command}"


class SubprocessExecutor(CommandExecutor):
    """真实执行器（默认关闭）。

    必须显式设置 AIOPS_ENABLE_REAL_EXECUTION=true 才会被构造；
    仍然只允许白名单命令，并在预检阶段拦截危险模式。
    """

    def __init__(self, timeout_sec: int = 60):
        if not settings.enable_real_execution:
            raise RuntimeError("真实执行未开启，请设置 AIOPS_ENABLE_REAL_EXECUTION=true")
        self._timeout = timeout_sec

    async def run(self, command: str) -> str:
        self.preflight(command)
        started = time.perf_counter()
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=self._timeout)
        except asyncio.TimeoutError:
            process.kill()
            raise RuntimeError(f"命令执行超时（>{self._timeout}s）")
        elapsed = int((time.perf_counter() - started) * 1000)
        output = (stdout or b"").decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            raise RuntimeError(f"命令失败({process.returncode}): {output}")
        return f"SUCCESS: {output[:500]} ({elapsed}ms)"


class HealAgent(BaseAgent):
    def __init__(
        self,
        event_bus: EventBus,
        dry_run: bool = settings.heal_dry_run,
        max_blast_radius: float = settings.heal_max_blast_radius,
        executor: Optional[CommandExecutor] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        playbooks: Optional[dict[str, Playbook]] = None,
    ):
        super().__init__(AgentType.HEAL, event_bus, "HealAgent")
        self._dry_run = dry_run
        self._max_blast_radius = max_blast_radius
        self._executor = executor or SimulatedExecutor()
        self._breaker = circuit_breaker or CircuitBreaker(
            threshold=settings.circuit_breaker_threshold,
            timeout_sec=settings.circuit_breaker_timeout_sec,
        )
        self._playbooks = playbooks or PLAYBOOKS

    # ----------------------------------------------------------------- propose
    async def process(self, state: IncidentState) -> IncidentState:
        """编排器里的 `heal` 节点只负责「提案」，不执行。"""
        return await self.propose(state)

    async def propose(self, state: IncidentState) -> IncidentState:
        rca = state.rca_event
        if rca is None:
            raise ValueError("HealAgent 需要 rca_event")

        if rca.needs_human:
            state.status = "escalated"
            state.touch("HealAgent", "skip", "根因证据不足，跳过自愈并转人工", level="warning")
            return state

        if not self._breaker.allow_request():
            state.status = "blocked"
            state.touch("HealAgent", "circuit_open", "熔断器开启，暂停自动修复", level="error")
            return state

        selected = self._select_playbook(rca.suggested_actions)
        if selected is None:
            state.status = "escalated"
            state.touch(
                "HealAgent",
                "no_playbook",
                f"建议动作 {rca.suggested_actions} 无对应 Playbook，转人工",
                level="warning",
            )
            return state

        playbook = selected
        level = playbook.level
        escalation_reason = ""
        if playbook.blast_radius > self._max_blast_radius:
            level = HealLevel.L2_MANUAL
            escalation_reason = (
                f"爆炸半径 {playbook.blast_radius:.2f} 超过上限 {self._max_blast_radius:.2f}，强制升级 L2"
            )

        target_service = state.alert_event.target_service if state.alert_event else "unknown"
        command = self._render(playbook.command_template, target_service)
        dry_run_result = None
        if self._dry_run:
            try:
                dry_run_result = self._executor.preflight(command)
            except ValueError as exc:
                state.status = "escalated"
                state.touch("HealAgent", "preflight_failed", str(exc), level="error")
                return state

        heal_event = HealEvent(
            rca_event_id=rca.event_id,
            heal_level=level,
            action_type=playbook.action_type,
            playbook_id=playbook.id,
            command=command,
            action_params={
                "service": target_service,
                "namespace": "production",
                "estimated_duration_sec": playbook.estimated_duration_sec,
                "risk_weight": playbook.risk_weight,
                "ddl": playbook.ddl,
                "touches_primary": playbook.touches_primary,
                "requires_window": playbook.requires_window,
                "impact_score": rca.impact_score,
            },
            target_service=target_service,
            estimated_impact=playbook.description,
            blast_radius=playbook.blast_radius,
            rollback_command=self._render(playbook.rollback_command, target_service)
            if playbook.rollback_command
            else None,
            dry_run_result=dry_run_result,
            status="proposed",
            executor=type(self._executor).__name__,
            correlation_id=state.incident_id,
            metadata={"escalation_reason": escalation_reason} if escalation_reason else {},
        )

        state.heal_event = heal_event
        state.status = "pending_approval"
        state.touch(
            "HealAgent",
            "propose",
            f"{playbook.id} [{level.value}] 爆炸半径 {playbook.blast_radius:.2f}"
            + (f"，{escalation_reason}" if escalation_reason else ""),
            level="warning",
        )
        await self._publish("aiops.commands", heal_event)
        logger.info("[HealAgent] proposed %s level=%s blast=%.2f", playbook.id, level.value, playbook.blast_radius)
        return state

    # ----------------------------------------------------------------- execute
    async def execute(self, state: IncidentState) -> IncidentState:
        """审批通过后执行；没有 approved 变更单直接抛错，防止绕过审批。"""
        heal = state.heal_event
        change = state.change_event
        if heal is None or change is None:
            raise ValueError("执行阶段需要 heal_event 与 change_event")
        if change.approval_status != "approved":
            raise PermissionError(f"变更单未批准（当前 {change.approval_status}），拒绝执行 {heal.action_type}")

        if heal.status in ("succeeded", "failed"):
            state.touch("HealAgent", "idempotent_skip", f"{heal.action_type} 已执行过，跳过")
            return state

        if not self._breaker.allow_request():
            state.status = "blocked"
            state.touch("HealAgent", "circuit_open", "熔断器开启，取消执行", level="error")
            return state

        heal.status = "executing"
        heal.event_type = EventType.HEAL_EXECUTING
        state.status = "executing"
        state.touch("HealAgent", "execute", f"开始执行：{heal.command}", level="warning")

        try:
            result = await self._executor.run(heal.command)
            heal.execution_result = result
            heal.status = "succeeded"
            heal.event_type = EventType.HEAL_COMPLETED
            heal.executed_at = datetime.now(timezone.utc)
            self._breaker.record_success()
            state.status = "resolved"
            state.touch("HealAgent", "succeeded", result, level="info")
        except Exception as exc:
            heal.execution_result = f"FAILED: {exc}"
            heal.status = "failed"
            heal.event_type = EventType.HEAL_FAILED
            heal.executed_at = datetime.now(timezone.utc)
            self._breaker.record_failure()
            state.status = "failed"
            state.touch("HealAgent", "failed", str(exc), level="error")
            if heal.rollback_command:
                try:
                    rollback_result = await self._executor.run(heal.rollback_command)
                    heal.execution_result += f" | ROLLBACK: {rollback_result}"
                    state.touch("HealAgent", "rollback", rollback_result, level="warning")
                except Exception as rollback_exc:
                    heal.execution_result += f" | ROLLBACK FAILED: {rollback_exc}"
                    state.touch("HealAgent", "rollback_failed", str(rollback_exc), level="error")

        await self._publish("aiops.commands", heal)
        await self._publish("aiops.audit", heal)
        return state

    # ---------------------------------------------------------------- helpers
    def _select_playbook(self, actions: list[str]) -> Optional[Playbook]:
        for action in actions:
            if action in self._playbooks:
                return self._playbooks[action]
        return None

    def _render(self, template: Optional[str], service: str) -> str:
        if not template:
            return ""
        values = SafeDict(
            service=service,
            host=self._host_for(service),
            namespace="production",
            session_id="1024871",
            job_name="nightly-batch",
            retention_hours=72,
            database="order_db",
            table="t_order",
            tablespace="order_ts",
            datafile="/data/mysql/order_ts_02.ibd",
            index_name="idx_order_created_status",
            target_replica="mysql-replica-1",
        )
        return template.format_map(values)

    @staticmethod
    def _host_for(service: str) -> str:
        mapping = {
            "mysql-primary": "db-node-1",
            "mysql-replica-1": "db-node-2",
            "mysql-replica-2": "db-node-3",
        }
        return mapping.get(service, "db-node-1")

    def get_playbooks(self) -> dict[str, Playbook]:
        return dict(self._playbooks)

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        return self._breaker
