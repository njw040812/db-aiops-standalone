"""根因分析 Agent：知识图谱 + 变更关联 + 归一化贝叶斯打分。

处理链路：
1. 从图谱拿故障对象的完整依赖链（影响面）；
2. 按指标选故障假设集（主从延迟 / 连接打满 / 慢查询 / 磁盘）；
3. 收集证据：指标异常、24h 内变更、共置主机、长事务、错误日志模式；
4. 对每个假设算 `posterior = prior × likelihood` 后**归一化**，
   得到可解释的置信度（原实现直接除以固定 0.5 再截断到 1，概率不守恒）；
5. 置信度不足或与第二名差距太小时标记 needs_human，交给编排器跳过自愈。

为什么要 needs_human：自愈系统最大的风险不是「修不好」，而是「证据不足时
仍然自信地执行一条有破坏性的命令」。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from agents.base_agent import BaseAgent
from config.settings import settings
from core.event_bus import EventBus
from core.knowledge_graph import InMemoryKnowledgeGraph
from models.events import AgentType, EventType, IncidentState, RCAEvent, Severity

logger = logging.getLogger(__name__)

# 每个指标对应的故障假设。prior 来自历史故障分布（可离线统计后回填），
# likelihood 由当前证据动态计算。
FAULT_CATALOG: dict[str, list[dict[str, Any]]] = {
    "mysql_replication_lag_sec": [
        {
            "type": "replica_ddl",
            "root_cause": "只读副本执行 DDL 阻塞复制回放，导致延迟堆积",
            "prior": 0.30,
            "evidence": "recent_ddl",
            "actions": ["kill_ddl", "restart_replica"],
        },
        {
            "type": "long_transaction",
            "root_cause": "主库存在长事务，binlog 迟迟不提交，从库无法回放",
            "prior": 0.25,
            "evidence": "long_transaction",
            "actions": ["kill_long_transaction", "route_reads_to_replica"],
        },
        {
            "type": "write_burst",
            "root_cause": "批量写入或大促流量突增，从库回放能力不足",
            "prior": 0.20,
            "evidence": "traffic_spike",
            "actions": ["throttle_batch_job", "scale_up"],
        },
        {
            "type": "replica_contention",
            "root_cause": "副本主机 CPU/IO 被其他进程抢占，回放线程得不到调度",
            "prior": 0.15,
            "evidence": "shared_host",
            "actions": ["restart_replica", "extend_tablespace"],
        },
        {
            "type": "network_partition",
            "root_cause": "主从之间网络抖动或丢包，复制中断后重连",
            "prior": 0.10,
            "evidence": "none",
            "actions": ["restart_replica"],
        },
    ],
    "mysql_connections_used_ratio": [
        {
            "type": "connection_leak",
            "root_cause": "应用连接池配置不当或连接未释放，连接数持续爬升",
            "prior": 0.35,
            "evidence": "recent_config",
            "actions": ["recycle_connection_pool", "rollback_config"],
        },
        {
            "type": "slow_query_hold",
            "root_cause": "慢查询长时间占用连接，连接池被逐步耗尽",
            "prior": 0.30,
            "evidence": "slow_query",
            "actions": ["kill_long_transaction", "add_index_online"],
        },
        {
            "type": "traffic_spike",
            "root_cause": "流量突增导致连接需求超过连接数上限",
            "prior": 0.20,
            "evidence": "traffic_spike",
            "actions": ["throttle_batch_job", "route_reads_to_replica"],
        },
        {
            "type": "deploy_regression",
            "root_cause": "近期发布引入连接泄漏，随流量上涨逐步打满",
            "prior": 0.15,
            "evidence": "recent_deploy",
            "actions": ["rollback_config", "recycle_connection_pool"],
        },
    ],
    "mysql_slow_query_qps": [
        {
            "type": "missing_index",
            "root_cause": "新上线查询缺少合适索引，执行计划退化为全表扫描",
            "prior": 0.35,
            "evidence": "recent_deploy",
            "actions": ["add_index_online", "kill_long_transaction"],
        },
        {
            "type": "stale_statistics",
            "root_cause": "统计信息过期，优化器选错执行计划",
            "prior": 0.25,
            "evidence": "none",
            "actions": ["analyze_table"],
        },
        {
            "type": "batch_window",
            "root_cause": "批量任务与在线业务叠加，慢查询集中出现",
            "prior": 0.20,
            "evidence": "traffic_spike",
            "actions": ["throttle_batch_job"],
        },
        {
            "type": "ddl_blocking",
            "root_cause": "DDL 变更持有元数据锁，阻塞后续查询",
            "prior": 0.20,
            "evidence": "recent_ddl",
            "actions": ["kill_ddl", "analyze_table"],
        },
    ],
    "mysql_cpu_usage_percent": [
        {
            "type": "slow_query_cpu",
            "root_cause": "慢查询消耗大量 CPU，单条 SQL 扫描行数过高",
            "prior": 0.30,
            "evidence": "slow_query",
            "actions": ["kill_long_transaction", "add_index_online"],
        },
        {
            "type": "backup_job",
            "root_cause": "备份/统计任务与业务高峰重叠，CPU 被抢占",
            "prior": 0.20,
            "evidence": "traffic_spike",
            "actions": ["throttle_batch_job"],
        },
        {
            "type": "deploy_regression",
            "root_cause": "近期发布引入低效 SQL，CPU 随流量上升",
            "prior": 0.25,
            "evidence": "recent_deploy",
            "actions": ["rollback_config", "kill_long_transaction"],
        },
        {
            "type": "replication_overhead",
            "root_cause": "主库复制线程与业务查询争抢 CPU",
            "prior": 0.25,
            "evidence": "shared_host",
            "actions": ["route_reads_to_replica"],
        },
    ],
    "mysql_disk_used_ratio": [
        {
            "type": "binlog_accumulation",
            "root_cause": "binlog 未及时清理，磁盘空间被持续占用",
            "prior": 0.35,
            "evidence": "none",
            "actions": ["purge_binlog", "extend_tablespace"],
        },
        {
            "type": "table_growth",
            "root_cause": "大表持续增长且缺少归档策略",
            "prior": 0.30,
            "evidence": "recent_ddl",
            "actions": ["extend_tablespace", "archive_table"],
        },
        {
            "type": "temp_spill",
            "root_cause": "大查询产生磁盘临时表，写入量激增",
            "prior": 0.20,
            "evidence": "slow_query",
            "actions": ["kill_long_transaction", "analyze_table"],
        },
        {
            "type": "purge_lag",
            "root_cause": "历史数据清理任务未按计划执行",
            "prior": 0.15,
            "evidence": "none",
            "actions": ["purge_binlog"],
        },
    ],
}

DEFAULT_HYPOTHESES = [
    {
        "type": "unknown",
        "root_cause": "未匹配到已知故障模式，需要人工介入补充判定规则",
        "prior": 1.0,
        "evidence": "none",
        "actions": [],
    }
]


class RCAAgent(BaseAgent):
    def __init__(
        self,
        event_bus: EventBus,
        knowledge_graph: Optional[InMemoryKnowledgeGraph] = None,
        min_confidence: float = settings.rca_min_confidence,
        min_margin: float = settings.rca_min_margin,
        max_depth: int = settings.rca_max_depth,
        lookback_hours: int = settings.rca_change_lookback_hours,
        now: Optional[datetime] = None,
    ):
        super().__init__(AgentType.RCA, event_bus, "RCAAgent")
        self._kg = knowledge_graph
        self._min_confidence = min_confidence
        self._min_margin = min_margin
        self._max_depth = max_depth
        self._lookback_hours = lookback_hours
        self._now = now

    def _clock(self) -> datetime:
        return self._now or datetime.now(timezone.utc)

    async def process(self, state: IncidentState) -> IncidentState:
        alert = state.alert_event
        if alert is None:
            raise ValueError("RCA 需要 alert_event，编排器不应在无告警时调度本节点")

        kg = self._kg
        affected = [alert.target_service]
        if kg is not None:
            affected += kg.trace_dependencies(alert.target_service, self._max_depth)

        changes = []
        if kg is not None:
            changes = kg.find_recent_changes(affected, within_hours=self._lookback_hours, now=self._clock())

        evidence = self._collect_evidence(alert, affected, changes, kg)
        hypotheses = self._hypotheses_for(alert.metric_name or alert.alert_name)
        ranked = self._rank(hypotheses, evidence)

        best, second = ranked[0], (ranked[1] if len(ranked) > 1 else None)
        confidence = best["posterior"]
        margin = confidence - (second["posterior"] if second else 0.0)
        needs_human = (
            best["type"] == "unknown"
            or confidence < self._min_confidence
            or margin < self._min_margin
        )

        impact = kg.compute_impact_score(alert.target_service) if kg is not None else 0.0

        rca_event = RCAEvent(
            event_type=EventType.RCA_COMPLETED if not needs_human else EventType.RCA_ESCALATED,
            alert_event_id=alert.event_id,
            root_cause=best["root_cause"],
            root_cause_service=alert.target_service,
            root_cause_type=best["type"],
            confidence=round(confidence, 4),
            affected_services=affected,
            evidence=evidence,
            suggested_actions=best["actions"],
            impact_score=impact,
            needs_human=needs_human,
            correlation_id=state.incident_id,
            metadata={
                "posterior_margin": round(margin, 4),
                "hypotheses": [
                    {"type": item["type"], "posterior": round(item["posterior"], 4)}
                    for item in ranked
                ],
                "recent_changes": changes[:5],
            },
        )

        state.rca_event = rca_event
        if needs_human:
            state.status = "escalated"
            state.touch(
                "RCAAgent",
                "escalate",
                f"置信度 {confidence:.2f} / 领先差距 {margin:.2f} 不足，转人工研判",
                level="warning",
            )
        else:
            state.status = "proposing"
            state.touch(
                "RCAAgent",
                "root_cause",
                f"{best['type']}（置信度 {confidence:.2f}，影响面 {impact:.2f}）",
            )

        await self._publish("aiops.events", rca_event)
        logger.info(
            "[RCAAgent] %s confidence=%.2f needs_human=%s",
            best["type"],
            confidence,
            needs_human,
        )
        return state

    # ---------------------------------------------------------------- helpers
    def _collect_evidence(
        self,
        alert,
        affected: list[str],
        changes: list[dict[str, Any]],
        kg: Optional[InMemoryKnowledgeGraph],
    ) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = [
            {
                "type": "metric_anomaly",
                "source": "prometheus",
                "detail": f"{alert.metric_name}={alert.metric_value}，告警等级 {alert.severity.value}",
                "weight": 0.8,
                "matched": True,
            }
        ]

        for change in changes:
            evidence.append(
                {
                    "type": "recent_change",
                    "source": "cmdb",
                    "detail": f"{change['service']}：{change['description']}（{change['age_hours']}h 前，类型 {change['type']}）",
                    "weight": round(0.6 + 0.4 * float(change.get("risk", 0.5)), 2),
                    "matched": True,
                    "change_type": change["type"],
                    "service": change["service"],
                }
            )

        # 共置主机证据：同一台物理机上的资源抢占是常见的隐性根因
        if kg is not None:
            for candidate in kg.find_root_candidates(alert.target_service, self._max_depth):
                shared = kg.find_shared_hosts(alert.target_service, candidate["service"])
                if shared:
                    evidence.append(
                        {
                            "type": "shared_host",
                            "source": "cmdb",
                            "detail": f"{alert.target_service} 与 {candidate['service']} 共置在 {','.join(shared)}",
                            "weight": 0.5,
                            "matched": True,
                            "service": candidate["service"],
                        }
                    )

        if alert.metadata.get("long_transactions"):
            evidence.append(
                {
                    "type": "long_transaction",
                    "source": "information_schema.innodb_trx",
                    "detail": f"存在 {alert.metadata['long_transactions']} 个超过 60s 的长事务",
                    "weight": 0.8,
                    "matched": True,
                }
            )
        if alert.metadata.get("slow_queries"):
            evidence.append(
                {
                    "type": "slow_query",
                    "source": "slow_query_log",
                    "detail": f"采样窗口内 {alert.metadata['slow_queries']} 条慢查询",
                    "weight": 0.7,
                    "matched": True,
                }
            )
        if alert.metadata.get("error_patterns"):
            evidence.append(
                {
                    "type": "log_pattern",
                    "source": "error_log",
                    "detail": f"错误日志命中: {', '.join(alert.metadata['error_patterns'][:3])}",
                    "weight": 0.6,
                    "matched": True,
                }
            )
        return evidence

    def _hypotheses_for(self, metric_name: str) -> list[dict[str, Any]]:
        normalized = (metric_name or "").replace("anomaly_", "")
        if normalized in FAULT_CATALOG:
            return FAULT_CATALOG[normalized]
        for key, value in FAULT_CATALOG.items():
            if key in normalized or normalized in key:
                return value
        return DEFAULT_HYPOTHESES

    def _rank(self, hypotheses: list[dict[str, Any]], evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """prior × likelihood 后归一化，保证 Σposterior = 1。"""
        evidence_types = {item["type"] for item in evidence}
        change_types = {item.get("change_type") for item in evidence if item["type"] == "recent_change"}

        scored: list[dict[str, Any]] = []
        for item in hypotheses:
            likelihood = self._likelihood(item["evidence"], evidence_types, change_types)
            scored.append({**item, "likelihood": likelihood, "weight": item["prior"] * likelihood})

        total = sum(item["weight"] for item in scored) or 1.0
        for item in scored:
            item["posterior"] = item["weight"] / total
        scored.sort(key=lambda item: item["posterior"], reverse=True)
        return scored

    @staticmethod
    def _likelihood(pattern: str, evidence_types: set[str], change_types: set[str]) -> float:
        mapping = {
            "recent_ddl": ({"recent_change"}, "ddl", 0.9, 0.25),
            "recent_config": ({"recent_change"}, "config", 0.85, 0.25),
            "recent_deploy": ({"recent_change"}, "deploy", 0.85, 0.25),
            "long_transaction": ({"long_transaction"}, None, 0.9, 0.20),
            "slow_query": ({"slow_query"}, None, 0.85, 0.25),
            "shared_host": ({"shared_host"}, None, 0.7, 0.30),
            "traffic_spike": ({"metric_anomaly", "log_pattern"}, None, 0.65, 0.40),
            "none": (set(), None, 0.45, 0.45),
        }
        required, change_type, hit, miss = mapping.get(pattern, (set(), None, 0.5, 0.3))
        if pattern == "none":
            return hit
        matched = bool(required & evidence_types)
        if change_type is not None:
            matched = matched and (change_type in change_types)
        return hit if matched else miss
