"""监控告警 Agent：检测 → 分级 → 收敛 → 发布。

处理链路：
1. 按 (service, metric) 维护滑动历史，冷启动样本不足时只记历史、不告警；
2. 用该指标的 `MetricPolicy` 选择单向/双向检测，多算法投票；
3. 用静态阈值 + 统计 ratio 共同定级（critical / high / medium / low）；
4. 告警指纹滑动窗口去重，并对同一服务的多条告警做关联，避免「告警风暴」；
5. 发布 AlertEvent 到 aiops.alerts。

Demo 数据也走完整检测链路（而不是硬编码一条告警），保证演示即真实路径。
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from agents.base_agent import BaseAgent
from config.settings import settings
from core.event_bus import EventBus
from models.events import AgentType, AlertEvent, EventType, IncidentState, Severity
from models.time_series import (
    EnsembleDetector,
    MetricPolicy,
    generate_db_series,
    policy_for,
)

logger = logging.getLogger(__name__)

DEMO_METRIC = "mysql_replication_lag_sec"
DEMO_SERVICE = "mysql-replica-2"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AlertFingerprint:
    """告警指纹 + 滑动窗口去重。"""

    def __init__(self, window_seconds: int = 300):
        self._window = timedelta(seconds=window_seconds)
        self._recent: dict[str, datetime] = {}

    @staticmethod
    def compute(alert_name: str, target_service: str, labels: dict[str, Any]) -> str:
        raw = f"{alert_name}|{target_service}|{sorted((labels or {}).items())}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def is_duplicate(self, fingerprint: str, now: Optional[datetime] = None) -> bool:
        now = now or _now()
        self._cleanup(now)
        if fingerprint in self._recent:
            return True
        self._recent[fingerprint] = now
        return False

    def _cleanup(self, now: datetime) -> None:
        expired = [key for key, ts in self._recent.items() if now - ts >= self._window]
        for key in expired:
            self._recent.pop(key, None)


class AlertCorrelator:
    """同一服务短时间内的多条告警合并成一次事件（告警收敛）。"""

    def __init__(self, window_seconds: int = 60):
        self._window = timedelta(seconds=window_seconds)
        self._recent: dict[str, list[tuple[datetime, str]]] = defaultdict(list)

    def correlate(self, service: str, alert_name: str, now: Optional[datetime] = None) -> list[str]:
        now = now or _now()
        history = [item for item in self._recent[service] if now - item[0] <= self._window]
        correlated = [name for _, name in history]
        history.append((now, alert_name))
        self._recent[service] = history
        return correlated


class MonitorAgent(BaseAgent):
    def __init__(
        self,
        event_bus: EventBus,
        dedup_window_sec: int = settings.monitor_dedup_window_sec,
        correlation_window_sec: int = settings.monitor_correlation_window_sec,
        min_votes: int = settings.monitor_min_votes,
        history_maxlen: int = 1500,
        detectors: Optional[list] = None,
    ):
        super().__init__(AgentType.MONITOR, event_bus, "MonitorAgent")
        self._fingerprint = AlertFingerprint(window_seconds=dedup_window_sec)
        self._correlator = AlertCorrelator(window_seconds=correlation_window_sec)
        self._min_votes = min_votes
        self._history_maxlen = history_maxlen
        self._history: dict[str, list[float]] = defaultdict(list)
        self._detectors: dict[str, EnsembleDetector] = {}
        self._detector_overrides = detectors

    # ------------------------------------------------------------------ entry
    async def process(self, state: IncidentState) -> IncidentState:
        sample = state.metadata.get("metric_data")
        if not sample:
            return await self._run_demo(state)

        metric_name = sample.get("metric_name") or "unknown_metric"
        value = float(sample.get("value", 0.0))
        service = sample.get("service") or "unknown"
        labels = sample.get("labels") or {}
        return await self._evaluate(
            state,
            metric_name,
            value,
            service,
            labels,
            baseline=sample.get("baseline"),
        )

    async def _evaluate(
        self,
        state: IncidentState,
        metric_name: str,
        value: float,
        service: str,
        labels: dict[str, Any],
        source: str = "prometheus",
        baseline: Optional[list[float]] = None,
    ) -> IncidentState:
        policy = policy_for(metric_name)
        key = f"{service}:{metric_name}"
        history = self._history[key]

        if not history and baseline:
            history.extend(float(item) for item in baseline)
            state.touch("MonitorAgent", "bootstrap", f"{key} 载入 {len(history)} 个基线样本")

        if len(history) < policy.min_history:
            history.append(value)
            self._trim(key)
            state.touch(
                "MonitorAgent",
                "warmup",
                f"{key} 历史样本 {len(history)}/{policy.min_history}，暂不判定异常",
            )
            return state

        detector = self._detector_for(metric_name, policy)
        is_anomaly, ratio, results = detector.detect(history, value)
        history.append(value)
        self._trim(key)

        if not is_anomaly:
            state.touch(
                "MonitorAgent",
                "no_anomaly",
                f"{key}={value:.2f}{policy.unit} 正常（ratio={ratio:.2f}）",
            )
            return state

        fingerprint = AlertFingerprint.compute(metric_name, service, labels)
        if self._fingerprint.is_duplicate(fingerprint):
            state.metadata["suppressed"] = state.metadata.get("suppressed", 0) + 1
            state.touch(
                "MonitorAgent",
                "suppress",
                f"{key} 命中去重窗口，已抑制重复告警",
                level="warning",
            )
            return state

        severity = Severity(policy.severity_for(value, ratio))
        correlated = self._correlator.correlate(service, metric_name)
        alert = AlertEvent(
            alert_name=metric_name,
            severity=severity,
            source=source,
            target_service=service,
            metric_name=metric_name,
            metric_value=round(value, 3),
            threshold=policy.warn,
            description=(
                f"{service} 的 {metric_name} 当前 {value:.2f}{policy.unit}，"
                f"告警阈值 {policy.warn}{policy.unit}，严重阈值 {policy.crit}{policy.unit}，"
                f"投票通过 {sum(1 for r in results if r.is_anomaly)}/"
                f"{sum(1 for r in results if not r.abstained)}"
            ),
            labels={str(k): str(v) for k, v in labels.items()},
            anomaly_score=round(value, 3),
            algorithm_votes=[r.to_dict() for r in results],
            dedup_key=fingerprint,
            suppressed_count=0,
            correlation_id=state.incident_id,
            metadata={"correlated_alerts": correlated, "policy": policy.name},
        )

        state.alert_event = alert
        state.status = "investigating"
        state.touch(
            "MonitorAgent",
            "alert_fired",
            f"{metric_name}={value:.2f}{policy.unit} → {severity.value}（ratio={ratio:.2f}）",
            level="warning" if severity.rank >= Severity.HIGH.rank else "info",
        )
        await self._publish("aiops.alerts", alert)
        logger.info("[MonitorAgent] %s fired on %s [%s]", metric_name, service, severity.value)
        return state

    def _detector_for(self, metric_name: str, policy: MetricPolicy) -> EnsembleDetector:
        if self._detector_overrides is not None:
            key = metric_name
            if key not in self._detectors:
                self._detectors[key] = EnsembleDetector(
                    policy=policy,
                    min_votes=self._min_votes,
                    detectors=self._detector_overrides,
                )
            return self._detectors[key]

        if metric_name not in self._detectors:
            self._detectors[metric_name] = EnsembleDetector(policy=policy, min_votes=self._min_votes)
        return self._detectors[metric_name]

    def _trim(self, key: str) -> None:
        if len(self._history[key]) > self._history_maxlen:
            self._history[key] = self._history[key][-self._history_maxlen :]

    def get_history(self, service: str, metric_name: str) -> list[float]:
        return list(self._history.get(f"{service}:{metric_name}", []))

    async def _run_demo(self, state: IncidentState) -> IncidentState:
        """用真实检测链路跑一次「主从延迟突增」故障。"""
        normal, _, _ = generate_db_series(DEMO_METRIC, n_points=120, inject_faults=False)
        self._history[f"{DEMO_SERVICE}:{DEMO_METRIC}"] = normal
        state.metadata["demo"] = True
        state.touch("MonitorAgent", "demo_seed", f"注入 120 个正常样本作为 {DEMO_SERVICE} 基线")
        return await self._evaluate(
            state,
            DEMO_METRIC,
            46.2,
            DEMO_SERVICE,
            {"host": "db-node-3", "role": "replica", "cluster": "order-db"},
        )
