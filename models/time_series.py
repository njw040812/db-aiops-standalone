"""数据库指标的异常检测。

面向 DBA 场景的指标和通用 APM 不一样：
- 主从延迟、慢查询数、连接数使用率都是**单向上冲**才算故障，
  用双侧 3-Sigma 会把「延迟掉到 0」也当成异常，制造噪音；
- 这些指标分布长尾、有周期性，Z-Score 的均值/标准差会被历史尖刺污染，
  所以引入 MAD（中位数绝对偏差）做鲁棒标准化；
- 冷启动阶段样本不足时必须弃权，而不是拿 3 个点算出个「异常」。

因此检测器分三层：
1. 统计层：3-Sigma / MAD / EWMA；
2. 机器学习层：Isolation Forest（需要 >= 50 个历史点）；
3. 策略层：每个指标一份 `MetricPolicy`（方向、静态告警/严重阈值），
   投票时只有「有结论」的算法计入分母。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Sequence

import numpy as np

logger = logging.getLogger(__name__)

MAD_SCALE = 1.4826  # 正态分布下 MAD → 标准差的换算系数


@dataclass
class AnomalyResult:
    is_anomaly: bool
    score: float
    algorithm: str
    threshold: float
    current_value: float
    expected_value: float
    detail: str = ""
    abstained: bool = False
    ratio: float = 0.0

    def to_dict(self) -> dict:
        return {
            "algorithm": self.algorithm,
            "is_anomaly": bool(self.is_anomaly),
            "abstained": bool(self.abstained),
            "score": float(self.score),
            "ratio": float(self.ratio),
            "expected_value": float(self.expected_value),
            "detail": self.detail,
        }


def _abstain(algorithm: str, current: float, detail: str) -> AnomalyResult:
    return AnomalyResult(
        is_anomaly=False,
        score=0.0,
        algorithm=algorithm,
        threshold=0.0,
        current_value=current,
        expected_value=current,
        detail=detail,
        abstained=True,
    )


class BaseDetector(ABC):
    name: str = "base"

    @abstractmethod
    def detect(self, values: Sequence[float], current: float) -> AnomalyResult: ...


class ThreeSigmaDetector(BaseDetector):
    """经典 3-Sigma：适合近似正态、无强周期的指标。"""

    name = "3sigma"

    def __init__(self, sigma: float = 3.0, direction: str = "both", min_history: int = 10):
        self._sigma = sigma
        self._direction = direction
        self._min_history = min_history

    def detect(self, values: Sequence[float], current: float) -> AnomalyResult:
        if len(values) < self._min_history:
            return _abstain(self.name, current, f"history<{self._min_history}")

        arr = np.asarray(values, dtype=float)
        mean, std = float(arr.mean()), float(arr.std())
        if std <= 1e-9:
            return _abstain(self.name, current, "zero variance")

        z = (current - mean) / std
        score = z if self._direction == "upper" else abs(z)
        is_anomaly = score > self._sigma
        return AnomalyResult(
            is_anomaly=bool(is_anomaly),
            score=round(score, 4),
            algorithm=self.name,
            threshold=self._sigma,
            current_value=current,
            expected_value=round(mean, 4),
            ratio=round(abs(score) / self._sigma, 4),
            detail=f"z={score:.3f} mean={mean:.3f} std={std:.3f}",
        )


class MADDetector(BaseDetector):
    """基于中位数 + MAD 的鲁棒 Z-Score。

    对长尾分布（慢查询、主从延迟）比均值/标准差稳得多：单个历史尖刺
    不会把基线拉偏，MAD 为 0 时按标准差兜底，仍为 0 则弃权。
    """

    name = "mad"

    def __init__(self, k: float = 3.5, direction: str = "upper", min_history: int = 10):
        self._k = k
        self._direction = direction
        self._min_history = min_history

    def detect(self, values: Sequence[float], current: float) -> AnomalyResult:
        if len(values) < self._min_history:
            return _abstain(self.name, current, f"history<{self._min_history}")

        arr = np.asarray(values, dtype=float)
        median = float(np.median(arr))
        mad = float(np.median(np.abs(arr - median))) * MAD_SCALE
        if mad <= 1e-9:
            mad = float(arr.std())
        if mad <= 1e-9:
            return _abstain(self.name, current, "zero dispersion")

        z = (current - median) / mad
        score = z if self._direction == "upper" else abs(z)
        return AnomalyResult(
            is_anomaly=bool(score > self._k),
            score=round(score, 4),
            algorithm=self.name,
            threshold=self._k,
            current_value=current,
            expected_value=round(median, 4),
            ratio=round(abs(score) / self._k, 4),
            detail=f"robust_z={score:.3f} median={median:.3f} mad={mad:.3f}",
        )


class EWMADetector(BaseDetector):
    """EWMA：对近期数据加权，擅长发现缓慢漂移（如连接数逐步爬升）。"""

    name = "ewma"

    def __init__(self, alpha: float = 0.3, threshold: float = 3.0, direction: str = "upper", min_history: int = 10):
        self._alpha = alpha
        self._threshold = threshold
        self._direction = direction
        self._min_history = min_history

    def detect(self, values: Sequence[float], current: float) -> AnomalyResult:
        if len(values) < self._min_history:
            return _abstain(self.name, current, f"history<{self._min_history}")

        ewma = float(values[0])
        ewma_var = 0.0
        for value in values[1:]:
            diff = value - ewma
            ewma = self._alpha * value + (1 - self._alpha) * ewma
            ewma_var = self._alpha * diff**2 + (1 - self._alpha) * ewma_var
        ewma_std = float(np.sqrt(ewma_var))
        if ewma_std <= 1e-9:
            return _abstain(self.name, current, "zero variance")

        deviation = (current - ewma) / ewma_std
        score = deviation if self._direction == "upper" else abs(deviation)
        return AnomalyResult(
            is_anomaly=bool(score > self._threshold),
            score=round(score, 4),
            algorithm=self.name,
            threshold=self._threshold,
            current_value=current,
            expected_value=round(ewma, 4),
            ratio=round(abs(score) / self._threshold, 4),
            detail=f"deviation={score:.3f} ewma={ewma:.3f}",
        )


class IsolationForestDetector(BaseDetector):
    """Isolation Forest：无分布假设，适合多维/非高斯指标，冷启动样本不足时弃权。"""

    name = "iforest"

    def __init__(self, contamination: float = 0.02, min_history: int = 50, random_state: int = 42):
        self._contamination = contamination
        self._min_history = min_history
        self._random_state = random_state

    def detect(self, values: Sequence[float], current: float) -> AnomalyResult:
        if len(values) < self._min_history:
            return _abstain(self.name, current, f"history<{self._min_history}")

        from sklearn.ensemble import IsolationForest

        arr = np.asarray(values, dtype=float).reshape(-1, 1)
        model = IsolationForest(
            contamination=self._contamination,
            random_state=self._random_state,
            n_estimators=100,
        )
        model.fit(arr)
        point = np.asarray([[current]], dtype=float)
        raw_score = float(-model.decision_function(point)[0])
        # decision_function 的阈值约等于 0，把它映射成 ratio 便于统一分级
        ratio = max(raw_score, 0.0) / 0.1 if raw_score > 0 else 0.0
        return AnomalyResult(
            is_anomaly=bool(model.predict(point)[0] == -1),
            score=round(raw_score, 4),
            algorithm=self.name,
            threshold=0.0,
            current_value=current,
            expected_value=round(float(np.median(values)), 4),
            ratio=round(ratio, 4),
            detail=f"iforest_score={raw_score:.3f} contamination={self._contamination}",
        )


class StaticThresholdDetector(BaseDetector):
    """传统静态阈值，作为对照组：便宜但不懂业务波动。"""

    name = "static"

    def __init__(self, warn: float, direction: str = "upper"):
        self._warn = warn
        self._direction = direction

    def detect(self, values: Sequence[float], current: float) -> AnomalyResult:
        if self._direction == "upper":
            ratio = current / self._warn if self._warn else 0.0
            is_anomaly = current > self._warn
        else:
            ratio = abs(current) / self._warn if self._warn else 0.0
            is_anomaly = abs(current) > self._warn
        return AnomalyResult(
            is_anomaly=bool(is_anomaly),
            score=round(current, 4),
            algorithm=self.name,
            threshold=self._warn,
            current_value=current,
            expected_value=self._warn,
            ratio=round(ratio, 4),
            detail=f"threshold={self._warn}",
        )


@dataclass(frozen=True)
class MetricPolicy:
    """单个指标的检测策略：方向 + 静态阈值 + 最小样本量。"""

    name: str
    unit: str
    direction: str
    warn: float
    crit: float
    min_history: int = 20
    description: str = ""

    def severity_for(self, value: float, ratio: float) -> str:
        """静态阈值决定「业务上多严重」，统计 ratio 只把未越线但明显偏离的告警抬到 medium。"""
        if self.direction == "upper":
            if value >= self.crit:
                return "critical"
            if value >= self.warn:
                return "high"
        else:
            if abs(value) >= self.crit:
                return "critical"
            if abs(value) >= self.warn:
                return "high"
        if ratio >= 2.0:
            return "medium"
        return "low"


DB_METRIC_POLICIES: dict[str, MetricPolicy] = {
    "mysql_replication_lag_sec": MetricPolicy(
        name="mysql_replication_lag_sec",
        unit="秒",
        direction="upper",
        warn=5.0,
        crit=30.0,
        description="主从复制延迟，超过 30s 视为读一致性风险",
    ),
    "mysql_connections_used_ratio": MetricPolicy(
        name="mysql_connections_used_ratio",
        unit="%",
        direction="upper",
        warn=0.75,
        crit=0.90,
        description="连接数使用率，打满会直接拒绝新连接",
    ),
    "mysql_slow_query_qps": MetricPolicy(
        name="mysql_slow_query_qps",
        unit="条/秒",
        direction="upper",
        warn=20.0,
        crit=50.0,
        description="慢查询速率，通常由缺索引或统计信息过期引起",
    ),
    "mysql_cpu_usage_percent": MetricPolicy(
        name="mysql_cpu_usage_percent",
        unit="%",
        direction="both",
        warn=70.0,
        crit=90.0,
        description="数据库主机 CPU，双向异常都要关注",
    ),
    "mysql_disk_used_ratio": MetricPolicy(
        name="mysql_disk_used_ratio",
        unit="%",
        direction="upper",
        warn=0.80,
        crit=0.90,
        description="表空间使用率，超过 90% 需要扩容",
    ),
    "mysql_qps": MetricPolicy(
        name="mysql_qps",
        unit="次/秒",
        direction="both",
        warn=8000.0,
        crit=12000.0,
        description="QPS，突增看容量，突降看可用性",
    ),
}


def policy_for(metric_name: str) -> MetricPolicy:
    return DB_METRIC_POLICIES.get(
        metric_name,
        MetricPolicy(
            name=metric_name,
            unit="",
            direction="both",
            warn=float("inf"),
            crit=float("inf"),
            description="未注册指标，使用通用策略",
        ),
    )


class EnsembleDetector:
    """多算法投票。

    只有「有结论」的检测器参与投票；少于 `min_votes` 个有效结论时直接判正常，
    避免冷启动误报。返回的 `ratio` 用于告警分级。
    """

    def __init__(
        self,
        policy: Optional[MetricPolicy] = None,
        min_votes: int = 2,
        detectors: Optional[list[BaseDetector]] = None,
    ):
        self.policy = policy or MetricPolicy("generic", "", "both", float("inf"), float("inf"))
        direction = self.policy.direction
        self._detectors = detectors or [
            MADDetector(k=3.5, direction=direction),
            EWMADetector(alpha=0.3, threshold=3.0, direction=direction),
            IsolationForestDetector(contamination=0.02),
        ]
        self._min_votes = min_votes

    def detect(
        self, values: Sequence[float], current: float
    ) -> tuple[bool, float, list[AnomalyResult]]:
        results = [detector.detect(values, current) for detector in self._detectors]
        active = [r for r in results if not r.abstained]
        if len(active) < max(2, self._min_votes):
            return False, 0.0, results

        votes = sum(1 for r in active if r.is_anomaly)
        ratio = max((r.ratio for r in active), default=0.0)
        is_anomaly = votes >= self._min_votes
        if is_anomaly:
            logger.info(
                "[Ensemble] anomaly metric=%s value=%.3f votes=%d/%d ratio=%.2f",
                self.policy.name,
                current,
                votes,
                len(active),
                ratio,
            )
        return is_anomaly, round(ratio, 4), results


# --------------------------------------------------------------------- 演示数据
_DB_SERIES_SPECS = {
    "mysql_replication_lag_sec": dict(base=1.2, seasonal=0.4, noise=0.35, spike=42.0, seed=7),
    "mysql_connections_used_ratio": dict(base=0.58, seasonal=0.06, noise=0.015, spike=0.34, seed=11),
    "mysql_slow_query_qps": dict(base=9.0, seasonal=3.0, noise=2.0, spike=55.0, seed=13),
    "mysql_cpu_usage_percent": dict(base=42.0, seasonal=8.0, noise=4.0, spike=50.0, seed=17),
    "mysql_disk_used_ratio": dict(base=0.62, seasonal=0.04, noise=0.012, spike=0.28, seed=19),
}


def generate_db_series(
    metric_name: str,
    n_points: int = 600,
    inject_faults: bool = True,
    seed: Optional[int] = None,
) -> tuple[list[float], list[int], list[tuple[int, int]]]:
    """生成带标签的数据库指标：正弦周期 + 噪声 + 注入故障窗口。

    返回 (values, labels, windows)，windows 是 [(start, end), ...] 半开区间，
    评测脚本用它算检测时延。
    """
    spec = _DB_SERIES_SPECS.get(
        metric_name, dict(base=50.0, seasonal=10.0, noise=3.0, spike=40.0, seed=23)
    )
    rng = np.random.default_rng(seed if seed is not None else spec["seed"])
    t = np.arange(n_points)
    values = (
        spec["base"]
        + spec["seasonal"] * np.sin(2 * np.pi * t / 288)
        + rng.normal(0, spec["noise"], n_points)
    )

    labels = [0] * n_points
    windows: list[tuple[int, int]] = []
    if inject_faults and n_points >= 120:
        starts = [int(n_points * f) for f in (0.25, 0.55, 0.82)]
        for idx, start in enumerate(starts):
            length = 6 + (idx % 2) * 4
            end = min(start + length, n_points)
            ramp = np.linspace(0.2, 1.0, end - start)
            values[start:end] += spec["spike"] * ramp
            for i in range(start, end):
                labels[i] = 1
            windows.append((start, end))

        # CPU 指标是双向的：再注入一个「骤降」窗口（流量塌陷/实例假死）
        if metric_name == "mysql_cpu_usage_percent":
            start = int(n_points * 0.68)
            end = min(start + 8, n_points)
            ramp = np.linspace(0.3, 1.0, end - start)
            values[start:end] -= 25.0 * ramp
            for i in range(start, end):
                labels[i] = 1
            windows.append((start, end))

    if metric_name == "mysql_replication_lag_sec":
        values = np.clip(values, 0.0, None)
    if metric_name.endswith("_ratio"):
        values = np.clip(values, 0.0, 1.0)
    return [round(float(v), 4) for v in values], labels, windows


def generate_demo_metrics(n_points: int = 200, inject_anomaly: bool = True) -> tuple[list[float], list[float]]:
    """兼容旧接口：返回 (values, labels)。"""
    values, labels, _ = generate_db_series(
        "mysql_cpu_usage_percent", n_points=n_points, inject_faults=inject_anomaly
    )
    return values, [float(x) for x in labels]
