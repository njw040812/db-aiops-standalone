"""异常检测算法：冷启动、方向、投票、标注数据。"""

from __future__ import annotations

import pytest

from models.time_series import (
    AnomalyResult,
    EWMADetector,
    EnsembleDetector,
    IsolationForestDetector,
    MADDetector,
    StaticThresholdDetector,
    ThreeSigmaDetector,
    generate_db_series,
    generate_demo_metrics,
    policy_for,
)


def _normal_lag(n: int = 80) -> list[float]:
    values, _, _ = generate_db_series("mysql_replication_lag_sec", n_points=n, inject_faults=False)
    return values


def test_mad_detects_upper_spike():
    detector = MADDetector(k=3.5, direction="upper")
    result = detector.detect(_normal_lag(), 46.2)
    assert result.is_anomaly is True
    assert result.score > 3.5
    assert result.abstained is False


def test_mad_upper_direction_ignores_drop():
    detector = MADDetector(k=3.5, direction="upper")
    result = detector.detect(_normal_lag(), 0.0)
    assert result.is_anomaly is False


def test_three_sigma_needs_enough_history():
    detector = ThreeSigmaDetector()
    result = detector.detect([1.0, 2.0, 3.0], 100.0)
    assert result.abstained is True
    assert result.is_anomaly is False


def test_ewma_detects_drift():
    detector = EWMADetector(alpha=0.3, threshold=3.0, direction="upper")
    result = detector.detect(_normal_lag(), 46.2)
    assert result.is_anomaly is True


def test_isolation_forest_optional_and_detects():
    pytest.importorskip("sklearn")
    detector = IsolationForestDetector(contamination=0.02, min_history=50)
    result = detector.detect(_normal_lag(120), 46.2)
    assert result.abstained is False
    assert result.is_anomaly is True


def test_isolation_forest_abstains_on_cold_start():
    detector = IsolationForestDetector(min_history=50)
    result = detector.detect([1.0] * 10, 9.0)
    assert result.abstained is True


def test_static_threshold_is_baseline():
    detector = StaticThresholdDetector(warn=5.0, direction="upper")
    assert detector.detect([], 6.0).is_anomaly is True
    assert detector.detect([], 4.0).is_anomaly is False


def test_ensemble_cold_start_does_not_alarm():
    policy = policy_for("mysql_replication_lag_sec")
    detector = EnsembleDetector(policy=policy, min_votes=2)
    is_anomaly, ratio, results = detector.detect([1.0, 1.1, 0.9], 99.0)
    assert is_anomaly is False
    assert ratio == 0.0
    assert all(result.abstained for result in results)


def test_ensemble_two_detectors_agree():
    policy = policy_for("mysql_replication_lag_sec")
    detector = EnsembleDetector(
        policy=policy,
        min_votes=2,
        detectors=[MADDetector(k=3.5), EWMADetector(alpha=0.3, threshold=3.0)],
    )
    is_anomaly, ratio, results = detector.detect(_normal_lag(120), 46.2)
    assert is_anomaly is True
    assert ratio > 1.0
    assert sum(1 for r in results if r.is_anomaly) == 2


def test_ensemble_requires_votes():
    policy = policy_for("mysql_replication_lag_sec")
    detector = EnsembleDetector(
        policy=policy,
        min_votes=2,
        detectors=[
            MADDetector(k=3.5),
            StaticThresholdDetector(warn=1e9),
        ],
    )
    is_anomaly, _, results = detector.detect(_normal_lag(120), 46.2)
    assert is_anomaly is False
    assert sum(1 for r in results if r.is_anomaly) == 1


def test_metric_policy_severity():
    policy = policy_for("mysql_replication_lag_sec")
    assert policy.severity_for(46.0, 10.0) == "critical"
    assert policy.severity_for(6.0, 1.2) == "high"
    assert policy.severity_for(3.0, 2.2) == "medium"
    assert policy.severity_for(1.0, 0.5) == "low"


def test_generate_db_series_labels_match_windows():
    values, labels, windows = generate_db_series("mysql_replication_lag_sec", 600, True)
    assert len(values) == len(labels) == 600
    assert windows
    for start, end in windows:
        assert all(label == 1 for label in labels[start:end])
    assert all(label == 0 for label in labels[: windows[0][0]])


def test_generate_demo_metrics_compat():
    values, labels = generate_demo_metrics(200, True)
    assert len(values) == len(labels) == 200
    assert any(labels)


def test_anomaly_result_to_dict_is_json_safe():
    result = AnomalyResult(
        is_anomaly=True,
        score=3.2,
        algorithm="mad",
        threshold=3.5,
        current_value=1.0,
        expected_value=1.0,
    )
    payload = result.to_dict()
    assert payload["is_anomaly"] is True
    assert isinstance(payload["score"], float)
