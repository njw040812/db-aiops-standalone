"""异常检测离线回放评测。

对 5 个数据库指标各生成 600 个点（含 3 个注入故障窗口），逐点回放，
对比四种检测器的点级精确率/召回率/F1、每千点误报数、故障窗口检出率
和首次告警时延。结果写入 benchmarks/detect_results.json。

用法：
    python -m benchmarks.detect_eval
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.time_series import (  # noqa: E402
    EnsembleDetector,
    MADDetector,
    StaticThresholdDetector,
    ThreeSigmaDetector,
    generate_db_series,
    policy_for,
)

METRICS = [
    "mysql_replication_lag_sec",
    "mysql_connections_used_ratio",
    "mysql_slow_query_qps",
    "mysql_cpu_usage_percent",
    "mysql_disk_used_ratio",
]
POINTS = 600
WARMUP = 50


def build_detector(name: str, policy):
    if name == "static_warn":
        return StaticThresholdDetector(warn=policy.warn, direction=policy.direction)
    if name == "static_crit":
        return StaticThresholdDetector(warn=policy.crit, direction=policy.direction)
    if name == "3sigma":
        return ThreeSigmaDetector(sigma=3.0, direction=policy.direction)
    if name == "mad":
        return MADDetector(k=3.5, direction=policy.direction)
    if name == "ensemble":
        return EnsembleDetector(policy=policy, min_votes=2)
    raise ValueError(name)


def evaluate(metric: str, detector_name: str, points: int = POINTS) -> dict:
    policy = policy_for(metric)
    values, labels, windows = generate_db_series(metric, n_points=points, inject_faults=True)
    detector = build_detector(detector_name, policy)

    alarms: list[bool] = [False] * points
    for index in range(WARMUP, points):
        outcome = detector.detect(values[:index], values[index])
        if isinstance(outcome, tuple):
            is_anomaly = outcome[0]
        else:
            is_anomaly = outcome.is_anomaly
        alarms[index] = bool(is_anomaly)

    tp = sum(1 for i in range(WARMUP, points) if alarms[i] and labels[i] == 1)
    fp = sum(1 for i in range(WARMUP, points) if alarms[i] and labels[i] == 0)
    fn = sum(1 for i in range(WARMUP, points) if not alarms[i] and labels[i] == 1)
    normal_points = sum(1 for i in range(WARMUP, points) if labels[i] == 0)

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    delays: list[int] = []
    for start, end in windows:
        hit = next((i for i in range(start, min(end + 30, points)) if alarms[i]), None)
        if hit is not None:
            delays.append(hit - start)

    return {
        "metric": metric,
        "detector": detector_name,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_alarms_per_1000": round(fp / normal_points * 1000, 2) if normal_points else 0.0,
        "window_detection_rate": round(len(delays) / len(windows), 4) if windows else 0.0,
        "median_delay_points": statistics.median(delays) if delays else None,
        "delays": delays,
    }


def aggregate(rows: list[dict]) -> dict:
    tp = sum(row["tp"] for row in rows)
    fp = sum(row["fp"] for row in rows)
    fn = sum(row["fn"] for row in rows)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    delays = [delay for row in rows for delay in row["delays"]]
    return {
        "detector": rows[0]["detector"],
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "detected_windows": len(delays),
        "median_delay_points": statistics.median(delays) if delays else None,
    }


def main() -> dict:
    details: list[dict] = []
    aggregates: list[dict] = []
    window_total = 0

    for metric in METRICS:
        _, _, windows = generate_db_series(metric, n_points=POINTS, inject_faults=True)
        window_total += len(windows)

    for detector_name in ("static_warn", "static_crit", "3sigma", "mad", "ensemble"):
        rows = [evaluate(metric, detector_name) for metric in METRICS]
        details.extend(rows)
        summary = aggregate(rows)
        summary["window_detection_rate"] = (
            round(summary["detected_windows"] / window_total, 4) if window_total else 0.0
        )
        aggregates.append(summary)

    print("\n=== 异常检测回放评测（5 指标 × 600 点，3 个注入故障窗口/指标）===\n")
    print(f"{'检测器':<14}{'P':>8}{'R':>8}{'F1':>8}{'误报/千点':>12}{'窗口检出':>10}{'中位时延':>10}")
    print("-" * 74)
    false_per_1000 = {
        row["detector"]: round(
            sum(r["false_alarms_per_1000"] for r in details if r["detector"] == row["detector"]) / len(METRICS), 2
        )
        for row in aggregates
    }
    for row in aggregates:
        median_delay = row["median_delay_points"]
        print(
            f"{row['detector']:<14}{row['precision']:>8.3f}{row['recall']:>8.3f}{row['f1']:>8.3f}"
            f"{false_per_1000[row['detector']]:>12.1f}{row['window_detection_rate']:>10.0%}"
            f"{(f'{median_delay:.0f}' if median_delay is not None else '-'):>10}"
        )

    print("\n--- ensemble 分指标 ---")
    print(f"{'指标':<32}{'P':>7}{'R':>7}{'F1':>7}{'误报/千点':>11}{'时延':>6}")
    print("-" * 72)
    for row in details:
        if row["detector"] != "ensemble":
            continue
        delay = row["median_delay_points"]
        delay_text = f"{delay:.0f}" if delay is not None else "-"
        print(
            f"{row['metric']:<32}{row['precision']:>7.3f}{row['recall']:>7.3f}{row['f1']:>7.3f}"
            f"{row['false_alarms_per_1000']:>11.1f}{delay_text:>6}"
        )

    payload = {
        "config": {"metrics": METRICS, "points": POINTS, "warmup": WARMUP, "window_total": window_total},
        "aggregate": aggregates,
        "false_alarms_per_1000_mean": false_per_1000,
        "details": details,
    }
    output = ROOT / "benchmarks" / "detect_results.json"
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n结果已写入 {output}")
    return payload


if __name__ == "__main__":
    main()
