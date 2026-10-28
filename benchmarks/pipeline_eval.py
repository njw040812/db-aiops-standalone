"""端到端流水线性能评测。

对默认 5 节点工作流跑 N 次（每次独立 bus + orchestrator），统计：
- 全链路耗时 p50 / p95 / max（毫秒）
- 状态分布、审批策略分布、自愈执行成功率
- 人工审批中断后恢复执行的耗时

用法：
    python -m benchmarks.pipeline_eval
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.event_bus import InMemoryEventBus  # noqa: E402
from core.orchestrator import Orchestrator  # noqa: E402

RUNS = 30
WARMUP = 2


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(ratio * (len(ordered) - 1))))
    return ordered[index]


async def run_once(pending: bool = False) -> dict:
    bus = InMemoryEventBus()
    await bus.start()
    orchestrator = Orchestrator(bus)
    if pending:
        orchestrator.change._auto_threshold = -1.0
        orchestrator.change._manual_threshold = -1.0

    started = time.perf_counter()
    state = await orchestrator.run()
    elapsed_ms = (time.perf_counter() - started) * 1000

    result = {
        "status": state.status,
        "elapsed_ms": elapsed_ms,
        "policy": state.change_event.policy if state.change_event else None,
        "risk_score": state.change_event.risk_score if state.change_event else None,
        "heal_status": state.heal_event.status if state.heal_event else None,
        "nodes": len(orchestrator.get_workflow_status(state.incident_id)),
        "incident_id": state.incident_id,
    }

    if pending:
        approved = await orchestrator.resume(state.incident_id, approve=True, approver="dba-tech-lead")
        result["resume_status"] = approved.status
        result["resume_heal_status"] = approved.heal_event.status if approved.heal_event else None

    await bus.stop()
    return result


async def main() -> dict:
    for _ in range(WARMUP):
        await run_once()

    normal_runs = [await run_once() for _ in range(RUNS)]
    pending_runs = [await run_once(pending=True) for _ in range(max(5, RUNS // 5))]

    durations = [item["elapsed_ms"] for item in normal_runs]
    pending_durations = [item["elapsed_ms"] for item in pending_runs]
    resolved = sum(1 for item in normal_runs if item["status"] == "resolved")
    executed = sum(1 for item in normal_runs if item["heal_status"] == "succeeded")
    policies: dict[str, int] = {}
    for item in normal_runs:
        policies[item["policy"]] = policies.get(item["policy"], 0) + 1

    summary = {
        "runs": RUNS,
        "latency_ms": {
            "mean": round(statistics.mean(durations), 2),
            "p50": round(percentile(durations, 0.50), 2),
            "p95": round(percentile(durations, 0.95), 2),
            "max": round(max(durations), 2),
        },
        "resolved_rate": round(resolved / RUNS, 4),
        "execution_success_rate": round(executed / RUNS, 4),
        "policies": policies,
        "pending": {
            "runs": len(pending_runs),
            "resume_success_rate": round(
                sum(1 for item in pending_runs if item.get("resume_status") == "resolved") / len(pending_runs), 4
            ),
            "resume_latency_p50_ms": round(percentile(pending_durations, 0.5), 2),
        },
    }

    print("\n=== 端到端流水线评测 ===")
    print(f"运行次数        : {RUNS}")
    print(f"延迟 p50/p95    : {summary['latency_ms']['p50']} / {summary['latency_ms']['p95']} ms")
    print(f"平均/最大       : {summary['latency_ms']['mean']} / {summary['latency_ms']['max']} ms")
    print(f"处理成功率      : {summary['resolved_rate']:.0%}")
    print(f"自愈执行成功率  : {summary['execution_success_rate']:.0%}")
    print(f"审批策略分布    : {policies}")
    print(f"人工审批恢复    : {summary['pending']['resume_success_rate']:.0%}（n={len(pending_runs)}）")

    output = ROOT / "benchmarks" / "pipeline_results.json"
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"结果已写入 {output}")
    return summary


if __name__ == "__main__":
    asyncio.run(main())
