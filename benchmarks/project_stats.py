"""项目规模统计：代码行数、测试数、Playbook 数、图谱规模。

用法：
    python -m benchmarks.project_stats
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.heal_agent import PLAYBOOKS  # noqa: E402
from core.knowledge_graph import create_demo_knowledge_graph  # noqa: E402


def count_lines(paths: list[Path]) -> tuple[int, int]:
    total = 0
    non_empty = 0
    for path in paths:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            total += 1
            if line.strip():
                non_empty += 1
    return total, non_empty


def main() -> dict:
    packages = ["agents", "core", "models", "api", "config"]
    source_files: list[Path] = []
    for package in packages:
        source_files.extend(sorted((ROOT / package).rglob("*.py")))
    test_files = sorted((ROOT / "tests").rglob("*.py"))
    benchmark_files = sorted((ROOT / "benchmarks").rglob("*.py"))

    source_total, source_non_empty = count_lines(source_files)
    test_total, test_non_empty = count_lines(test_files)
    bench_total, bench_non_empty = count_lines(benchmark_files)

    collected = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    test_count = 0
    for line in collected.stdout.splitlines():
        # pytest -q --collect-only 的输出形如 "tests/test_api.py: 17"
        match = re.match(r"^(.+\.py):\s+(\d+)$", line.strip())
        if match:
            test_count += int(match.group(2))

    kg = create_demo_knowledge_graph()
    summary = kg.summary()

    stats = {
        "source": {
            "files": len(source_files),
            "lines": source_total,
            "non_empty_lines": source_non_empty,
        },
        "tests": {"files": len(test_files), "lines": test_total, "count": test_count},
        "benchmarks": {"files": len(benchmark_files), "lines": bench_total},
        "playbooks": len(PLAYBOOKS),
        "knowledge_graph": {
            "nodes": summary["total_nodes"],
            "edges": summary["total_edges"],
            "node_types": summary["node_types"],
        },
    }

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    (ROOT / "benchmarks" / "project_stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return stats


if __name__ == "__main__":
    main()
