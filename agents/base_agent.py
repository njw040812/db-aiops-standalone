"""Agent 基类：模板方法 + 统一指标 + 异常语义。

关键约定（面试可讲）：
- `handle()` 负责埋点、计时、异常包装；子类只实现 `process()`；
- **异常必须向上抛**，由编排器决定是否重试。原实现把异常吞掉写成
  `state.error_message`，编排器既不知道失败、也无法真正重试；
- 「业务上不满足条件」（例如证据不足）不是异常，正常返回 state 即可。
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

from core.event_bus import EventBus
from models.events import AgentType, IncidentState

logger = logging.getLogger(__name__)


class AgentExecutionError(RuntimeError):
    """Agent 执行失败，编排器据此重试或终止工作流。"""

    def __init__(self, agent: str, message: str):
        super().__init__(f"[{agent}] {message}")
        self.agent = agent
        self.message = message


class BaseAgent(ABC):
    def __init__(self, agent_type: AgentType, event_bus: EventBus, name: Optional[str] = None):
        self.agent_type = agent_type
        self.name = name or agent_type.value
        self.event_bus = event_bus
        self._processed = 0
        self._errors = 0
        self._total_latency_ms = 0.0
        self._latencies: deque[float] = deque(maxlen=200)
        self._last_run_at: Optional[datetime] = None

    async def handle(self, state: IncidentState) -> IncidentState:
        start = time.perf_counter()
        self._last_run_at = datetime.now(timezone.utc)
        state.current_agent = self.agent_type
        state.updated_at = self._last_run_at

        try:
            state = await self.process(state)
        except AgentExecutionError:
            self._errors += 1
            raise
        except Exception as exc:
            self._errors += 1
            logger.exception("[%s] unexpected failure", self.name)
            raise AgentExecutionError(self.name, str(exc)) from exc
        else:
            self._processed += 1
            return state
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._latencies.append(elapsed_ms)
            self._total_latency_ms += elapsed_ms
            logger.debug("[%s] handled in %.1fms", self.name, elapsed_ms)

    @abstractmethod
    async def process(self, state: IncidentState) -> IncidentState:
        ...

    async def _publish(self, topic: str, event) -> None:
        await self.event_bus.publish(topic, event)

    def get_metrics(self) -> dict[str, Any]:
        return {
            "agent": self.name,
            "type": self.agent_type.value,
            "processed_count": self._processed,
            "error_count": self._errors,
            "avg_latency_ms": round(self._total_latency_ms / self._processed, 2) if self._processed else 0.0,
            "last_latency_ms": round(self._latencies[-1], 2) if self._latencies else 0.0,
            "p95_latency_ms": self._p95(),
            "last_run_at": self._last_run_at.isoformat() if self._last_run_at else None,
        }

    def _p95(self) -> float:
        if not self._latencies:
            return 0.0
        ordered = sorted(self._latencies)
        index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
        return round(ordered[index], 2)

    def reset_metrics(self) -> None:
        self._processed = 0
        self._errors = 0
        self._total_latency_ms = 0.0
        self._latencies.clear()
        self._last_run_at = None
