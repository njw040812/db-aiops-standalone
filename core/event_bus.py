"""事件总线。

Agent 之间不直接互相调用，只通过 topic 通信：
    aiops.alerts   ← MonitorAgent
    aiops.events   ← RCAAgent
    aiops.commands ← HealAgent
    aiops.audit    ← ChangeAgent（由 AuditSink 消费并哈希链落盘）

两个实现：
- InMemoryEventBus：本地开发和单测用，无外部依赖；
- KafkaEventBus：生产用，手动 commit + 死信队列 + 阻塞调用丢线程池。

注意：Kafka 的 `poll()` / `flush()` 是阻塞的，直接在协程里调用会把事件循环
卡死，所以这里统一用 `asyncio.to_thread` 包一层。
"""

from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

EventHandler = Callable[[dict[str, Any]], Any]


class EventBus(ABC):
    """事件总线抽象基类（策略模式，方便切换 Kafka / 内存实现）。"""

    @abstractmethod
    async def publish(self, topic: str, event: BaseModel) -> None: ...

    @abstractmethod
    async def subscribe(self, topic: str, group_id: str, handler: EventHandler) -> None: ...

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...


class InMemoryEventBus(EventBus):
    """内存事件总线。

    订阅者按消费组隔离：同一 group 内只保留一个 handler，避免同一 Agent
    被重复注册导致重复处理。
    """

    def __init__(self, max_log: int = 2000):
        self._subscribers: dict[str, dict[str, EventHandler]] = defaultdict(dict)
        self._running = False
        self._event_log: deque[dict[str, Any]] = deque(maxlen=max_log)
        self._handler_errors: int = 0

    async def publish(self, topic: str, event: BaseModel) -> None:
        event_data = event.model_dump(mode="json")
        self._event_log.append(
            {
                "topic": topic,
                "event_type": event_data.get("event_type"),
                "correlation_id": event_data.get("correlation_id"),
                "timestamp": event_data.get("timestamp"),
                "event": event_data,
            }
        )
        logger.debug("[EventBus] publish %s -> %s", topic, event_data.get("event_type"))

        for group_id, handler in list(self._subscribers.get(topic, {}).items()):
            try:
                result = handler(event_data)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # 单个订阅者失败不影响其他订阅者
                self._handler_errors += 1
                logger.error("[EventBus] handler %s failed on %s: %s", group_id, topic, exc)

    async def subscribe(self, topic: str, group_id: str, handler: EventHandler) -> None:
        self._subscribers[topic][group_id] = handler
        logger.info("[EventBus] group %s subscribed to %s", group_id, topic)

    async def start(self) -> None:
        self._running = True

    async def stop(self) -> None:
        self._running = False

    def get_event_log(self) -> list[dict[str, Any]]:
        return list(self._event_log)

    def clear(self) -> None:
        self._event_log.clear()
        self._handler_errors = 0

    @property
    def handler_errors(self) -> int:
        return self._handler_errors


class KafkaEventBus(EventBus):
    """Kafka 事件总线（可选依赖 confluent-kafka）。

    - producer: acks=all，保证故障处理事件不丢；
    - consumer: 手动 commit，处理成功才提交，失败进 `<topic>.dlq`；
    - 所有阻塞调用都放进线程池，避免阻塞事件循环。
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092", max_retries: int = 3):
        self._bootstrap_servers = bootstrap_servers
        self._max_retries = max_retries
        self._producer = None
        self._consumers: list[tuple[asyncio.Task, Any]] = []
        self._pending: list[tuple[str, str, EventHandler]] = []
        self._running = False
        self._producer_lock = asyncio.Lock()

    def _ensure_producer(self):
        if self._producer is None:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "acks": "all",
                    "retries": self._max_retries,
                    "retry.backoff.ms": 100,
                }
            )
        return self._producer

    async def publish(self, topic: str, event: BaseModel) -> None:
        payload = event.model_dump_json().encode("utf-8")
        async with self._producer_lock:
            producer = self._ensure_producer()

            def _produce() -> None:
                producer.produce(topic=topic, value=payload, callback=self._delivery_callback)
                producer.flush(timeout=5)

            await asyncio.to_thread(_produce)
        logger.debug("[KafkaEventBus] published to %s", topic)

    async def subscribe(self, topic: str, group_id: str, handler: EventHandler) -> None:
        self._pending.append((topic, group_id, handler))

    async def start(self) -> None:
        from confluent_kafka import Consumer

        self._running = True
        for topic, group_id, handler in self._pending:
            consumer = Consumer(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "group.id": group_id,
                    "auto.offset.reset": "latest",
                    "enable.auto.commit": False,
                }
            )
            consumer.subscribe([topic])
            task = asyncio.create_task(self._consume_loop(consumer, topic, handler))
            self._consumers.append((task, consumer))
        self._pending.clear()
        logger.info("[KafkaEventBus] started with %d consumers", len(self._consumers))

    async def _consume_loop(self, consumer, topic: str, handler: EventHandler) -> None:
        while self._running:
            msg = await asyncio.to_thread(consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("[KafkaEventBus] consumer error: %s", msg.error())
                continue
            try:
                event_data = json.loads(msg.value().decode("utf-8"))
                result = handler(event_data)
                if asyncio.iscoroutine(result):
                    await result
                await asyncio.to_thread(consumer.commit, False)
            except Exception as exc:
                logger.error("[KafkaEventBus] handler failed: %s", exc)
                await self._send_to_dlq(topic, msg.value())

    async def stop(self) -> None:
        self._running = False
        for task, consumer in self._consumers:
            task.cancel()
            await asyncio.to_thread(consumer.close)
        self._consumers.clear()
        if self._producer is not None:
            await asyncio.to_thread(self._producer.flush, 10)
        logger.info("[KafkaEventBus] stopped")

    async def _send_to_dlq(self, original_topic: str, value: bytes) -> None:
        dlq_topic = f"{original_topic}.dlq"
        producer = self._ensure_producer()

        def _produce() -> None:
            producer.produce(topic=dlq_topic, value=value)
            producer.flush(timeout=5)

        await asyncio.to_thread(_produce)
        logger.warning("[KafkaEventBus] message moved to %s", dlq_topic)

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.error("[KafkaEventBus] delivery failed: %s", err)


def create_event_bus(use_kafka: bool = False, **kwargs) -> EventBus:
    if use_kafka:
        return KafkaEventBus(**kwargs)
    return InMemoryEventBus(**kwargs)


def event_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()
