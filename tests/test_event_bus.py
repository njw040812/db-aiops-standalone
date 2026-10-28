"""事件总线：订阅、异常隔离、异步 handler、日志上限。"""

from __future__ import annotations

from models.events import AgentType, EventType, BaseEvent


class DummyEvent(BaseEvent):
    event_type: EventType = EventType.ALERT_FIRED
    source_agent: AgentType = AgentType.MONITOR


async def test_publish_reaches_subscriber(bus):
    received: list[dict] = []

    async def handler(event: dict) -> None:
        received.append(event)

    await bus.subscribe("aiops.alerts", "group-a", handler)
    await bus.publish("aiops.alerts", DummyEvent())

    assert len(received) == 1
    assert received[0]["event_type"] == "alert.fired"


async def test_handler_failure_does_not_block_others(bus):
    received: list[str] = []

    def broken(_: dict) -> None:
        raise RuntimeError("handler down")

    async def healthy(event: dict) -> None:
        received.append(event["event_id"])

    await bus.subscribe("aiops.alerts", "broken-group", broken)
    await bus.subscribe("aiops.alerts", "healthy-group", healthy)

    await bus.publish("aiops.alerts", DummyEvent())

    assert len(received) == 1
    assert bus.handler_errors == 1


async def test_same_group_is_not_duplicated(bus):
    calls: list[int] = []

    async def first(_: dict) -> None:
        calls.append(1)

    async def second(_: dict) -> None:
        calls.append(2)

    await bus.subscribe("aiops.alerts", "group-a", first)
    await bus.subscribe("aiops.alerts", "group-a", second)
    await bus.publish("aiops.alerts", DummyEvent())

    assert calls == [2]


async def test_event_log_is_bounded():
    from core.event_bus import InMemoryEventBus

    bus = InMemoryEventBus(max_log=2)
    for _ in range(5):
        await bus.publish("aiops.alerts", DummyEvent())

    assert len(bus.get_event_log()) == 2


async def test_clear_resets_log_and_errors(bus):
    async def broken(_: dict) -> None:
        raise RuntimeError("x")

    await bus.subscribe("aiops.alerts", "broken", broken)
    await bus.publish("aiops.alerts", DummyEvent())
    assert bus.handler_errors == 1

    bus.clear()
    assert bus.get_event_log() == []
    assert bus.handler_errors == 0


async def test_create_event_bus_factory():
    from core.event_bus import InMemoryEventBus, create_event_bus

    assert isinstance(create_event_bus(use_kafka=False), InMemoryEventBus)
