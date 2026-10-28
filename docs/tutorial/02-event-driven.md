# 事件驱动与解耦

## 两种调用方式

同步 RPC：

```
A 调用 B → B 处理 → 返回 → A 继续
A 必须等 B，B 挂了 A 也挂
```

事件驱动：

```
A 发布事件 → 立即返回
B 订阅事件 → 自己处理
A 不知道 B 存在
```

## 本项目的真实做法

需要说清楚：**当前 Agent 由 Orchestrator 直接调用**，事件总线负责三件事：

1. 广播处理结果，供审计/测试/外部系统消费；
2. 解耦「谁产生事件」和「谁记录事件」；
3. 为切换到 Kafka 保留接口。

如果改成完全事件驱动，Agent 需要自己订阅 topic 并在消费端维护状态；
这会把「审批中断」的时序控制从编排器转移到消息层，复杂度更高，所以没有做。

### Topic 设计

| Topic | 发布者 | 当前订阅者 | 内容 |
|-------|--------|-----------|------|
| `aiops.alerts` | MonitorAgent | 审计/测试/外部告警平台 | 告警事件 |
| `aiops.events` | RCAAgent | 审计/测试 | 根因结果 |
| `aiops.commands` | HealAgent | 执行侧/测试 | 提案与执行结果 |
| `aiops.audit` | ChangeAgent、HealAgent | AuditSink | 审批、执行、手动审批 |
| `*.dlq` | KafkaEventBus | 运维人员 | 消费失败消息 |

### 事件模型

所有事件继承 `BaseEvent`：

```python
class BaseEvent(BaseModel):
    event_id: str
    event_type: EventType
    timestamp: datetime
    source_agent: AgentType
    correlation_id: str   # 同一故障链共用 incident_id
```

`correlation_id` 让一次故障的所有事件可以串起来，跨 topic 也能追踪。

### 双模实现

```python
def create_event_bus(use_kafka: bool = False):
    if use_kafka:
        return KafkaEventBus()
    return InMemoryEventBus()
```

内存实现按消费组去重（同一 group 只保留一个 handler），handler 异常不会影响其他订阅者。
Kafka 实现的阻塞调用（`produce`/`flush`/`poll`）都用 `asyncio.to_thread` 包住，避免卡住事件循环。

## Kafka 复习（通用知识）

### Producer

```
acks=all          → 所有 ISR 确认
retries=3         → 失败重试
enable.idempotence=true → 幂等，避免重试重复
```

### Consumer

```
enable.auto.commit=false → 处理完手动提交
auto.offset.reset=latest
group.id=db-aiops-agents
```

### DLQ

消费失败的消息写到 `topic.dlq`，记录原始 topic、失败原因、重试次数，人工处理。
本项目的 Kafka 实现会把失败消息转发到 `<topic>.dlq`。

## 面试问答

**Q：Kafka 怎么保证不丢消息？**
A：Producer `acks=all` + Broker `min.insync.replicas>=2` + Consumer 手动 commit。

**Q：怎么避免重复消费？**
A：消费端用 `event_id` 做幂等；或者用 Kafka 事务把「处理 + 提交 offset」放进同一事务。

**Q：背压怎么处理？**
A：Kafka 是 pull 模型，消费者处理不过来就不拉；应用层再加限流和有界队列。

**Q：为什么本项目没有直接上 Kafka？**
A：没有 broker 环境，联调不了就不写在简历里；内存实现保证 clone 下来能跑，
接口对齐后生产可替换。
