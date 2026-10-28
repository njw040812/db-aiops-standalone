# Agent 设计细节

## 统一抽象：模板方法 + 异常语义

```python
class BaseAgent(ABC):
    async def handle(self, state: IncidentState) -> IncidentState:
        # 计时、埋点、current_agent、异常包装
        try:
            state = await self.process(state)
        except Exception as exc:
            raise AgentExecutionError(self.name, str(exc)) from exc
        ...

    @abstractmethod
    async def process(self, state: IncidentState) -> IncidentState: ...
```

两个约定：

1. **异常必须向上抛**。原实现把异常写进 `state.error_message` 就返回，编排器以为成功，
   重试永远不会发生；
2. **业务上不满足条件不算异常**。比如 RCA 证据不足，返回 `state.status="escalated"` 即可。

## MonitorAgent：检测、分级、收敛

```
历史基线（>= 20 点）
  → 指标策略：direction / warn / crit
    → MAD（鲁棒）+ EWMA（漂移）+ iForest（无分布假设）
      → 只统计有结论的算法，>= 2 票才报
        → 静态阈值定级，ratio 兜底抬到 medium
          → 指纹 300s 去重 + 同服务 60s 收敛
            → AlertEvent
```

关键取舍：

- 主从延迟、连接数只检测上冲，避免「延迟归零」误报；
- 冷启动弃权，不拿 3 个点算异常；
- 严重度优先看静态阈值，统计分数只做补充。

## RCAAgent：图谱 + 变更 + 归一化贝叶斯

```
1. affected = [告警对象] + trace_dependencies(告警对象)
2. changes  = find_recent_changes(affected, 24h)
3. evidence = 指标异常 + 变更 + 共置主机 + 长事务/慢查询/日志
4. hypotheses = FAULT_CATALOG[metric]
5. weight_i = prior_i × likelihood_i(evidence)
6. posterior_i = weight_i / Σ weight
7. 置信度 < 0.30 或 margin < 0.05 → needs_human
```

`likelihood` 是证据命中函数，例如：

| 假设 | 需要的证据 | 命中 | 未命中 |
|------|-----------|------|--------|
| replica_ddl | 24h 内有 ddl 变更 | 0.90 | 0.25 |
| long_transaction | 存在长事务 | 0.90 | 0.20 |
| shared_host | 与告警对象共置主机 | 0.70 | 0.30 |
| none | 无 | 0.45 | 0.45 |

演示场景：副本 9 小时前执行过 gh-ost 加索引，24h 内命中了 `ddl`，
`replica_ddl` 的后验为 0.50，高于 `write_burst` 的 0.24，于是建议 `kill_ddl` 而不是重启主库。

## HealAgent：两阶段

```
propose():
  选择 Playbook（按 RCA 建议顺序）
  → 爆炸半径 > 0.20 强制 L2
  → 渲染命令（SafeDict 兜底）
  → executor.preflight() 做 dry-run
  → HealEvent(status=proposed)

execute():
  校验 change.approval_status == approved，否则 PermissionError
  → 熔断器放行？
  → 已执行过则跳过（幂等）
  → executor.run()
  → 成功：succeeded / 失败：failed + 回滚 + 熔断计数
```

Playbook 的 14 个动作分三档：

- L0：回收连接池、限流批量任务、清理 binlog、ANALYZE TABLE；
- L1：KILL 长事务、终止 DDL、切读流量、回滚参数、扩容表空间、重启复制、扩容；
- L2：归档表、在线加索引、主从切换。

## ChangeAgent：风险模型与审批策略

```
risk = 0.30·blast_radius
     + 0.25·operation_risk
     + 0.15·window_factor
     + 0.10·criticality
     + 0.10·history_risk
     + 0.10·data_loss
```

| 条件 | 结果 |
|------|------|
| L2 | pending + `CHG-` 工单，必须 DBA TL |
| 需要窗口且业务时段 | pending（CHANGE_WINDOW_REQUIRED） |
| L1 且 ≤ 0.55 | approved（当班） |
| L1 且 > 0.55 | pending（L1_PENDING） |
| L0 且 ≤ 0.30 | approved（自动） |
| L0 且 > 0.30 | pending（转当班） |

失败一次后 `history_risk` 按 EWMA 抬高，下一次同类操作更容易被挂起。

## Orchestrator：节点、重试、检查点

- `NodeSpec` 不可变：handler、condition、max_retries；
- `NodeRun` 每次运行独立，按 incident 隔离；
- 重试耗尽 → 终止工作流，不执行后续节点；
- 每个节点完成后深拷贝检查点；
- 人工审批后 `resume()` 只执行 execute 节点。

## 面试怎么讲

1. **为什么多 Agent**：检测、根因、执行、审批的关注点和权限不同，拆开才能分别测试；
2. **怎么协作**：状态机顺序推进 + 事件总线广播 + 共享图谱；
3. **怎么保证安全**：两阶段变更 + 审批门禁 + 幂等 + 熔断 + 回滚 + 审计。
