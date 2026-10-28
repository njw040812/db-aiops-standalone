# 从零跑通：DB-AIOps 入门

> 面向第一次接触这个仓库的人。跑完这一篇，你能看到一次完整的
> 「告警 → 根因 → 提案 → 审批 → 执行 → 审计」。

## 前置知识

- 知道 HTTP 的 GET/POST 即可；
- 知道 MySQL 主从复制、慢查询的大致含义更好；
- 不需要 Docker、Kafka、Neo4j。

## 第一步：理解问题

数据库运维里一次主从延迟告警，通常要经过：

1. 看监控确认延迟多高、持续多久；
2. 翻 CMDB 确认副本从哪个主库复制、还有谁依赖它；
3. 翻变更记录，看最近有没有 DDL / 参数调整；
4. 判断是 KILL 会话、重启复制、还是切读流量；
5. 找人审批，执行，留记录。

项目把 1–3 做成监控 + 根因 Agent，4 做成 Playbook 提案，5 做成审批 + 执行 + 审计。
**执行永远发生在审批之后。**

## 第二步：项目结构

```
api/main.py                 FastAPI 入口
core/orchestrator.py        5 节点工作流（monitor → rca → heal → change → execute）
core/knowledge_graph.py     服务/主从/主机拓扑与图算法
core/event_bus.py           内存/Kafka 双模事件总线
core/audit.py               哈希链审计
agents/monitor_agent.py     异常检测、分级、去重
agents/rca_agent.py         根因：图谱 + 变更 + 归一化贝叶斯
agents/heal_agent.py        Playbook、dry-run、审批后执行
agents/change_agent.py      6 因子风险模型、审批、工单
models/time_series.py       检测算法与指标策略
tests/                      108 个用例
benchmarks/                 可复现评测
```

## 第三步：安装与运行

```bash
python -m venv .venv
.venv\Scripts\activate        # macOS/Linux: source .venv/bin/activate

pip install -r requirements.txt
# 网络慢可以加：-i https://pypi.tuna.tsinghua.edu.cn/simple
```

跑 Demo：

```bash
python -m core.orchestrator
```

典型输出（节选）：

```
[1] 监控告警
    指标      : mysql_replication_lag_sec = 46.2
    服务      : mysql-replica-2 / 严重度 critical
    算法投票  : mad=异常, ewma=异常, iforest=异常

[2] 根因分析
    根因      : 只读副本执行 DDL 阻塞复制回放，导致延迟堆积
    类型/置信度: replica_ddl / 0.50
    影响链    : mysql-replica-2 → mysql-primary

[3] 自愈提案（未执行）
    Playbook  : kill_ddl [L1]
    命令      : mysql -h db-node-3 -e "KILL QUERY 1024871;"
    dry-run   : DRY-RUN OK: 命令通过白名单与危险模式校验

[4] 变更审批
    结果      : approved / 风险分 0.241
    策略      : L1_ONCALL_APPROVED

[5] 执行结果
    状态      : succeeded
```

## 第四步：跑测试和评测

```bash
python -m pytest                     # 108 passed
python -m benchmarks.detect_eval     # 检测回放
python -m benchmarks.pipeline_eval   # 30 次端到端
python -m benchmarks.project_stats   # 代码/测试/图谱规模
```

## 第五步：启动 API

```bash
python -m uvicorn api.main:app --reload --port 8000
```

打开 http://localhost:8000/docs，重点试这几个：

```bash
# 触发一次默认故障（主从延迟）
curl -X POST http://localhost:8000/api/v1/incidents/trigger

# 查看拓扑
curl http://localhost:8000/api/v1/topology/mysql-replica-2/dependencies

# 查看审计链是否完整
curl http://localhost:8000/api/v1/audit/verify
```

## 第六步：代码执行流程

```
1. Orchestrator.run()
2. MonitorAgent：历史基线 → MAD/EWMA/iForest 投票 → 分级 → 去重 → AlertEvent
3. 条件：alert_event 存在 → RCAAgent
4. RCAAgent：图谱依赖链 + 24h 变更 + 证据 → 归一化后验
   置信度 < 0.30 或领先 < 0.05 → needs_human → 跳过自愈
5. HealAgent：匹配 Playbook → 爆炸半径 → dry-run → HealEvent(proposed)
6. ChangeAgent：6 因子风险 → L0 自动 / L1 当班 / L2 工单
   pending → 工作流中断，等 /approve
7. HealAgent.execute：校验 approved → 白名单 → 幂等 → 执行 → 失败回滚
8. AuditSink：审批与执行事件写入哈希链
```

## 下一步

- [02-event-driven.md](02-event-driven.md) — 事件与解耦
- [03-agent-design.md](03-agent-design.md) — 四个 Agent 的设计细节
- [04-knowledge-graph.md](04-knowledge-graph.md) — 图谱与根因算法
- [05-deploy.md](05-deploy.md) — 部署与边界
