# DB-AIOps · 数据库智能运维多 Agent 系统

> 把「告警 → 根因 → 修复提案 → 变更审批 → 执行」做成一条可回放、可审计、可阻断的流水线。
> 面向 MySQL 一主两从场景，Python 单语言实现，不依赖外部服务即可完整跑通。

> **来源说明**：本仓库是开源项目
> [bcefghj/multi-agent-aiops](https://github.com/bcefghj/multi-agent-aiops)（MIT）的 GitHub fork，
> 在其 Python 实现基础上二次开发。原项目版权归原作者，本仓库修改部分归倪建伟。
> 具体改动见文末「来源与致谢」。

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Tests](https://img.shields.io/badge/tests-108%20passed-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

---

## 1. 这个项目解决什么问题

数据库运维有三个反复出现的问题：

| 问题 | 现实表现 | 本项目的做法 |
|------|---------|-------------|
| 告警噪音 | 主从延迟、连接数、慢查询各自为战，凌晨被 P3 告警叫醒 | 按指标配置检测方向与阈值，多算法投票 + 指纹去重 + 同服务告警收敛 |
| 根因靠人 | 看到 `mysql-replica-2` 延迟高，要翻 CMDB、变更记录、主机监控 | 知识图谱遍历依赖链 + 24h 变更关联 + 归一化贝叶斯打分，输出候选根因和证据 |
| 变更靠胆量 | 在线 DDL、主从切换、KILL 会话，改错一次就是事故 | 14 个 DBA Playbook + 6 因子风险评分 + L0/L1/L2 审批门禁 + 审批后才执行 |

核心原则只有一句：**没有审批单，不允许执行任何变更。**

---

## 2. 能力清单

- **监控告警**：6 个 MySQL 指标策略（主从延迟 / 连接数使用率 / 慢查询 QPS / CPU / 磁盘 / QPS，评测覆盖前 5 个），MAD 鲁棒检测 + EWMA + Isolation Forest 投票，冷启动样本不足时弃权
- **根因分析**：13 节点 17 条边的服务-数据库-主机拓扑，反向 BFS 找根因候选，变更时间窗关联，后验概率归一化，置信度不足自动转人工
- **自愈提案**：14 个 Playbook（L0 4 个 / L1 7 个 / L2 3 个），命令模板渲染、爆炸半径评估、dry-run 预检
- **变更审批**：6 因子风险模型、DDL 变更窗口检查、L2 自动生成 `CHG-` 工单、人工审批/拒绝、失败后自动抬高历史风险
- **执行安全**：审批状态二次校验、命令白名单与危险模式拦截、幂等检查、熔断器、失败自动回滚
- **审计**：哈希链追加写 `var/audit/audit-YYYYMMDD.jsonl`，支持整链校验，篡改/删行会被定位
- **工程化**：FastAPI 17 个接口、Prometheus 文本指标、108 个 pytest 用例、3 个可复现评测脚本

---

## 3. 架构

```
                    ┌─────────────────────────────────────────────┐
   Prometheus ─────▶│  MonitorAgent                               │
   MySQL 指标        │  方向/阈值策略 → MAD·EWMA·iForest 投票       │
                    │  → 分级 → 指纹去重 → 告警收敛                │
                    └──────────────────────┬──────────────────────┘
                                           │ AlertEvent → aiops.alerts
                    ┌──────────────────────▼──────────────────────┐
                    │  RCAAgent                                   │
                    │  知识图谱依赖链 + 24h 变更 + 归一化贝叶斯     │
                    │  置信度不足 → needs_human → 跳过自愈         │
                    └──────────────────────┬──────────────────────┘
                                           │ RCAEvent → aiops.events
                    ┌──────────────────────▼──────────────────────┐
                    │  HealAgent（只提案）                         │
                    │  Playbook 匹配 → 爆炸半径 → dry-run 预检     │
                    └──────────────────────┬──────────────────────┘
                                           │ HealEvent(proposed)
                    ┌──────────────────────▼──────────────────────┐
                    │  ChangeAgent                                │
                    │  6 因子风险分 → L0 自动 / L1 当班 / L2 工单   │
                    │  pending 时工作流在此中断                     │
                    └──────────────────────┬──────────────────────┘
                                           │ ChangeEvent(approved) → aiops.audit
                    ┌──────────────────────▼──────────────────────┐
                    │  HealAgent.execute（审批后执行）             │
                    │  熔断 → 白名单 → 幂等 → 执行 → 失败回滚       │
                    └──────────────────────┬──────────────────────┘
                                           ▼
                         InMemoryEventBus / KafkaEventBus
                         InMemoryKnowledgeGraph / Neo4j
                         AuditSink（哈希链 JSONL）
```

一次故障的真实链路（`python -m core.orchestrator` 输出）：

```
19:37:07  MonitorAgent  注入 120 个正常样本作为 mysql-replica-2 基线
19:37:08  MonitorAgent  mysql_replication_lag_sec=46.20秒 → critical（ratio=54.43）
19:37:08  RCAAgent      replica_ddl（置信度 0.50，影响面 0.40）
19:37:08  HealAgent     propose kill_ddl [L1] 爆炸半径 0.08
19:37:08  ChangeAgent   approved：当班 DBA 批准（risk=0.241 <= 0.55）
19:37:08  HealAgent     execute  mysql -h db-node-3 -e "KILL QUERY 1024871;"
19:37:08  HealAgent     succeeded  SUCCESS(simulated)
```

注意执行发生在一张 `approved` 变更单之后，而不是在提案时。

---

## 4. 快速开始

要求 Python 3.10+，不需要 Kafka / Neo4j / Docker。

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows
# source .venv/bin/activate       # macOS / Linux

pip install -r requirements.txt
```

如果直连 PyPI 很慢，可以换镜像：

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

跑完整 Demo：

```bash
python -m core.orchestrator
```

跑测试：

```bash
python -m pytest
# 108 passed
```

启动 API：

```bash
python -m uvicorn api.main:app --reload --port 8000
# http://localhost:8000/docs
```

跑评测：

```bash
python -m benchmarks.detect_eval     # 异常检测回放
python -m benchmarks.pipeline_eval   # 端到端流水线性能
python -m benchmarks.project_stats   # 代码/测试/图谱规模
```

---

## 5. API 一览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/incidents/trigger` | 触发一次故障处理，可传指标和证据 |
| GET | `/api/v1/incidents` | 故障列表 |
| GET | `/api/v1/incidents/{id}` | 详情（含 timeline 和节点状态） |
| POST | `/api/v1/incidents/{id}/approve` | 人工批准并继续执行 |
| POST | `/api/v1/incidents/{id}/reject` | 人工拒绝 |
| GET | `/api/v1/topology` | 知识图谱摘要 |
| GET | `/api/v1/topology/{service}/dependencies` | 依赖链、影响面、近期变更 |
| POST | `/api/v1/anomaly/detect` | 单点异常检测 |
| GET | `/api/v1/anomaly/demo` | 回放演示 |
| GET | `/api/v1/agents/status` | Agent 指标 + 工作流状态 |
| GET | `/api/v1/playbooks` | 14 个 Playbook |
| GET | `/api/v1/risk-model` | 风险权重与阈值 |
| GET | `/api/v1/audit/verify` | 审计链完整性 |
| GET | `/api/v1/audit/tail` | 最近审计记录 |
| GET | `/metrics` | Prometheus 文本格式 |

触发示例：

```bash
curl -X POST http://localhost:8000/api/v1/incidents/trigger \
  -H "Content-Type: application/json" \
  -d '{"metric_name":"mysql_connections_used_ratio","metric_value":0.95,
       "target_service":"mysql-primary","long_transactions":2}'
```

---

## 6. 关键设计（面试重点）

### 6.1 两阶段变更，而不是「先斩后奏」

`HealAgent` 拆成 `propose()` 和 `execute()`。`execute()` 的第一行就是校验：

```python
if change.approval_status != "approved":
    raise PermissionError("变更单未批准，拒绝执行")
```

编排器里 `execute` 节点只有在 `change_event.approval_status == "approved"` 时才被路由到；
pending 的变更单让工作流停在 `change` 节点，等人工审批后 `resume()` 只跑执行节点，不重跑监控和根因。

### 6.2 根因：图谱 + 变更 + 归一化贝叶斯

候选根因的权重是 `prior × likelihood`，然后**除以所有候选的权重和**，保证后验概率守恒：

```
posterior_i = prior_i · likelihood_i / Σ_j(prior_j · likelihood_j)
```

置信度低于 0.30 或与第二名差距小于 0.05 时标记 `needs_human`，直接跳过自愈。
这比「让模型硬选一个」更符合运维的实际情况：证据不足时，不动手才是正确动作。

### 6.3 检测：单向、鲁棒、可弃权

- 主从延迟、连接数这类指标只有上冲才危险，双向检测会把「延迟归零」当异常；
- 均值/标准差会被历史尖刺污染，MAD（中位数绝对偏差）更稳；
- 历史样本不足 20 个时，所有算法弃权，而不是拿 3 个点算出异常；
- 告警严重度优先看静态阈值（业务容忍度），统计 ratio 只负责把未越线但明显偏离的告警抬到 medium。

### 6.4 风险模型：6 个因子，权重和为 1

| 因子 | 权重 | 说明 |
|------|------|------|
| 爆炸半径 | 0.30 | 图谱影响面 / Playbook 预估 |
| 操作固有风险 | 0.25 | DDL、主从切换显著更高 |
| 变更窗口 | 0.15 | 需要窗口的 DDL 在业务时段强制挂起 |
| 实例关键度 | 0.10 | 主库最高 |
| 历史成功率 | 0.10 | 失败一次后用 EWMA 抬高后续风险 |
| 数据丢失风险 | 0.10 | 归档、切换类操作 |

L0 且 ≤ 0.30 自动放行；L1 且 ≤ 0.55 当班批准；L2 一律生成工单；需要窗口的 DDL 在业务时段直接挂起。

### 6.5 审计：可校验的哈希链

每条记录包含 `prev_hash` 与 `hash = SHA256(prev_hash + 规范化JSON(payload))`。
删掉中间一条、改一个字段，`/api/v1/audit/verify` 都会指出具体文件和行号。
这对应运维合规里的「谁、什么时候、对哪个实例、做了什么、依据是什么」。

### 6.6 执行护栏

```
熔断器 → 爆炸半径上限（超限强制 L2）→ 命令白名单 → 危险模式拦截
→ 审批门禁 → 幂等检查 → 执行 → 失败回滚
```

默认执行器是 `SimulatedExecutor`，只做模拟，不碰任何真实数据库。
真实执行需要显式设置 `AIOPS_ENABLE_REAL_EXECUTION=true`，且仍然受白名单约束。

---

## 7. 评测结果

数据来自 `benchmarks/*.py`，可复现；结果是合成数据集，不代表生产流量。

### 7.1 异常检测回放

5 个指标 × 600 个采样点，16 个注入故障窗口（含 1 个 CPU 骤降），逐点回放：

| 检测器 | 精确率 | 召回率 | F1 | 每千点误报 | 窗口检出率 | 首告警时延（点） |
|--------|--------|--------|----|-----------|-----------|----------------|
| 静态阈值（warn） | 1.000 | 0.695 | 0.820 | 0.0 | 94% | 2 |
| 静态阈值（crit） | 1.000 | 0.212 | 0.350 | 0.0 | 69% | 5 |
| 3-Sigma | 0.988 | 0.703 | 0.822 | 0.4 | 94% | 2 |
| MAD | 1.000 | 0.703 | 0.826 | 0.0 | 94% | 2 |
| **MAD + EWMA + iForest 投票** | **0.976** | **0.695** | **0.812** | **0.8** | **94%** | **0** |

结论：如果只按静态严重阈值报警，16 个窗口里只能检出 69%，且平均晚 5 个采样点；
动态检测把窗口检出率提到 94%，首告警时延中位数为 0，代价是每千点 0.8 次误报。
（warn 档静态阈值在这份合成数据上同样准确，是因为故障幅度大且阈值贴得很紧；
真实环境里 warn 档通常会带来大量噪音，这也是动态检测存在的意义。）

### 7.2 端到端流水线

30 次独立运行（每次都新建 bus + orchestrator）：

| 指标 | 结果 |
|------|------|
| 处理成功率 | 100% |
| 自愈执行成功率 | 100% |
| 延迟 p50 / p95 | 69.8 ms / 72.2 ms |
| 人工审批后恢复执行 | 6/6 成功 |

延迟主要是 Isolation Forest 在线拟合的开销，不含真实数据库执行时间。

---

## 8. 项目结构

```
.
├── agents/
│   ├── base_agent.py        # 模板方法、指标、异常语义
│   ├── monitor_agent.py     # 检测、分级、去重、收敛
│   ├── rca_agent.py         # 图谱根因、证据、归一化贝叶斯
│   ├── heal_agent.py        # Playbook、预检、审批后执行、熔断
│   └── change_agent.py      # 风险模型、审批策略、审计
├── core/
│   ├── orchestrator.py      # 5 节点状态机、重试、检查点、resume
│   ├── event_bus.py         # 内存/Kafka 双模
│   ├── knowledge_graph.py   # 图算法、Neo4j 适配
│   └── audit.py             # 哈希链审计
├── models/
│   ├── events.py            # 事件与状态模型
│   └── time_series.py       # 检测算法、指标策略、数据生成
├── api/main.py              # FastAPI 接口与指标
├── config/settings.py       # 环境变量配置
├── tests/                   # 108 个用例
├── benchmarks/              # 3 个评测脚本 + 结果 JSON
└── docs/
    ├── architecture.md
    ├── engineering-notes.md # 从初版到现在的 15 个工程问题
    └── interview/           # 简历、STAR、八股、Q&A
```

---

## 9. 初版的问题与修复

初版是「能跑起来的演示」：README 声称有 Java/Go 两套实现和 LangGraph/Prophet/ChromaDB，
实际只有一套 Python，依赖装了但没用；`dry_run=True` 时反而执行修复，审批在执行之后；
编排器的重试计数跨故障累积；RCA 拿着知识图谱参数却用另一份硬编码拓扑。

这轮改造的完整清单（现象 / 根因 / 修复 / 验证方式）在
[docs/engineering-notes.md](docs/engineering-notes.md)。

---

## 10. 边界与后续

明确没做的事，避免面试时被问穿：

- **不是 LLM Agent**：根因是规则 + 贝叶斯，没有调用大模型。图谱候选和证据链已经结构化，接入 LLM 做摘要/排序是下一步，而不是现状。
- **执行是模拟的**：`SimulatedExecutor` 不会连 MySQL；真实执行器默认关闭。
- **Kafka / Neo4j 只做了适配层**：本机没有 broker，未做联调；默认内存实现保证开箱即用。
- **状态在进程内**：检查点没有落盘，重启后丢失；审计已落盘。
- **单进程**：没有分布式锁和多副本协调，不能直接当生产控制面。
- **评测是合成数据**：没有生产流量回放，数字只能说明方法有效，不能说明线上收益。

后续优先级：检查点持久化 → 接真实 Prometheus/MySQL 只读账号 → LLM 辅助根因排序 → Kafka 多副本。

---

## 11. 面试材料

- [简历与面试口径写法](docs/interview/resume-template.md)
- [STAR 话术与追问应对](docs/interview/star-method.md)
- [工程问题复盘](docs/engineering-notes.md)
- [架构说明](docs/architecture.md)

---

## 12. 来源与致谢

本仓库是 [bcefghj/multi-agent-aiops](https://github.com/bcefghj/multi-agent-aiops) 的 GitHub fork，
不是从零实现的原创仓库：

- **原项目**：[bcefghj/multi-agent-aiops](https://github.com/bcefghj/multi-agent-aiops)
  （作者 `bcefghj`，MIT 许可，原仓库声明可自由用于学习、修改和面试展示）
- **仓库关系**：GitHub fork（forked from `bcefghj/multi-agent-aiops`）
- **本仓库范围**：只基于原项目的 Python 实现，未使用其 Java / Go 实现
- **原项目版权**：归 `bcefghj` 所有；本仓库的修改部分归倪建伟
- **改动说明**：重构为面向 MySQL 的 5 节点两阶段审批流程，接入知识图谱，
  重写检测策略、14 个 DBA Playbook、6 因子风险模型、哈希链审计，
  补充 108 个测试、3 个评测脚本，并修正原项目中审批/执行时序等 15 个问题。
  详见 [docs/engineering-notes.md](docs/engineering-notes.md)。
- **声明**：本仓库不是原项目的官方版本，也不代表原作者的观点。

## License

MIT，见 [LICENSE](LICENSE)。原作者的版权声明与本仓库修改部分的声明均保留在该文件中。
