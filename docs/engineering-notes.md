# 工程问题复盘：从「能跑」到「敢用」

这份记录是面试时最有用的材料。初版代码能跑通 Demo，但多处逻辑是反的。
下面每条都给出：现象 → 根因 → 修复 → 验证方式。

---

## 总览

| # | 问题 | 严重度 | 修复位置 |
|---|------|--------|---------|
| 1 | `dry_run=True` 反而执行修复 | 高 | `agents/heal_agent.py` |
| 2 | 执行发生在审批之前 | 高 | `core/orchestrator.py` |
| 3 | 重试计数跨故障累积 | 高 | `core/orchestrator.py` |
| 4 | 节点失败后继续执行后续节点 | 高 | `core/orchestrator.py` |
| 5 | Agent 吞异常，重试形同虚设 | 高 | `agents/base_agent.py` |
| 6 | RCA 拿着图谱参数却用硬编码拓扑 | 中 | `agents/rca_agent.py` |
| 7 | 贝叶斯后验概率不归一、被截断 | 中 | `agents/rca_agent.py` |
| 8 | Neo4j 可变长路径参数化报错 | 中 | `core/knowledge_graph.py` |
| 9 | 配置模块从未被引用 | 中 | 全局 |
| 10 | 依赖声明与实际使用不符 | 中 | `requirements.txt` |
| 11 | README 宣称 Java/Go 实现 | 高（可信度） | `README.md` |
| 12 | 零测试 | 高 | `tests/` |
| 13 | numpy.bool 导致事件发布失败 | 中 | `models/time_series.py` |
| 14 | 审计只在内存，无防篡改 | 中 | `core/audit.py` |
| 15 | 告警分级与静态阈值脱节 | 低 | `models/time_series.py` |

---

## 1. `dry_run=True` 反而执行

**现象**：默认配置下 Demo 会打印「SUCCESS: 已执行」，把 `dry_run` 改成 `False` 反而不执行。

**原代码**：

```python
if playbook_level == HealLevel.L0_AUTO and self._dry_run:
    execution_result = await self._execute_remediation(playbook, target_service)
```

**根因**：把 `dry_run` 当成了「Demo 模式」，语义完全反了。

**修复**：拆成 `propose()` 与 `execute()`；`dry_run` 只控制 `preflight()`，
执行由 `execute()` 负责，且必须存在 approved 变更单。

**验证**：`tests/test_heal_agent.py::test_proposal_only_does_not_execute` 断言执行器一次都没被调用。

---

## 2. 执行早于审批

**现象**：L0 动作在 Heal 节点内直接执行，Change 节点事后才补一张审批单；
L1 动作审批通过后没有任何代码去执行它，但状态却被改成 `resolved`。

**根因**：工作流是 `Monitor → RCA → Heal → Change`，把「提案」和「执行」放在同一个节点，
审批只能做事后追认；而 L1 的「执行」压根不存在。

**修复**：

- 工作流变成 5 个节点：`monitor → rca → heal(提案) → change(审批) → execute(执行)`；
- `execute` 节点的路由条件是 `change.approval_status == "approved"`；
- `HealAgent.execute()` 第一行再次校验审批状态，防止绕过编排器直接调用。

**验证**：

- `test_execute_without_approval_is_rejected`
- `test_pending_approval_stops_before_execute`
- `test_resume_approve_executes_only_execute_node`

---

## 3. 重试计数跨故障累积

**现象**：同一个 Orchestrator 处理第二个故障时，节点一上来就处于「重试耗尽」状态。

**根因**：`retry_count` 和 `status` 挂在共享的 `WorkflowNode` 对象上，
而 Agent 是复用的单例。第一次故障重试 3 次后，第二次从 4 开始。

**修复**：节点只保留不可变的 `NodeSpec`（handler、condition、max_retries），
每次运行生成独立的 `NodeRun` 记录，按 `incident_id` 隔离。

**验证**：`test_second_run_does_not_inherit_retry_counts` 断言第二次运行 attempts=1。

---

## 4. 节点失败后继续执行

**现象**：RCA 抛异常后，工作流仍然进入自愈节点，用空的或过期的根因生成修复命令。

**根因**：`for node in nodes` 循环没有 break，失败只记了一个状态。

**修复**：节点重试耗尽后立即终止，状态置 `failed`，并写一条 halt 时间线。

**验证**：`test_failure_halts_workflow` 断言后续节点一次都没被调用。

---

## 5. Agent 吞异常，重试形同虚设

**现象**：编排器的 `while retry_count <= max_retries` 永远只跑一次。

**原代码**：

```python
except Exception as e:
    state.error_message = f"[{self.name}] Error: {e}"
    return state
```

**根因**：异常在 `BaseAgent.handle()` 里被吞掉，编排器看到的是一次「成功」；
重试条件依赖 `state.error_message`，但成功路径下它是空的。

**修复**：包装成 `AgentExecutionError` 向上抛；编排器负责重试与终止。
「业务上不满足条件」不抛异常，正常返回 state。

**验证**：`test_retry_then_success` 断言 handler 被调用 3 次。

---

## 6. RCA 拿着图谱参数却用硬编码拓扑

**现象**：`RCAAgent.__init__(knowledge_graph=...)` 接收图谱，但 `process()` 里从未使用，
根因用的是 `MOCK_SERVICE_TOPOLOGY` 这个第二份「假图」。

**根因**：Demo 先写了一个字典，后来加了图谱模块，没有回接。

**修复**：删除 MOCK 字典；`_collect_evidence` 通过 `find_root_candidates / find_shared_hosts /
find_recent_changes` 取证据；新增 `trace_dependencies` 与 `compute_impact_score`。

**验证**：`tests/test_rca_agent.py` 中影响链、影响面、共置主机证据均来自图谱；
`test_works_without_knowledge_graph` 覆盖降级路径。

---

## 7. 贝叶斯后验概率不归一

**原代码**：

```python
posterior = (likelihood * prior) / 0.5
return min(posterior, 1.0)
```

**问题**：

- 除以固定 0.5 没有概率意义；
- 超过 1 直接截断，多个候选的后验相加不为 1；
- 没有「与第二名差距」的判断，容易在模棱两可时硬选一个。

**修复**：

```
posterior_i = prior_i × likelihood_i / Σ_j(prior_j × likelihood_j)
```

并增加 `confidence < 0.30` 或 `margin < 0.05` → `needs_human`。

**验证**：`test_posteriors_are_normalized` 断言和为 1；`test_low_confidence_escalates`。

---

## 8. Neo4j 可变长路径参数化

**原代码**：

```cypher
MATCH path = (s:Service {name: $service})-[:DEPENDS_ON*1..$max_depth]->(root)
```

**问题**：Cypher 不允许把可变长路径的跳数写成参数，生产环境会直接报语法错误。

**修复**：`max_depth` 先 `int()` 并 clamp 到 1–10，再拼进查询串；
节点标签和关系类型走白名单，避免拼接注入。

**验证**：`test_neo4j_depth_is_validated`。

---

## 9. 配置模块从未被引用

**现象**：`config/settings.py` 定义了 30 多个配置项，全仓库没有任何 import；
`ChangeAgent` 的自动审批阈值硬编码 0.3，而配置里写的是 0.9。

**修复**：Agent 构造函数以 `settings` 作为默认值，环境变量前缀 `AIOPS_` 可覆盖；
测试通过显式参数注入，不依赖全局状态。

---

## 10. 依赖声明与实际使用不符

**现象**：`requirements.txt` 装了 LangGraph、LangChain、Prophet、ChromaDB、prometheus-client、
structlog、tenacity，代码里一行都没用；安装体积大、失败点多。

**修复**：只保留实际使用的依赖；Kafka/Neo4j 拆到 `requirements-optional.txt`。
Prometheus 指标改为手写文本 exposition，不再引入客户端库。

---

## 11. README 宣称三语言实现

**现象**：README 和 `docs/project-plan.md` 写了 `python/`、`java/`、`golang/` 三套实现，
仓库里实际只有根目录一套 Python；还宣称用了 LangGraph / Prophet / ChromaDB。

**影响**：这是简历项目最危险的问题——面试官打开仓库一看，可信度归零。

**修复**：README 重写为与代码一致的单语言实现；删除所有未使用技术的描述；
新增「边界与后续」章节，明确哪些是扩展点。

---

## 12. 零测试

**现象**：`tests/` 只有一个空的 `__init__.py`，但 requirements 里有 pytest。

**修复**：补 108 个用例，覆盖：

- 图谱遍历/影响面/环检测
- 检测算法冷启动/方向/投票
- 告警去重/收敛/严重度
- 根因归一化/证据不足
- 审批门禁/幂等/熔断/回滚/爆炸半径
- 编排器重试隔离/失败终止/审批恢复
- 审计链篡改检测
- API 端到端

**验证**：`python -m pytest` → `108 passed`。

---

## 13. numpy.bool 导致事件发布失败

**现象**：告警已经生成，但发布时抛 `Unable to serialize unknown type: <class 'numpy.bool'>`，
编排器重试，第二次被去重窗口抑制，最终状态错乱。

**根因**：检测器的 `score > threshold` 返回 `numpy.bool_`，被塞进 `AlertEvent.algorithm_votes`
的 `dict[str, Any]`，Pydantic 的 JSON 序列化不认识。

**修复**：检测结果在构造时统一 `bool()` / `float()` 转换；`to_dict()` 再做一次兜底。

**验证**：`test_anomaly_result_to_dict_is_json_safe`；Demo 不再出现重试。

---

## 14. 审计只在内存，且可篡改

**现象**：`AuditLogger` 只是一个 list，进程重启即丢；没有防篡改能力。

**修复**：`AuditSink` 订阅 `aiops.audit`，按天写 `audit-YYYYMMDD.jsonl`，
每条记录带 `prev_hash` 和 `hash`；`verify_dir()` 跨文件校验并定位第一处不匹配。

**验证**：`tests/test_audit.py` 覆盖改字段、删中间行、重启续链三种场景。

---

## 15. 告警分级与静态阈值脱节

**现象**：连接数使用率 0.70（低于 0.75 的 warn）会因为统计分数高被判为 high。

**修复**：严重度优先看静态阈值（业务容忍度），统计 ratio 只负责把未越线但明显偏离的告警抬到 medium。

**验证**：`test_connection_ratio_severity` 与 `test_metric_policy_severity`。

---

## 从这轮改造里能讲什么

1. **先问「这个自动化有没有权力动手」**：两阶段变更、审批门禁、幂等、回滚，是自愈系统的底线。
2. **失败要能停**：节点失败即终止，比「尽力继续」安全得多。
3. **概率要守恒**：不归一的置信度无法解释，也无法设阈值。
4. **文档即契约**：README 写了不存在的实现，比没有 README 更糟。
5. **数字要能复现**：所有指标都有 `benchmarks/` 脚本，面试时可以直接跑给面试官看。
