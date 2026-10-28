# 知识图谱与根因算法

## 图数据模型

演示拓扑面向 MySQL 一主两从：

| 节点类型 | 示例 | 关键属性 |
|---------|------|---------|
| Service | order-service | tier、owner |
| Gateway | api-gateway | tier |
| Proxy | mysql-proxy | engine=ProxySQL |
| Database | mysql-primary / mysql-replica-1 | role、version |
| Cache | redis-cache | engine |
| Host | db-node-1 | cpu_cores、role |

| 关系 | 含义 | 示例 |
|------|------|------|
| DEPENDS_ON | 服务依赖 | order-service → mysql-proxy |
| REPLICATES_FROM | 主从复制 | mysql-replica-2 → mysql-primary |
| RUNS_ON | 部署位置 | mysql-primary → db-node-1 |

节点上还挂 `recent_changes`（类型、描述、时间、风险），根因分析会查 24h 内的变更。

## 四个图算法

### 1. 依赖闭包（BFS）

从故障对象沿 `DEPENDS_ON` / `REPLICATES_FROM` 做 BFS，得到整条候选链路。
时间复杂度 O(V+E)，需要 visited 集合防环。

### 2. 根因候选（叶子依赖）

```python
def find_root_candidates(self, start, max_depth=5):
    distances = {start: 0}
    queue = deque([start])
    candidates = []
    while queue:
        current = queue.popleft()
        depth = distances[current]
        deps = self.get_dependencies(current)
        if not deps and current != start:
            candidates.append({"service": current, "distance": depth, ...})
            continue
        if depth >= max_depth:
            continue
        for dep in deps:
            if dep not in distances:
                distances[dep] = depth + 1
                queue.append(dep)
    return candidates
```

### 3. 影响面

从目标节点沿反向依赖边 BFS，统计可达的服务节点数 / 服务节点总数。
演示图谱里：主库 1.0、副本 0.4、报表服务 0.2。

### 4. 变更时间窗

按 `at >= now - lookback` 过滤节点上的变更，返回带 `age_hours` 的列表。

## 贝叶斯打分

```
weight_i   = prior_i × likelihood_i
posterior_i = weight_i / Σ weight
```

`prior` 来自故障目录（可离线统计历史工单后回填），`likelihood` 由证据命中决定。
归一化后再判断「置信度是否 >= 0.30」「是否领先第二名 0.05」。

## Neo4j 查询

```cypher
MATCH path = (s {name: $service})-[:DEPENDS_ON|REPLICATES_FROM*1..5]->(root)
WHERE NOT (root)-[:DEPENDS_ON|REPLICATES_FROM]->()
RETURN root.name AS root_cause, length(path) AS distance
ORDER BY distance ASC
```

注意：`*1..5` 的 5 不能写成参数（`*1..$depth` 会语法错误），
必须先 `int()` 并 clamp，再拼进查询串；标签和关系类型也要走白名单。

## 面试问答

**Q：为什么用图而不是关系型数据库？**
A：多跳依赖查询在图上是指针跳转，SQL 需要多次 JOIN；但小规模拓扑用内存图也够，
本项目默认内存实现，Neo4j 是可选项。

**Q：根因为什么取叶子节点？**
A：叶子没有下游依赖，通常是故障源；如果所有节点都有下游，说明拓扑有环或数据缺失，
需要人工介入。

**Q：图谱数据从哪来？**
A：CMDB（服务/主机/主从关系）、变更系统（DDL/发布）、Prometheus（指标）。
本项目用演示数据构造，生产应做增量同步。

**Q：图谱怎么更新？**
A：变更事件触发增量更新；本项目的 `add_change` 就是最小实现。
