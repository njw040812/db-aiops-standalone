"""知识图谱：服务 / 数据库 / 主机 的依赖拓扑 + 图算法。

节点类型
    Service / Gateway / Proxy / Database / Cache / Host

关系类型
    DEPENDS_ON      服务依赖（order-service → mysql-proxy）
    REPLICATES_FROM 主从复制（mysql-replica-1 → mysql-primary）
    RUNS_ON         部署位置（mysql-primary → db-node-1）

RCA 用到的四个算法都在这里实现，Agent 不再自己维护一份假拓扑：
1. `trace_dependencies()`  从故障服务沿依赖边做 BFS，拿到整条候选链路；
2. `find_root_candidates()` 取链路中的「叶子」节点作为根因候选；
3. `find_recent_changes()`  在时间窗内找变更，把「变更相关性」变成证据；
4. `compute_impact_score()` 反向可达服务数 / 服务总数，量化影响面（爆炸半径）。

Neo4j 实现只用于生产：Cypher 不支持把可变长路径的长度写成参数，
必须把 `max_depth` 转成整数拼进查询串，这是原实现会直接报错的地方。
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

DEP_REL_TYPES = ("DEPENDS_ON", "REPLICATES_FROM")
SERVICE_TYPES = ("service", "gateway")


def _as_aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class ServiceNode:
    def __init__(
        self,
        name: str,
        node_type: str = "service",
        namespace: str = "production",
        properties: Optional[dict] = None,
    ):
        self.name = name
        self.node_type = node_type
        self.namespace = namespace
        self.properties = properties or {}


class Relationship:
    def __init__(self, source: str, target: str, rel_type: str, properties: Optional[dict] = None):
        self.source = source
        self.target = target
        self.rel_type = rel_type
        self.properties = properties or {}


class InMemoryKnowledgeGraph:
    """不依赖 Neo4j 的内存图，接口与 Neo4jKnowledgeGraph 对齐。"""

    def __init__(self, service_types: Iterable[str] = SERVICE_TYPES):
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: list[Relationship] = []
        self._out: dict[str, list[tuple[str, str]]] = defaultdict(list)
        self._in: dict[str, list[tuple[str, str]]] = defaultdict(list)
        self._service_types = tuple(service_types)
        self._impact_cache: dict[str, float] = {}

    # ------------------------------------------------------------------ build
    def add_node(
        self,
        name: str,
        node_type: str = "service",
        properties: Optional[dict] = None,
        **extra: Any,
    ) -> None:
        props = dict(properties or {})
        props.update(extra)
        props.setdefault("recent_changes", [])
        self._nodes[name] = {
            "name": name,
            "type": node_type,
            "properties": props,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._impact_cache.clear()

    def add_relationship(
        self,
        source: str,
        target: str,
        rel_type: str = "DEPENDS_ON",
        properties: Optional[dict] = None,
    ) -> None:
        if source not in self._nodes:
            self.add_node(source, "service")
        if target not in self._nodes:
            self.add_node(target, "service")
        edge = Relationship(source, target, rel_type, properties or {})
        self._edges.append(edge)
        self._out[source].append((target, rel_type))
        self._in[target].append((source, rel_type))
        self._impact_cache.clear()

    def add_change(
        self,
        service: str,
        change_type: str,
        description: str,
        at: Optional[datetime] = None,
        risk: float = 0.5,
    ) -> dict[str, Any]:
        node = self._nodes.setdefault(
            service,
            {"name": service, "type": "service", "properties": {"recent_changes": []}},
        )
        change = {
            "type": change_type,
            "description": description,
            "at": _as_aware(at or datetime.now(timezone.utc)).isoformat(),
            "risk": risk,
        }
        node["properties"].setdefault("recent_changes", []).append(change)
        return change

    # ------------------------------------------------------------------ query
    def get_node(self, name: str) -> Optional[dict[str, Any]]:
        return self._nodes.get(name)

    def nodes(self) -> list[str]:
        return list(self._nodes)

    def node_type(self, name: str) -> str:
        return self._nodes.get(name, {}).get("type", "unknown")

    def get_dependencies(self, service: str) -> list[str]:
        return [t for t, rel in self._out.get(service, []) if rel in DEP_REL_TYPES]

    def get_dependents(self, service: str) -> list[str]:
        return [s for s, rel in self._in.get(service, []) if rel in DEP_REL_TYPES]

    def trace_dependencies(self, start: str, max_depth: int = 5) -> list[str]:
        """从 start 沿依赖边 BFS，返回去重后的节点顺序（不含 start）。"""
        seen = {start}
        order: list[str] = []
        queue: deque[tuple[str, int]] = deque([(start, 0)])
        while queue:
            current, depth = queue.popleft()
            if depth >= max_depth:
                continue
            for dep in self.get_dependencies(current):
                if dep in seen:
                    continue
                seen.add(dep)
                order.append(dep)
                queue.append((dep, depth + 1))
        return order

    def find_root_candidates(self, start: str, max_depth: int = 5) -> list[dict[str, Any]]:
        """叶子依赖（没有任何下游依赖）作为根因候选，带最短距离。"""
        distances = {start: 0}
        queue: deque[str] = deque([start])
        candidates: list[dict[str, Any]] = []
        while queue:
            current = queue.popleft()
            depth = distances[current]
            deps = self.get_dependencies(current)
            if not deps and current != start:
                candidates.append(
                    {
                        "service": current,
                        "distance": depth,
                        "type": self.node_type(current),
                        "impact_score": self.compute_impact_score(current),
                    }
                )
                continue
            if depth >= max_depth:
                continue
            for dep in deps:
                if dep not in distances:
                    distances[dep] = depth + 1
                    queue.append(dep)
        candidates.sort(key=lambda c: (c["distance"], -c["impact_score"]))
        return candidates

    def paths_to(self, start: str, target: str, max_depth: int = 5) -> list[list[str]]:
        """找出 start → target 的所有简单路径（用于解释故障传播链）。"""
        results: list[list[str]] = []
        stack: list[tuple[str, list[str]]] = [(start, [start])]
        while stack:
            current, path = stack.pop()
            if current == target and len(path) > 1:
                results.append(path)
                continue
            if len(path) > max_depth:
                continue
            for dep in self.get_dependencies(current):
                if dep in path:
                    continue
                stack.append((dep, path + [dep]))
        return results

    def find_recent_changes(
        self,
        services: Iterable[str],
        within_hours: int = 24,
        now: Optional[datetime] = None,
    ) -> list[dict[str, Any]]:
        now_aware = _as_aware(now or datetime.now(timezone.utc))
        threshold = now_aware - timedelta(hours=within_hours)
        changes: list[dict[str, Any]] = []
        for service in services:
            node = self._nodes.get(service, {})
            for change in node.get("properties", {}).get("recent_changes", []):
                try:
                    at = _as_aware(datetime.fromisoformat(change["at"]))
                except (KeyError, TypeError, ValueError):
                    continue
                if at >= threshold:
                    changes.append({"service": service, **change, "age_hours": round((now_aware - at).total_seconds() / 3600, 2)})
        changes.sort(key=lambda c: c["age_hours"])
        return changes

    def compute_impact_score(self, service: str) -> float:
        """影响面 = 反向可达的服务节点数 / 服务节点总数。

        比「直接上游个数 / 总节点数」更接近真实爆炸半径：一个数据库挂了，
        它的代理、依赖它的服务、以及服务的上游网关都会被波及。
        """
        if service in self._impact_cache:
            return self._impact_cache[service]

        services = [n for n, data in self._nodes.items() if data["type"] in self._service_types]
        if not services:
            return 0.0

        seen = {service}
        queue = deque([service])
        while queue:
            current = queue.popleft()
            for dependent in self.get_dependents(current):
                if dependent not in seen:
                    seen.add(dependent)
                    queue.append(dependent)

        affected = sum(1 for n in seen if n in services and n != service)
        score = round(affected / max(len(services), 1), 4)
        self._impact_cache[service] = score
        return score

    def find_shared_hosts(self, service_a: str, service_b: str) -> list[str]:
        """两个服务是否共置在同一台主机（资源抢占是常见的隐性根因）。"""
        hosts_a = {t for t, rel in self._out.get(service_a, []) if rel == "RUNS_ON"}
        hosts_b = {t for t, rel in self._out.get(service_b, []) if rel == "RUNS_ON"}
        return sorted(hosts_a & hosts_b)

    def summary(self) -> dict[str, Any]:
        node_types: dict[str, int] = defaultdict(int)
        for data in self._nodes.values():
            node_types[data["type"]] += 1
        rel_types: dict[str, int] = defaultdict(int)
        for edge in self._edges:
            rel_types[edge.rel_type] += 1
        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "node_types": dict(node_types),
            "relationship_types": dict(rel_types),
            "nodes": list(self._nodes),
        }


class Neo4jKnowledgeGraph:
    """生产环境实现（可选依赖 neo4j）。"""

    def __init__(self, uri: str, user: str, password: str):
        from neo4j import GraphDatabase

        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self._driver.close()

    def init_schema(self) -> None:
        with self._driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Database) REQUIRE d.name IS UNIQUE")
            session.run("CREATE INDEX IF NOT EXISTS FOR (a:Alert) ON (a.timestamp)")

    def add_service(self, name: str, node_type: str = "Service", properties: Optional[dict] = None) -> None:
        props = properties or {}
        with self._driver.session() as session:
            session.run(
                f"MERGE (s:{self._label(node_type)} {{name: $name}}) SET s += $props",
                name=name,
                props=props,
            )

    def add_dependency(self, source: str, target: str, rel_type: str = "DEPENDS_ON") -> None:
        rel = self._rel(rel_type)
        with self._driver.session() as session:
            session.run(
                f"""
                MATCH (s {{name: $source}})
                MATCH (t {{name: $target}})
                MERGE (s)-[:{rel}]->(t)
                """,
                source=source,
                target=target,
            )

    def find_root_causes(self, service: str, max_depth: int = 5) -> list[dict]:
        # 可变长路径的长度不能参数化，必须先转成 int 再拼接
        depth = max(1, min(int(max_depth), 10))
        with self._driver.session() as session:
            result = session.run(
                f"""
                MATCH path = (s {{name: $service}})-[:DEPENDS_ON|REPLICATES_FROM*1..{depth}]->(root)
                WHERE NOT (root)-[:DEPENDS_ON|REPLICATES_FROM]->()
                RETURN root.name AS root_cause,
                       length(path) AS distance,
                       [n IN nodes(path) | n.name] AS path
                ORDER BY distance ASC
                LIMIT 20
                """,
                service=service,
            )
            return [dict(record) for record in result]

    def find_recent_changes(self, service: str, hours: int = 24) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(
                """
                MATCH (s {name: $service})-[:DEPENDS_ON|REPLICATES_FROM*0..3]->(dep)
                MATCH (c:Change)-[:AFFECTS]->(dep)
                WHERE c.timestamp >= datetime() - duration({hours: $hours})
                RETURN c.description AS change, dep.name AS service, c.timestamp AS time
                ORDER BY c.timestamp DESC
                """,
                service=service,
                hours=hours,
            )
            return [dict(record) for record in result]

    @staticmethod
    def _label(node_type: str) -> str:
        allowed = {"Service", "Gateway", "Proxy", "Database", "Cache", "Host"}
        label = node_type.capitalize()
        if label not in allowed:
            raise ValueError(f"unsupported node label: {node_type}")
        return label

    @staticmethod
    def _rel(rel_type: str) -> str:
        allowed = {"DEPENDS_ON", "REPLICATES_FROM", "RUNS_ON", "AFFECTS", "MONITORS"}
        if rel_type not in allowed:
            raise ValueError(f"unsupported relationship: {rel_type}")
        return rel_type


def create_demo_knowledge_graph(now: Optional[datetime] = None) -> InMemoryKnowledgeGraph:
    """演示用数据库拓扑：MySQL 一主两从 + 缓存 + 一个代理层。

    变更记录用相对时间生成，保证任何一天运行 demo 都能命中「24h 内有变更」。
    """
    now = _as_aware(now or datetime.now(timezone.utc))
    kg = InMemoryKnowledgeGraph()

    nodes = {
        "api-gateway": ("gateway", {"tier": "入口"}),
        "order-service": ("service", {"tier": "交易", "owner": "交易组"}),
        "payment-service": ("service", {"tier": "支付", "owner": "支付组"}),
        "user-service": ("service", {"tier": "用户", "owner": "用户组"}),
        "report-service": ("service", {"tier": "报表", "owner": "数据组"}),
        "mysql-proxy": ("proxy", {"tier": "数据库接入层", "engine": "ProxySQL"}),
        "mysql-primary": ("database", {"engine": "MySQL 8.0", "role": "primary", "version": "8.0.36"}),
        "mysql-replica-1": ("database", {"engine": "MySQL 8.0", "role": "replica", "version": "8.0.36"}),
        "mysql-replica-2": ("database", {"engine": "MySQL 8.0", "role": "replica", "version": "8.0.36"}),
        "redis-cache": ("cache", {"engine": "Redis 7.2"}),
        "db-node-1": ("host", {"cpu_cores": 32, "role": "生产数据库主机"}),
        "db-node-2": ("host", {"cpu_cores": 32, "role": "生产数据库主机"}),
        "db-node-3": ("host", {"cpu_cores": 16, "role": "只读副本主机"}),
    }
    for name, (node_type, props) in nodes.items():
        kg.add_node(name, node_type, props)

    dependencies = [
        ("api-gateway", "order-service", "DEPENDS_ON"),
        ("api-gateway", "user-service", "DEPENDS_ON"),
        ("api-gateway", "report-service", "DEPENDS_ON"),
        ("order-service", "payment-service", "DEPENDS_ON"),
        ("order-service", "user-service", "DEPENDS_ON"),
        ("order-service", "mysql-proxy", "DEPENDS_ON"),
        ("payment-service", "mysql-primary", "DEPENDS_ON"),
        ("payment-service", "redis-cache", "DEPENDS_ON"),
        ("user-service", "mysql-replica-1", "DEPENDS_ON"),
        ("user-service", "redis-cache", "DEPENDS_ON"),
        ("report-service", "mysql-replica-2", "DEPENDS_ON"),
        ("mysql-proxy", "mysql-primary", "DEPENDS_ON"),
        ("mysql-replica-1", "mysql-primary", "REPLICATES_FROM"),
        ("mysql-replica-2", "mysql-primary", "REPLICATES_FROM"),
        ("mysql-primary", "db-node-1", "RUNS_ON"),
        ("mysql-replica-1", "db-node-2", "RUNS_ON"),
        ("mysql-replica-2", "db-node-3", "RUNS_ON"),
    ]
    for source, target, rel_type in dependencies:
        kg.add_relationship(source, target, rel_type)

    kg.add_change(
        "order-service",
        "deploy",
        "order-service 发布 v2.3.1（新增订单查询缓存逻辑）",
        at=now - timedelta(hours=2),
        risk=0.6,
    )
    kg.add_change(
        "mysql-primary",
        "config",
        "max_connections 800 → 1200，未同步调整应用侧连接池",
        at=now - timedelta(hours=5),
        risk=0.7,
    )
    kg.add_change(
        "mysql-replica-2",
        "ddl",
        "报表库新增联合索引 idx_report_created_status（gh-ost 执行中）",
        at=now - timedelta(hours=9),
        risk=0.8,
    )

    return kg
