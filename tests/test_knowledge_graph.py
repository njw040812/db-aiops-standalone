"""知识图谱：拓扑、遍历、影响面、变更时间窗。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.knowledge_graph import InMemoryKnowledgeGraph, create_demo_knowledge_graph


def test_graph_summary(kg):
    summary = kg.summary()
    assert summary["total_nodes"] == 13
    assert summary["total_edges"] == 17
    assert summary["node_types"]["database"] == 3
    assert summary["relationship_types"]["REPLICATES_FROM"] == 2


def test_dependency_trace(kg):
    assert kg.get_dependencies("mysql-replica-2") == ["mysql-primary"]
    assert kg.trace_dependencies("mysql-replica-2") == ["mysql-primary"]
    assert set(kg.trace_dependencies("order-service")) >= {"mysql-proxy", "payment-service", "mysql-primary"}


def test_root_candidates_are_leaves(kg):
    candidates = kg.find_root_candidates("report-service")
    services = [item["service"] for item in candidates]
    assert "mysql-primary" in services
    assert all(item["distance"] >= 1 for item in candidates)
    assert kg.find_root_candidates("mysql-primary") == []


def test_impact_score_counts_transitive_dependents(kg):
    # mysql-primary 被所有 5 个服务直接或间接依赖
    assert kg.compute_impact_score("mysql-primary") == 1.0
    # report-service 只影响它的调用方 api-gateway
    assert kg.compute_impact_score("report-service") == 0.2
    assert kg.compute_impact_score("mysql-replica-2") == 0.4


def test_paths_to_primary(kg):
    paths = kg.paths_to("report-service", "mysql-primary")
    assert ["report-service", "mysql-replica-2", "mysql-primary"] in paths


def test_recent_changes_window(kg):
    now = datetime.now(timezone.utc)
    recent = kg.find_recent_changes(["order-service", "mysql-primary"], within_hours=24, now=now)
    assert {item["service"] for item in recent} == {"order-service", "mysql-primary"}

    kg.add_change("user-service", "config", "48 小时前的旧变更", at=now - timedelta(hours=48))
    recent = kg.find_recent_changes(["user-service"], within_hours=24, now=now)
    assert recent == []


def test_shared_host_detection():
    graph = InMemoryKnowledgeGraph()
    graph.add_node("a", "service")
    graph.add_node("b", "database")
    graph.add_node("host-1", "host")
    graph.add_relationship("a", "host-1", "RUNS_ON")
    graph.add_relationship("b", "host-1", "RUNS_ON")
    assert graph.find_shared_hosts("a", "b") == ["host-1"]
    assert graph.find_shared_hosts("a", "nonexistent") == []


def test_cycle_does_not_loop_forever():
    graph = InMemoryKnowledgeGraph()
    graph.add_node("a", "service")
    graph.add_node("b", "service")
    graph.add_relationship("a", "b")
    graph.add_relationship("b", "a")
    assert graph.trace_dependencies("a") == ["b"]
    assert graph.find_root_candidates("a") == []


def test_impact_cache_invalidated_on_new_edge():
    graph = InMemoryKnowledgeGraph()
    graph.add_node("db", "database")
    graph.add_node("svc", "service")
    assert graph.compute_impact_score("db") == 0.0
    graph.add_relationship("svc", "db")
    assert graph.compute_impact_score("db") == 1.0


def test_neo4j_depth_is_validated():
    from core.knowledge_graph import Neo4jKnowledgeGraph

    assert Neo4jKnowledgeGraph._label("database") == "Database"
    try:
        Neo4jKnowledgeGraph._label("Service; DROP")
    except ValueError:
        pass
    else:
        raise AssertionError("非法节点类型必须被拒绝")
