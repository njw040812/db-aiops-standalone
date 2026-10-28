"""全局配置。

所有阈值都走环境变量（前缀 `AIOPS_`），不把「魔法数字」散落在业务代码里。
Agent 构造函数保留同名参数，测试可以绕过配置直接注入。
"""

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "DB-AIOps 数据库智能运维"
    env: str = "local"

    # ---- 事件总线 ----
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "db-aiops-agents"
    topic_alerts: str = "aiops.alerts"
    topic_events: str = "aiops.events"
    topic_commands: str = "aiops.commands"
    topic_audit: str = "aiops.audit"
    event_log_max: int = 2000

    # ---- 知识图谱 ----
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "dbaiops_password"

    # ---- 监控告警 Agent ----
    monitor_dedup_window_sec: int = 300
    monitor_correlation_window_sec: int = 60
    monitor_min_history: int = 20
    monitor_history_maxlen: int = 1000
    monitor_min_votes: int = 2

    # ---- 根因分析 Agent ----
    rca_max_depth: int = 5
    rca_min_confidence: float = 0.30
    rca_min_margin: float = 0.05
    rca_change_lookback_hours: int = 24

    # ---- 自愈 Agent ----
    heal_dry_run: bool = True
    heal_max_blast_radius: float = 0.20
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout_sec: int = 60
    # 真实执行必须显式开启，默认只做 dry-run + 模拟执行
    enable_real_execution: bool = False

    # ---- 变更审批 Agent ----
    change_auto_approve_threshold: float = 0.30
    change_manual_threshold: float = 0.55
    change_require_ticket_for_l2: bool = True
    business_hours_start: int = 8
    business_hours_end: int = 22

    # ---- 审计 ----
    audit_dir: str = "var/audit"

    # ---- API ----
    api_host: str = "127.0.0.1"
    api_port: int = 8000

    # ---- 可选项（不装也能跑） ----
    prometheus_url: str = "http://localhost:9090"
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    openai_base_url: Optional[str] = None

    model_config = SettingsConfigDict(
        env_prefix="AIOPS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
