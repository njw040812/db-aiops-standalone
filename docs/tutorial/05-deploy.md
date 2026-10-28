# 部署、观测与边界

## 本地运行

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn api.main:app --reload --port 8000
```

打开 http://localhost:8000/docs。

不需要 Docker / Kafka / Neo4j：默认使用内存事件总线和内存图谱，
审计写入 `var/audit/audit-YYYYMMDD.jsonl`。

## 可选基础设施

仓库根目录的 `docker-compose.yml` 可以拉起 Kafka、Neo4j、Prometheus、Grafana：

```bash
docker-compose up -d
```

注意：

- 这些适配层**没有在本机联调**，不保证开箱即用；
- Neo4j 默认密码在 `config/settings.py` 与 compose 中保持一致（`dbaiops_password`）；
- 切换 Kafka 需要安装 `requirements-optional.txt`，并在 `create_event_bus(use_kafka=True)` 处开启。

## 观测

| 位置 | 内容 |
|------|------|
| `GET /metrics` | `dbaiops_incidents_total`、审批计数、Agent 处理数/错误数/P95 延迟 |
| `GET /api/v1/agents/status` | 结构化 JSON，含每个节点的 attempts/status |
| `GET /api/v1/audit/verify` | 审计链完整性 |
| `state.timeline` | 单次故障的完整时间线 |

Demo 的报告里会打印时间线和节点状态，可以直接贴给面试官看。

## 如果要多副本部署，需要补什么

当前实现是单进程、状态在内存，**不能直接多副本**。要补：

1. **检查点持久化**：`_checkpoints` 写 Redis/Postgres；
2. **去重与收敛共享**：告警指纹、服务级收敛状态外置；
3. **审批锁**：`resume()` 用分布式锁，避免两个副本同时执行同一条变更；
4. **熔断状态共享**：否则每个副本各自计数；
5. **审计写入串行化**：哈希链需要单一写入者，或改用带 CAS 的存储；
6. **Kafka 消费者组**：多副本时用 group 分摊 partition。

## Kubernetes 示例（未验证）

下面只是示意，仓库没有镜像构建和 Helm chart：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: db-aiops
spec:
  replicas: 1          # 多副本前先完成上面的状态外置
  template:
    spec:
      containers:
        - name: db-aiops
          image: db-aiops:latest
          ports:
            - containerPort: 8000
          env:
            - name: AIOPS_ENV
              value: production
            - name: AIOPS_ENABLE_REAL_EXECUTION
              value: "false"     # 默认关闭真实执行
          livenessProbe:
            httpGet: { path: /health, port: 8000 }
```

## 上线前必须确认的事

- 真实执行器：`AIOPS_ENABLE_REAL_EXECUTION=true` 前，先补超时、权限隔离、操作审计；
- 数据库账号：只读账号采集指标，执行账号按最小权限授权；
- 审批流：L1/L2 的审批人、工单系统对接；
- 备份：审计文件与检查点需要纳入备份；
- 回滚预案：至少演练一次「执行失败 → 回滚」。
