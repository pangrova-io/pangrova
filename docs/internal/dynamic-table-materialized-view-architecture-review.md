# Pangrova 动态表与物化视图架构审查及完善方案

## 结论摘要

当前实现更接近一组相互独立的原型骨架，而不是已经形成闭环的动态表（Dynamic Table，DT）与物化视图（Materialized View，MV）子系统。解析器、DDL 执行器、状态存储、刷新调度、增量检测、Planner rewrite 和 native dispatch 等部件大多已经存在，但生产启动链路、对象身份、刷新提交协议、CDC 语义、物化写入、查询改写与故障恢复之间没有建立可证明正确的端到端契约。

因此，不建议在当前状态下把 DT/MV 作为生产功能开放。最严重的风险不是“刷新慢”，而是：

1. **生产路径实际上未接通**：生产 bootstrap 没有注册动态表 DDL task，也没有给 Planner 注入 `DynamicTableRewriter`；关键刷新组件还被架构测试明确列为 orphan baseline。
2. **刷新完成语义错误**：native 路径只完成 dispatch 就按成功处理并推进 checkpoint，没有等待 worker 完成与目标表原子提交。
3. **增量写入会产生错误结果**：刷新统一使用 `APPEND`；DELETE 被丢弃，UPDATE/聚合结果会追加重复，无法表达 retract/upsert/replace。
4. **HA fencing 不成立**：租约 epoch 没有进入状态与 sink commit，`coordinatorId` 没有可靠写入，过期 leader 仍可能提交。
5. **查询改写没有等价性保证**：当前 MV matcher 主要依赖表名/列名集合，replacement table 又不是真实 catalog table；join、聚合、谓词、权限、快照语义都可能被破坏。
6. **多租户对象身份不安全**：DDL 丢弃 catalog/schema，状态主键、registry、DAG、lease 和物理目标都以简单名称索引，跨 tenant/schema 同名对象会冲突。
7. **测试产生了错误信心**：DT E2E 通过把物化目标指向源表或内联原 SQL 来模拟刷新，实际上没有验证真实物化、checkpoint、native execution、故障恢复和幂等提交。

推荐的总体方向是：**用统一的 Derived Table 内核承载 MV、DT 与持续查询的物化语义**。MV 是手动、事件或周期驱动的物化策略；DT 是以端到端 freshness SLA 驱动的托管 MV；持续查询是采用流式 checkpoint 的刷新策略。三者共享对象身份、definition version、dependency graph、source position、materialization generation、refresh attempt 和 commit protocol，不再维护互不一致的平行模型。

## 范围、方法与置信度

本次审查以 2026-09-12 的当前工作树为准，覆盖：

- Coordinator 生产 bootstrap、DDL、scheduler、DAG、recovery、lock、refresh pipeline 和 writer；
- Planner 的动态表 SQL rewrite、MV 候选匹配与关系计划替换；
- Metadata 的 DT state/history、MV store/persistence 和数据库 migration；
- CDC、snapshot、connector split 与 native dispatch 边界；
- 单元、property、integration 和 E2E 测试是否真正覆盖生产链路。

工作树包含未提交修改，尤其是 refresh lock/pipeline/recovery；本文只新增审查文档，没有覆盖这些改动。结论分为三类：代码可直接证实的“事实”、由事实推导的“风险”、以及需要通过实现和故障注入验证的“建议”。

## 当前架构与实际数据流

设计意图大致如下：

```text
CREATE DYNAMIC TABLE
        |
        v
DDL executor --> DT state store --> MV registry --> dependency DAG
        |                              |
        +--------> refresh scheduler --+
                           |
                           v
          change detector / incremental evaluator
                           |
                 Java writer or native dispatch
                           |
                    physical target table
                           |
                    advance checkpoint

SELECT FROM dynamic_table
        |
        +--> SQL name rewrite --> physical target
        |
        +--> MV relational rewrite --> candidate target
```

实际生产代码的关键断点是：DDL task 与 refresh subsystem 没有从 bootstrap 构造；Planner 只注入 MV rewriter，未注入 dynamic-table name rewriter；native dispatch 与 completion/commit 被混为一谈；merge-on-read 只生成 hint，执行层没有消费；MV persistence listener 和 JDBC hook 没有生产调用链。因此，上图目前不是一条闭环数据流。

## 成熟度矩阵

| 能力 | 当前状态 | 生产判断 |
|---|---|---|
| SQL AST / DDL 类 | 已有 CREATE、ALTER、SHOW、DESCRIBE 等骨架 | 可作为 API 草案 |
| 生产 bootstrap | DT task、scheduler、refresh coordinator 未接通 | 阻断发布 |
| 对象元数据 | DT/MV/asset 三套模型，简单名称主键 | 需重构 |
| 初次物化 | 没有完整的物理 target 创建与原子 publish | 阻断发布 |
| 增量刷新 | detector/evaluator 骨架存在，语义不完整 | 会错数 |
| native 刷新 | dispatch 后即视为成功 | 会丢数/假成功 |
| CDC checkpoint | 使用事件时间字符串，不是 partition offset/LSN vector | 不可恢复 |
| 写入幂等 | 只检查 latest commit property，固定 APPEND | 不成立 |
| 分布式 fencing | lease 与 sink/state epoch 未贯通 | 不成立 |
| 调度 | 单线程 JVM timers + 本地计数器 | 不可扩展 |
| DAG | JVM 内存双 map，无持久化事务 | HA 后不可靠 |
| MV rewrite | 简化结构匹配 + 伪 table ref | 有错结果风险 |
| merge-on-read | 仅有未消费 hint | 未实现 |
| schema evolution | 字符串 schema 占位解析、模糊匹配 | 不安全 |
| 多租户与权限 | 简单名索引，权限上下文未纳入 rewrite/refresh | 不安全 |
| observability | 主要是日志，没有完整 SLO 指标/系统表 | 不可运营 |
| E2E | 多数通过源表透传模拟物化 | 无法证明正确性 |

## P0：必须先解决的正确性与可用性问题

### P0-1 生产装配链路缺失

本次历史审计时，`QueryExecutionBootstrap` 和 `QueryExecutionWiring` 注册了 index/stream task，却没有动态表 task。全仓构造搜索显示 `DynamicTableDdlContext`、`DynamicTableDdlExecutor`、`RefreshCoordinator`、`RefreshScheduler`、`JdbcDynamicTableStateStore` 和 `JdbcRefreshHistoryStore` 只在测试路径被完整组装。`BootstrapReachabilityTest` 还把 `RefreshCoordinator`、`RefreshLockCoordinator` 和 `RefreshScheduler` 放在 `BASELINED_ORPHANS` 中。后续修复已删除没有任何调用方的 `QueryExecutionWiring` 副本；当前唯一装配入口是 `QueryExecutionBootstrap`。这项清理不构成当时 DT 能力已接通的证明。

生产 Planner 的 `PlannerWiring` 给 `PlannerContext` 的 `dynamicTableRewriter` 与 `metadataVersionProvider` 传入 `null`。因此，即便状态被外部写入，正常 SQL 也不会通过生产 Planner 解析动态表名。

2026-10-16 增量修正：`DynamicTableSqlRewriter` 的文本替换现在用 SQL lexer 的 token 位置匹配 AST 表引用；字符串、注释及投影列中先出现同名文本时，不再误改它们。此修正只封闭名称替换的局部错误，不补齐上述生产装配、CTE/子查询遍历、租户与权限域校验，也不证明物化结果可见性。

**影响**：功能在测试环境看似存在，但标准 coordinator 启动后无法形成 create → schedule → refresh → query 的链路。任何后续优化都必须先以“production reachability”作为验收门槛。

### P0-2 dispatch 被错误当成 commit

`RefreshCoordinator.executeNativeRefreshPipeline` 调用 `nativeRefreshDispatch().apply(...)` 后忽略 `DispatchResult`，立即构造零行成功结果并推进 checkpoint。`DispatchResult` 只包含 query/endpoint 等控制面信息，不代表 worker 已经完成，更不代表目标表已经提交。代码中虽存在 `NativeRefreshOutcome`，但接口仍返回 `DispatchResult`，且 outcome 没有消费者。

这打破了最基本的不变量：

```text
published_checkpoint <= target_table_committed_source_position
```

当前实现可能出现：worker 尚未开始或已经失败，metadata checkpoint 已推进；下一次刷新从新 checkpoint 继续，永久跳过上一批数据。

**必须改成**：dispatch 返回 execution handle；coordinator 等待所有 fragments 达到 prepared；目标 catalog 使用受 fencing token 保护的原子 commit 发布新 generation；只有 commit 成功后，metadata 才能把 active generation 与 committed source position 一起发布。

2026-10-16 增量修正：`DynamicTableRefreshDispatchFunction` 已改为返回终态 `NativeRefreshOutcome`，`RefreshCoordinator` 拒绝 null、失败及不匹配的已提交 checkpoint，重新检查 epoch/租约后才推进状态，并记录实际行数/字节数；聚合、投影和 merge 的直接分发调用也检查同一结果。此修正仅封闭“收到派发描述符即报成功”的接口漏洞。生产装配、真实目标表原子 generation 提交、源位置与目标提交的共同 fencing、撤销/未知提交恢复和实际物化 SQL E2E 尚未完成，不能据此宣称 P0-2 全链闭合。

### P0-3 APPEND 不能承载一般增量维护

`DynamicTableWriter` 固定使用 `WriteMode.APPEND`。Filter/project 路径忽略 DELETE，aggregate 路径把新的 group state 作为行追加。对于 UPDATE、DELETE、聚合回撤、重复 replay，这分别造成陈旧行、重复行和累计错误。

完整刷新与增量刷新至少需要以下写入语义之一：

| 变化类型 | 可接受的 target 语义 |
|---|---|
| 全量重算 | 写入新 generation 后 atomic swap；不能向旧结果 APPEND |
| 仅 insert 的单调查询 | APPEND + 唯一 batch token |
| 主键表的 update/delete | UPSERT/MERGE 或 equality/position delete |
| 可撤回聚合 | keyed state + retract/upsert affected groups |
| 任意非增量算子 | 自动降级为新 generation 全量刷新 |

Iceberg 的快照、sequence number 和 delete file 机制可以承载原子发布与行级删除；其可靠性设计通过原子替换当前 metadata pointer 提供一致快照与并发写冲突校验，而不是靠“最后一次属性”猜测幂等性。[Apache Iceberg Reliability](https://iceberg.apache.org/docs/1.7.1/reliability/) [Apache Iceberg Specification](https://iceberg.apache.org/spec/)

### P0-4 lease 不是可执行的 fencing

当前 lock/lease 只在 coordinator 内存控制流程中检查：

- `DynamicTableState.epoch` 初始化后没有与 `DistributedLease.epoch` 对齐；
- target writer/connector commit 没有接收并验证 fencing token；
- `coordinatorId` 没有可靠写入 DT state，而 lock acquisition 依赖它；
- `RefreshLockCoordinator` 使用一个全局 `AtomicBoolean renewalAborted`，多个表之间会相互清除或污染失锁状态；
- lease 可能在“写前检查”之后、实际 catalog commit 期间过期。

**影响**：leader failover、长 GC pause 或网络分区后，旧 coordinator 能在新 leader 之后写入，产生 generation 回退或重复提交。

**必须改成**：每个 refresh claim 生成单调的 `fencing_epoch`；worker fragment、target commit 和 metadata publish 都携带 `(object_id, definition_version, attempt_id, fencing_epoch)`；catalog adapter 在原子提交点拒绝小于当前 epoch 的写入。失锁标志必须绑定 attempt，而不是 coordinator 全局字段。

### P0-5 MV rewrite 缺少关系等价性证明

当前 `StructuralSubsumptionMatcher` 主要比较基表名集合和列名集合；含 WHERE 的 MV 会直接被拒绝，而 join 的多重性、谓词蕴含、表达式、类型/nullability、aggregate function、roll-up、约束和时间旅行都没有被完整证明。`DigestPlanRewriter` 用原 query row type 构造一个 `MaterializedViewTableRef`，这个 table ref 的 schema、`toRel`、keys、distribution 等关键信息为空或固定值，再把整棵 plan 替换成 scan，最多补偿一层 filter。

`CalciteMaterializedViewRewriter` 的说明声称使用 Calcite substitution，但实现仍委托上述 matcher/digest rewriter。Calcite 官方提供 `SubstitutionVisitor`、`RelOptMaterialization` 和 `RelOptMaterializations`，支持 residual predicate、join、aggregate、roll-up、partial/union rewrite，同时也明确指出复杂 join 上的等价变换枚举存在可扩展性成本。[Apache Calcite Materialized Views](https://calcite.apache.org/docs/materialized_views.html) [RelOptMaterializations API](https://calcite.apache.org/javadocAggregate/org/apache/calcite/plan/RelOptMaterializations.html)

**短期措施**：生产默认关闭自动 MV rewrite，只允许用户显式读物理对象或白名单化的 `SELECT-project-filter` 精确等价场景。**长期措施**：用真实 catalog `RelOptTable`、正式 relational equivalence、source/version/security coverage 与 cost-based selection 替代 SQL/digest 猜测。

### P0-6 简单名称导致跨租户冲突

DDL 的 `extractName` 只保留 identifier 最后一段。`dynamic_table_state.dt_name` 是全局主键；registry、DAG、refresh history、lease map 和默认 `__dt_<name>` target 也都以简单名索引。

在 `tenant_a.sales.daily_orders` 和 `tenant_b.finance.daily_orders` 同时存在时，当前模型无法区分对象，可能发生覆盖状态、误刷新、误 drop、跨租户读写或使用错误物化结果。

统一身份必须是稳定 UUID，并用 fully-qualified logical key 作为唯一约束：

```text
object_id UUID
logical_key = (tenant_id, catalog_id, schema_id, normalized_name)
physical_generation_key = (object_id, generation_id)
```

所有 dependency、attempt、checkpoint、lease、history、rewrite candidate 与 metrics 都引用 `object_id`，不能再以显示名称做内部主键。

### P0-7 测试没有验证真实物化

`SqlE2ETestBase` 中的 DT refresh helper 对简单查询把 target override 指向源表，对复杂查询则注册原 SQL 的 inline subquery，然后设置 `enableQueryRewrite=true`。因此所谓 DT query、CDC 和 merge-on-read E2E 实际是在直接读源数据或每次重新执行定义 SQL。

`DynamicTableMemoryQuotaE2ETest` 没有关键断言；E2E base 在 native library 缺失时使用 assumption 跳过，与仓库要求的 native prerequisite blocking 相冲突。

**影响**：测试能通过恰恰掩盖了物理 target 未创建、DELETE 未处理、native completion 未等待、checkpoint 错误推进和 recovery 不成立。

## P1：大规模分布式环境下的结构性问题

### 调度器无法扩展到大量对象

`RefreshScheduler` 使用单线程 `ScheduledExecutorService`，并在该线程同步执行 refresh。即使配置了 `maxConcurrentGlobal`，实际刷新仍串行。每张表使用一个 `scheduleAtFixedRate(targetLag/2)` timer，形成 O(N) JVM timer、同周期惊群和 coordinator 内存增长。

所谓 tenant queue 只是计数器：没有 slot 时任务被跳过而非持久化排队；`queueDepth()` 返回的是注册表数，不是待执行 work item 数。retry 会取消周期 future 并安排一次 one-shot；该 retry 成功后没有恢复周期调度，单次瞬时失败可让表永久停止自动刷新。全局/tenant 并发计数也只存在单 JVM，无法跨 coordinator 提供公平与限流。

此外，当前 `triggerCascadeRefresh` 对同一个 upstream completion 连续调用两次 `onUpstreamRefreshComplete`。在依赖传播没有全局幂等键的前提下，一个上游成功会重复触发整片下游，放大扫描、争抢 slot，并进一步增加重复提交概率。

Snowflake 对 target lag 的定义是 freshness target，而不是固定刷新间隔；对 DT pipeline 会按依赖顺序协调、避免同一表并发刷新，并在同一 pipeline cycle 中保持一致快照。[Snowflake Target Lag](https://docs.snowflake.com/user-guide/dynamic-tables-target-lag) Pangrova 当前把 lag 机械映射为 interval/2，既无法预测执行耗时，也不能管理 DAG 的端到端 lag。

### DAG 不持久、不原子、不具备 pipeline snapshot

`DynamicTableDagManager` 用两个 `ConcurrentHashMap` 维护正反向边，跨 map 更新不是原子的；cycle check 与 mutation 没有串行化，两个并发 DDL 可以分别通过检查后共同形成环。深度、reachability 和 dependents 使用递归，在深链路上有栈溢出风险。

dependency 没有作为 definition 的持久化记录；recovery 清空 DAG 后逐条重建，读者会看到空图或半图。恢复仅处理部分 lifecycle state，且要求已有 checkpoint；创建中、首次刷新前、PAUSED、LAGGING 等状态可能永久丢失调度或依赖。

更关键的是，当前刷新没有为一条 pipeline 选择统一 source cut。上游成功、下游失败或多源 join 读取不同时间点后，用户可能看到跨时间拼接的数据。成熟语义应保证：失败导致 staleness，不导致 pipeline 内部 inconsistent result；Snowflake 的 pipeline snapshot isolation 与 failure propagation 是一个可参考的外部基线。[Snowflake Data Consistency](https://docs.snowflake.com/en/user-guide/dynamic-tables/data-consistency)

### CDC position 不是可恢复 checkpoint

`ChangeResult.checkpoint()` 使用最后一个事件的 `eventTime().toString()`。时间戳无法表达 Kafka topic-partition offset、数据库 LSN/SCN、Iceberg snapshot + sequence 或多源 position vector；跨 partition 乱序、相同时间戳、时钟精度和迟到事件都会造成丢失或重复。

DT detector 还重复实现了一套 CDC 注册/heartbeat/消费逻辑，没有复用仓库已有的 `CdcPipelineLoop`、`CdcTwoPhaseCommitter` 和 durable commit state。AUTO 从 CDC fallback 到 snapshot 时只替换 pipeline 类型，却继续复用原 detector/evaluator，实质上可能仍在走 CDC detector；CDC offset 与 snapshot ID 之间也没有使用一致的 handoff boundary。

Snapshot 增量 split 也没有真正进入 refresh plan。此前 `tryUseIncrementalSplits` 的注释称会调用 `ConnectorSplitManager.getIncrementalSplits()` 并消费 split source，实际实现只询问 `IncrementalScanCapability` 并记录日志，而且传入的是 DT 名称而不是已解析的 source object。现已删除这段不会改变扫描计划的检查和 `dt_incremental_splits_enabled` 误导性日志；真正的修复仍需把 source handle、快照范围及 connector 产生的 splits 接入实际 refresh dispatch，并用物理扫描证据验证 O(delta)。

现有跨快照 E2E 夹具 `SqlE2ETestBase.executeRefresh` 把 DT 读取直接重写为源表/定义查询，未运行 `RefreshCoordinator`，因此行值正确不能证明物化刷新、CDC checkpoint 或增量 split。该用例已改名为 `DynamicTableReadRewriteE2ETest`，仅记录它实际覆盖的查询重写行为；生产验收需要独立进程的真实 refresh executor、持久化物化结果以及物理扫描统计。

可恢复的 position 应是 connector-owned opaque vector：

```text
SourcePosition {
  source_object_id,
  source_generation,
  partitions: [{partition_id, inclusive_start, exclusive_end}],
  snapshot_id?, sequence_number?, schema_id?
}
```

Exactly-once 不能只靠 coordinator state。Flink 的官方语义要求可 replay source、与 operator state 一致的 checkpoint，以及参与 checkpoint 的 transactional/idempotent sink；keyed state 与 stream 同分区，才能在扩缩容时重分布而保持一致。[Flink Stateful Stream Processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/) [Flink Fault Tolerance Guarantees](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/)

### 增量 evaluator 不能承载分布式状态

Aggregate evaluator 把 group state 放在 coordinator JVM 的 `HashMap` 中：不持久、不可分区/扩缩、非线程安全、无 checkpoint、无 TTL。group key 用字符串拼接与 `toString`，会丢失类型并有碰撞；COUNT(NULL)、AVG 精度、MIN/MAX retract、deleted group cleanup 等语义也不完整。

这违反项目自身的 Java coordination / C++ execution 边界。大规模 IVM 的 operator state 必须在 worker 侧按 key 分片，采用可 checkpoint 的 state backend；或者只生成 delta plan，由 connector/target table 的 MERGE 原子应用。Coordinator 只能持有控制面元数据，不能承载业务数据状态。

### metadata 表模型形成热点且无法演进

单个 `dynamic_table_state` row 同时承载 desired config、observed lifecycle、ownership、checkpoint、failure count 和 refresh time。scheduler、DDL、failure handler、recovery 的 blind upsert 会互相覆盖字段。checkpoint 列 `VARCHAR(1024)` 无法容纳多源、多 partition position vector。

`listAll()` 在启动与 schema 事件中全表扫描；history 以 OFFSET 分页，无对象外键、时间分区和完整 retention job。MV store 默认是内存实现，JDBC hook 没有生产 wiring，使用 H2 风格 `MERGE INTO ... KEY`，与生产 PostgreSQL migration 不兼容；异常只记录日志并返回空，使内存与 durable store 静默分叉。

### DDL 不是事务性 lifecycle

CREATE 顺序大致是 register MV → add DAG → state ACTIVE → trigger initial refresh → register scheduler。状态在首次刷新前就 ACTIVE；初次刷新以 failure value 返回时不会使 CREATE 失败；真实成功后也没有生产代码开启 query rewrite。物理 target 创建没有形成显式步骤。

DROP 先删 state，再删 registry；普通 DROP 没有可靠移除 DAG node，也没有统一清理 target、history、lease/checkpoint。CASCADE 先改变内存图再逐对象删除，崩溃会留下部分状态。REDEFINE 没有新 definition version/generation、锁、rollback 或 atomic swap，可能让“新定义”读到“旧定义”的物化结果。

SUSPEND/RESUME/SET LAG 分别修改 store 和 scheduler，任一步失败都会导致 desired state 与运行时状态分裂。`warehouse`、properties 和 feasibility checker 基本未进入实际策略；source dependency 只看 query 的直接 `FROM`，漏掉 JOIN、CTE、subquery、view 和 table function。

### MV 并非独立完整产品

仓库没有完整的 `CREATE MATERIALIZED VIEW` / `REFRESH MATERIALIZED VIEW` 用户入口。`MaterializedViewAsset` 只是一份未与 planner/refresh 生命周期连通的 catalog model；`MaterializedViewMetadata`、`MaterializedViewDefinition` 与 `DynamicTableState` 各自表达一部分状态，没有共同 object ID、definition version、physical generation 或 source version。

MV recovery 只恢复 incremental 条目，并用不带 structural metadata 的 constructor 重建 definition；恢复后 `baseTableNames` 为空，候选无法通过 structural matcher。registry 的 lifecycle listener 没有实际事件调用，持久化逻辑事实上是死路径。

### schema、权限与治理语义未闭环

- schema recovery 使用逗号/冒号字符串占位解析，unknown/empty 被当作 compatible；
- schema inspector 通过 `query().contains(table)` 判断影响范围，既漏报也误报，且每次扫描所有 DT；
- CREATE 固定 `sourceSchemaVersion=1`，没有绑定 field ID 或真实 schema version vector；
- refresh identity 只有 principal 字符串，没有 role/policy version 与 privilege revalidation；
- query rewrite 没有验证 tenant、row filter、masking、RLS、time travel 或 security domain 等价；
- 内部物理 target 没有明确的 system namespace 与 ACL 隔离。

Databricks 的增量 MV 明确要求 source capability，例如 row tracking、deletion vectors/change feed，并指出 row filter/column mask 会限制增量刷新；同时所有 source 必须仍能承受 full-refresh fallback。[Databricks Incremental Refresh](https://docs.databricks.com/gcp/en/ldp/incremental-refresh) Pangrova 也需要把 connector、算子和安全策略的能力交集变成可解释的 eligibility decision，而不是任何 SQL 都先接受再尽量执行。

### merge-on-read 只有 hint，没有执行语义

`DynamicTableRewriter` 在检测到 source snapshot 更新后返回 `MergeOnRead`，但 SQL 仍替换成 materialized target。Planner 把 resolution 收集到 `PlanResult/PlannedQuery.mergeOnReadHints`；全仓没有执行侧读取这些 hints 的代码。因此注释中的：

```sql
SELECT ... FROM materialized_target
UNION ALL
SELECT ... FROM source_delta
```

从未被构造成关系计划，更没有处理 update/delete 去重、聚合重算或 snapshot boundary。snapshot provider 异常时 rewriter 还会静默 fallback 到 direct materialized read，返回已知可能陈旧的数据。

### 可观测性不足以运营 freshness SLA

当前主要输出日志，没有真实闭环的 metrics/system tables。refresh history 在多处只写“refresh failed”，丢失 connector error code、阶段、attempt、worker 与 source position。缺少：

- actual lag、time within target、overdue duration、queue wait、run duration；
- input/output rows/bytes、change amplification、full-refresh fallback 原因；
- 当前 source position、target generation、definition version 与 schema version；
- retry/fencing conflict、commit latency、orphan files、state size；
- rewrite hit/miss 及拒绝原因、候选成本；
- tenant/warehouse fairness 与资源消耗。

Snowflake 暴露 mean/max lag、time above target、within-target ratio、latest data timestamp 和 last refresh state 等对象级指标，说明 target lag 产品不能只依赖任务日志运营。[Snowflake DYNAMIC_TABLES](https://docs.snowflake.com/en/sql-reference/functions/dynamic_tables)

## 大规模场景下的具体失效模式

| 场景 | 当前行为 | 结果 |
|---|---|---|
| 10 万 DT，目标 lag 相近 | 10 万 JVM periodic timers，在半 lag 周期惊群 | coordinator GC、调度漂移、遗漏刷新 |
| 宽 DAG 的一个 root 更新 | 每表独立 timer，无 pipeline coalescing | 重复扫描同一 source，成本放大 |
| 深 DAG 中间节点失败 | 下游仍可能独立运行 | 读取不同 source cut，结果不一致 |
| leader 在 target commit 前后切换 | lease epoch 不进入 sink，旧 leader 可继续写 | 双提交、generation 回退 |
| Kafka 多 partition CDC | checkpoint 只保存最后事件时间 | partition 进度丢失，重放不可确定 |
| UPDATE/DELETE 密集 | evaluator 丢 delete，writer APPEND | stale/duplicate rows |
| 1 亿 group 聚合 | state 位于单 coordinator HashMap | OOM，无法扩缩和恢复 |
| 两租户同名 DT | 全局 `dt_name` 主键/registry key 冲突 | 越权、误删、错读 |
| ALTER definition 与 refresh 并发 | 无 definition version/generation fence | 新定义发布旧数据 |
| source schema change | 模糊字符串匹配和 fail-open compatibility | 运行时错列或静默错数 |
| source snapshot 被 vacuum | 没有 pinned source vector/retention contract | refresh 无法重试且无清晰恢复策略 |
| retry 一次成功 | periodic future 已取消且未恢复 | 此后永久不自动刷新 |

## 目标架构

### 统一 Derived Table 内核

```text
                         ┌─────────────────────────────┐
SQL DDL / API ---------->| Derived Object Catalog      |
                         | object + versioned definition|
                         └──────────────┬──────────────┘
                                        |
                    transactional dependency graph
                                        |
                         ┌──────────────v──────────────┐
source commits --------->| Persistent SLA Work Planner |
manual/event trigger --->| deadline + DAG + admission  |
                         └──────────────┬──────────────┘
                                        | claim(attempt, epoch)
                         ┌──────────────v──────────────┐
                         | Distributed Refresh Runtime |
                         | pinned source vector        |
                         | native plan + keyed state   |
                         └──────────────┬──────────────┘
                                        | prepared fragments
                         ┌──────────────v──────────────┐
                         | Fenced Generation Commit    |
                         | target snapshot + checkpoint|
                         └──────────────┬──────────────┘
                                        | atomic publish
                         ┌──────────────v──────────────┐
query planner ---------->| Rewrite Candidate Index     |
                         | equivalence/security/freshness|
                         └─────────────────────────────┘
```

对象类型只影响 refresh policy：

| 产品概念 | 统一内核中的表达 |
|---|---|
| Materialized View | Derived Object + MANUAL/EVENT/SCHEDULE policy |
| Dynamic Table | Derived Object + TARGET_LAG/DOWNSTREAM SLA policy |
| Continuous Query | Derived Object + STREAMING_CHECKPOINT policy |

### 持久化数据模型

建议将当前单表 state 拆为不可变定义、运行态、刷新 attempt 与提交记录：

```text
derived_object(
  object_id, tenant_id, catalog_id, schema_id, name,
  object_type, desired_state, owner, run_as, created_at
)

derived_definition(
  object_id, definition_version, normalized_sql, logical_plan_digest,
  output_schema, refresh_policy, warehouse_id, security_context_version,
  created_at, status
)

derived_dependency(
  object_id, definition_version, source_object_id,
  source_kind, boundary_type, referenced_field_ids
)

refresh_intent(
  intent_id, object_id, definition_version, deadline,
  priority, reason, desired_source_cut, state, shard_key
)

refresh_attempt(
  attempt_id, intent_id, fencing_epoch, coordinator_id,
  execution_id, started_at, heartbeat_at, phase, error_code
)

source_checkpoint(
  attempt_id, source_object_id, partition_id,
  start_position, end_position, source_schema_id
)

materialization_generation(
  generation_id, object_id, definition_version, target_table_id,
  target_snapshot_id, source_vector_digest, schema_id,
  fencing_epoch, status, committed_at
)

refresh_commit(
  commit_id, attempt_id, generation_id, source_vector,
  target_snapshot_id, manifest_uri, committed_at
)
```

`derived_object.active_definition_version` 和 `active_generation_id` 应通过 metadata transaction/CAS 发布。大 payload（plan、schema、source vector、manifest）使用 JSONB/side artifact，而不是塞进 1 KB 字符串列。history 用 keyset pagination、时间分区和 retention policy。

### 刷新提交协议

推荐状态机：

```text
PENDING -> CLAIMED -> PLANNING -> RUNNING -> PREPARED -> COMMITTING -> COMMITTED
               \          \          \          \
                +----------+----------+-----------> FAILED / ABORTED
```

协议步骤：

1. Scheduler 在 durable queue 中生成/合并 intent；claim 使用 CAS，返回单调 fencing epoch。
2. Planner 固定 `definition_version` 和所有 source 的一致 `SourcePositionVector`，验证 snapshot retention。
3. Native workers 执行全量或 delta plan；stateful operator state 按 key group 分区并 checkpoint。
4. Workers 输出带 `attempt_id + epoch + fragment_id` 的 immutable fragment/manifests，重复上传幂等。
5. Coordinator 等待所有 fragment prepared；任何 worker failure 都不能推进 source position。
6. Catalog adapter 以 expected target snapshot 和 fencing epoch 执行 atomic commit，产生新 target snapshot/generation。
7. Metadata 原子发布 active generation 与 committed source vector；若进程在 6/7 之间崩溃，recovery 根据 durable `refresh_commit` 重放 publish，而不是重写数据。
8. commit 后异步清理旧 generation/orphan fragment；保留足够历史以支持 rollback 和 audit。

核心不变量：

1. checkpoint 只随已发布的 target generation 前进；
2. 每个 generation 绑定唯一 definition version 和完整 source vector；
3. 小于 current fencing epoch 的 commit 永远被 target 与 metadata 拒绝；
4. 同一对象任一时刻只有一个 active generation；
5. replay 同一 attempt/fragment/commit 不改变最终结果；
6. downstream 只消费同一个 pipeline source cut 中已提交的 upstream generation；
7. query rewrite 不得跨 tenant/security/snapshot/semantic domain。

### Refresh capability 与增量策略

在 CREATE/ALTER 时生成可持久化的 capability decision：

```text
eligibility = intersection(
  logical_operator_capabilities,
  source_connector_change_capabilities,
  target_connector_write_capabilities,
  security_policy_constraints,
  schema_evolution_constraints
)
```

结果应是 `INCREMENTAL`、`FULL_ONLY` 或 `UNSUPPORTED`，并列出 operator/source/reason。`AUTO` 在 definition version 内可动态选择，但每次选择与 fallback 原因必须记录；FULL fallback 必须使用新 generation replace，而不是沿用 delta evaluator。

ChangeSet 统一表达 `INSERT / UPDATE_BEFORE / UPDATE_AFTER / DELETE`，保留 row identity、source partition 和 position。只有可证明单调的查询允许 append-only。聚合、join、distinct、window 等需要 operator-specific IVM 或完整重算，不能由一个通用 HashMap evaluator 冒充。

### 可扩展调度器与 DAG

放弃 per-table JVM timer，改为 durable deadline queue：

- `next_deadline = root_data_timestamp + target_lag - predicted_runtime`；
- 对同一 source cut 的上下游 intent 做 pipeline coalescing；
- 以 consistent hashing/shard lease 分散到多个 coordinator；
- priority 综合 overdue、critical path、tenant weight、cost、manual urgency；
- per-warehouse resource group 管控 CPU、memory、scan bytes 与 concurrency；
- 无 slot 的工作保持 PENDING，不得静默跳过；
- 加 jitter、backpressure、load shedding，并明确“无法满足 SLA”状态；
- 同一 `(object_id, definition_version)` 最多一个有效 refresh，新的 source changes 合并到后续 intent；
- `DOWNSTREAM` 节点只由需求方 pipeline 触发，避免独立重复刷新。

DAG edge 随 definition version 持久化。DDL 在单事务内做 name reservation、dependency resolution、cycle validation 与 definition activation。内存图是可重建 cache，不是真相来源。更新使用增量 topological/SCC 检查；refresh cycle 固定 graph version 与 source cut。

### 安全的 MV/DT 查询改写

改写流程应为：

1. 用 source relation IDs、operator signature 与 output field IDs 建 candidate index，避免每个 query 扫描全部 MV；
2. 从真实 catalog 构造 materialized table `RelOptTable`；
3. 使用 Calcite `RelOptMaterialization`/substitution 或等价的正式规则产生多个正确候选；
4. 检查谓词蕴含、join multiplicity/constraints、aggregate roll-up、类型/nullability 与 expression equivalence；
5. 检查 generation 的 source vector 覆盖 query snapshot/time travel；
6. 检查 tenant、owner/run-as、RLS/mask/filter policy version 完全兼容；
7. 用真实 row count、NDV、files/partitions、refresh age 和 delta merge cost 做 cost selection；
8. 无法证明时 fail closed，返回原 plan，并暴露拒绝 reason。

merge-on-read 不应是 side hint。它必须构造成正式 relational plan，并根据 changelog 语义做 dedup/retract/upsert。第一阶段建议完全关闭 MOR，只提供 committed-generation consistency；待 source delta scan、row identity 和 delete semantics 完整后再开启。

### schema evolution 与重建

- 依赖以稳定 field ID 绑定，不以列名或 SQL substring 判断；
- 每个 generation 记录 input/output schema IDs；
- additive/widening 等兼容变化按规则自动演进；rename/drop/narrowing、表达式语义变化进入 `NEEDS_REBUILD`；
- rebuild 生成新 definition/generation，完成后 atomic swap；旧 generation 在 retention window 内保留；
- source snapshot expiration 在执行前检测，明确选择 FULL rebuild 或失败，不得从未知 checkpoint 继续；
- dependency boundary 必须显式声明是否保留 pipeline snapshot isolation。

### 权限、隔离与审计

- object/physical target 均使用 tenant-scoped ID 与内部 namespace；
- 明确 DEFINER/INVOKER 语义，refresh 前重验 source USAGE/SELECT 与 warehouse permission；
- definition 保存 security context/policy version，策略变化使旧 generation 不再 eligible 或触发 rebuild；
- internal target 默认禁止用户直读，所有访问经过逻辑对象授权；
- DDL、refresh claim、source read、target commit、rewrite decision 均写不可变 audit record；
- SHOW/DESCRIBE/HISTORY 全部以 tenant/object_id 授权，不能只按显示名称查询。

### 运维面与系统表

至少提供：

- `system.derived_tables`：desired/observed state、definition/generation、actual/target lag；
- `system.refresh_history`：attempt phase、source vector、target snapshot、rows/bytes、error；
- `system.refresh_queue`：deadline、priority、queue time、warehouse、claim owner；
- `system.derived_dependencies`：versioned graph 与 boundary；
- `system.rewrite_decisions`：候选、freshness/security/cost 判定与拒绝原因；
- `EXPLAIN MATERIALIZATION`：incremental eligibility、operator fallback 与预估成本。

告警以 SLO 为中心：time-within-target ratio、overdue duration、oldest checkpoint、queue saturation、full-refresh amplification、fencing conflict、recovery backlog、orphan storage 与 rewrite correctness guard rejection。

## 分阶段落地计划

### 阶段 0：止损与建立真实性门槛

目标：避免错误结果，禁止“测试绿但生产不可达”。

- DT/MV production feature flag 默认关闭；自动 MV rewrite 与 merge-on-read 默认关闭；
- 把 native dispatch API 拆为 `dispatch()` 与 `awaitCommittedOutcome()`，禁止收到 `DispatchResult` 后推进 checkpoint；
- 对非 append-only 查询拒绝 `APPEND`；FULL 使用 generation replace；
- 修复 qualified object identity，至少在新表中加入 tenant/catalog/schema/object UUID；
- 删除 E2E 的 source override/inline query 伪物化，测试必须创建真实 target 并验证 source 与 target 可独立变化；
- production bootstrap reachability test 不再允许 DT orphan baseline；
- 用 PostgreSQL Testcontainers 验证 migration 与 store，禁止 persistence exception 静默降级为内存；
- 建立 P0 failpoint 测试，未通过前不开放 preview。

**退出条件**：create → initial refresh → physical target commit → query → restart/recover 的真实链路可运行；worker failure 不推进 checkpoint；update/delete E2E 无重复/陈旧行。

### 阶段 1：统一对象模型与事务性 DDL

目标：建立稳定身份、definition version、dependency 与 generation。

- 新建 `derived_*` migrations 与 repository API；
- 将 MV/DT/continuous query 映射到统一对象，不再平行维护三套状态；
- DDL 使用 metadata transaction 与状态机：
  `CREATING -> INITIALIZING -> ACTIVE/DEGRADED -> PAUSED/NEEDS_REBUILD -> DROPPING -> DROPPED`；
- CREATE 先 reserve name，再验证 query/capability/DAG，再创建 target，最后激活 definition；
- REDEFINE 构建新 definition/generation，成功后 swap；DROP 使用 tombstone + 异步 GC，保证可恢复；
- dependency extractor 基于 validated RelNode，覆盖 join/CTE/subquery/view。

**退出条件**：并发 CREATE/ALTER/DROP/property tests 不能形成环、覆盖同名其他租户或暴露 definition/generation 错配。

### 阶段 2：分布式刷新与 fenced commit

目标：建立 exactly-once effect，而不是仅 exactly-once coordinator state。

- 实现 durable refresh attempt/commit journal 与 monotonic epoch；
- native execution 返回可等待的 task graph outcome 和 fragment manifests；
- connector SPI 增加 atomic generation commit、expected snapshot、fencing token、idempotency key；
- CDC 复用 streaming 2PC/checkpoint 基础设施，保存 partition/source vector；
- stateful IVM 下沉到 native worker + checkpointable keyed state；
- crash recovery 覆盖 PREPARED、target committed but metadata unpublished、lost lease 等阶段。

**退出条件**：所有 commit 边界 failpoint 循环中无数据丢失、无重复可见结果、旧 leader 永远不能覆盖新 generation。

### 阶段 3：可扩展 SLA scheduler 与 pipeline consistency

目标：对象规模与 coordinator 数量线性扩展。

- persistent deadline queue、sharded ownership、jitter/coalescing；
- DAG critical-path scheduling、DOWNSTREAM policy、consistent source cut；
- tenant/warehouse admission、fair queue、cost-aware concurrency；
- event-driven source change notification + 低频 reconciliation，取代 per-table timer；
- 启动 recovery 分页/并行/限流，不再 `listAll()` 后串行恢复。

**退出条件**：10 万对象/100 万 edges 的 synthetic benchmark 中 coordinator memory 有界，无 timer 惊群；HA 切换后 queue/claims 可收敛；lag SLO 可测且违约可解释。

### 阶段 4：安全、可成本化的查询改写

目标：只做可证明正确且收益为正的 rewrite。

- 真实 `RelOptMaterialization` 与 catalog table metadata；
- formal equivalence + compensation/roll-up/partial rewrite；
- source/security/snapshot coverage guard；
- candidate inverted index 与 CBO ranking；
- shadow rewrite 对比原 plan 结果与成本，逐场景灰度；
- 最后再实现 changelog-aware MOR relational operator。

**退出条件**：随机查询/property differential test 与原查询结果一致；任何 security/snapshot mismatch 都拒绝改写；候选选择有可解释 cost。

### 阶段 5：生产硬化与 rollout

- shadow scheduler 先只计算决策，不触发写入；
- metadata dual-write、backfill stable IDs、校验后切读；
- 既有 DT 以新 generation 重建，不把旧 checkpoint 强行转换为 CDC vector；
- canary tenant/warehouse，逐步开放 operator/connector capability；
- 保留全局 kill switch、per-object rewrite/refresh disable 与 generation rollback；
- 完成 chaos、容量、升级/降级、灾备与运维 runbook。

## 模块级工作包

| 模块 | 主要工作 |
|---|---|
| `pangrova-parser` | 统一 MV/DT DDL，fully-qualified identifiers，refresh policy/initialization/security 语法 |
| `pangrova-metadata` | derived object schema、definition/dependency/generation/attempt/commit repository、事务/CAS、keyset history |
| `pangrova-planner` | capability analyzer、正式 MV substitution、candidate index、security/snapshot guards、MOR RelNode |
| `pangrova-coordinator` | production wiring、durable scheduler、DAG cache、attempt orchestrator、recovery/state machine |
| connector SPI | source position vector、change capabilities、incremental splits、atomic/fenced generation commit |
| native worker | distributed delta plan、keyed state、checkpoint、fragment manifest、retract/upsert operators |
| `pangrova-testing` | real-target E2E、Postgres/connector integration、multi-process HA/failpoint/scale/differential tests |
| docs/ops | SQL semantics、consistency contract、SLO、system tables、failure recovery 与 rollout runbook |

## 验证矩阵与发布门槛

### 正确性测试

- differential：同一 source snapshot 下，DT/MV 结果与原定义 SQL 全量执行逐行一致；
- changelog：INSERT、UPDATE_BEFORE/AFTER、DELETE、乱序、重复、迟到与 partition rebalance；
- SQL：filter/project/join/outer join/aggregate/distinct/window/subquery/CTE/null/type/decimal；
- schema：add/rename/drop/widen/narrow、field ID 演进、policy change；
- rewrite：谓词补偿、aggregate roll-up、partial rewrite、time travel、RLS/masking/tenant 隔离。

### 故障注入点

1. claim 后、plan 前崩溃；
2. 部分 worker fragment 完成后崩溃；
3. target data files 写完但 snapshot 未 commit；
4. target commit 成功但 metadata generation 未 publish；
5. metadata publish 后、source acknowledgement 前崩溃；
6. write/commit 过程中 lease 过期；
7. 旧 leader GC pause 后恢复；
8. refresh 与 REDEFINE/DROP/SUSPEND 并发；
9. source snapshot expiration/schema change；
10. catalog commit conflict、object store eventual visibility、worker 重试。

每个点都要重复执行并验证三个条件：不丢数、不产生重复可见结果、checkpoint 不越过 active generation。

### 扩展性测试

- 10 万 derived objects、100 万 dependency edges；
- 1 万同时 overdue，验证 jitter、fairness、admission 与 queue recovery；
- 1000 CDC partitions、多 source join 的 position vector；
- TB 级 keyed aggregate state 的 checkpoint/rescale；
- coordinator 横向扩缩与 leader failover；
- history/metrics 保留期内的写放大、索引与查询延迟。

### 建议发布 SLO

| 指标 | Preview 门槛 |
|---|---|
| refresh correctness | failpoint/differential suite 0 mismatch |
| stale leader commit | 0 次被接受 |
| checkpoint/generation invariant | 0 次违反 |
| scheduler durability | coordinator 全部重启后 intent 不丢失 |
| tenant isolation | 同名与 policy differential suite 0 越界 |
| within-target ratio | 可按对象/tenant/warehouse量化并告警；具体目标由产品等级定义 |
| rewrite safety | 无 proof/coverage 时 100% fail closed |
| operational explainability | 每次 full fallback、skip、failure、rewrite rejection 均有稳定 reason code |

## 建议立即创建的工程任务

按依赖顺序，第一批任务应是：

1. `DT-001`：用 production reachability test 证明并补齐 bootstrap wiring；在此之前 feature flag off。
2. `DT-002`：禁止 dispatch-as-success，定义 `RefreshExecutionHandle/CommittedOutcome`。
3. `DT-003`：设计并迁移 stable object ID、qualified key、definition/generation schema。
4. `DT-004`：实现 target generation replace 与 fenced catalog commit SPI。
5. `DT-005`：移除 source passthrough E2E，建立真实 physical target test fixture。
6. `DT-006`：统一 CDC source position vector，复用 streaming 2PC。
7. `DT-007`：将 scheduler 改为 persistent queue，修复 retry 后永久停刷问题。
8. `DT-008`：versioned/persistent DAG 与 pipeline source cut。
9. `MV-001`：production 关闭当前 digest/structural rewrite，建立 shadow rewrite harness。
10. `MV-002`：实现真实 Calcite materialization + security/snapshot guard + candidate index。

## 最终架构判断

Pangrova 不需要分别建设一套“动态表系统”和一套“物化视图系统”。两者真正困难且必须一致的部分——对象版本、依赖图、source checkpoint、分布式增量状态、原子物化提交、HA fencing、查询可见 generation、schema/security coverage——完全相同。分开演进只会继续制造模型、状态机和恢复语义的分叉。

正确的产品分层是：

```text
Product API:       MV / Dynamic Table / Continuous Query
Policy layer:      manual / event / target-lag / downstream / streaming
Common kernel:     definition + DAG + scheduler + refresh + generation commit
Execution layer:   Java control plane + C++ distributed data plane
Storage layer:     connector capabilities + atomic snapshots/change log
```

在这个分层下，第一优先级不是增加更多 SQL 语法或支持更多增量算子，而是先建立“一个 refresh 到底何时算成功、成功后哪个 generation 可见、checkpoint 与 source/target 如何原子对应、旧 leader 如何被拒绝”这四个基础契约。只要这四点没有闭环，大规模并行只会更快地产生不可恢复的错误结果。

## 代码证据索引

以下位置是本审查的主要本地证据入口，行号以审查时工作树为准：

- 历史审计的生产 task/bootstrap 位置：`pangrova-coordinator/.../bootstrap/QueryExecutionBootstrap.java:234-321`；另一个当时检查的 `QueryExecutionWiring.java:191-292` 副本已因无调用方删除。
- Planner wiring：`pangrova-coordinator/.../bootstrap/PlannerWiring.java:58-69`
- orphan baseline：`pangrova-testing/.../arch/BootstrapReachabilityTest.java:77-100`
- E2E 透传物化：`pangrova-testing/.../SqlE2ETestBase.java:1990-2054, 2118-2157`
- native dispatch-as-success：`pangrova-coordinator/.../refresh/pipeline/RefreshCoordinator.java:170-185`
- dispatch result contract：`pangrova-coordinator/.../query/execution/dispatch/DispatchResult.java:21-35`
- APPEND writer：`pangrova-coordinator/.../dynamictable/write/DynamicTableWriter.java:75-90`
- scheduler：`pangrova-coordinator/.../dynamictable/schedule/RefreshScheduler.java:52, 140-180, 260-300`
- lock/fencing：`pangrova-coordinator/.../refresh/pipeline/RefreshLockCoordinator.java:23-134`
- simple-name DDL：`pangrova-coordinator/.../dynamictable/DynamicTableDdlExecutor.java:456-459`
- DT schema/state migration：`pangrova-metadata/src/main/resources/db/migration/V1__initial_schema.sql:330-348`
- CDC checkpoint：`pangrova-coordinator/.../dynamictable/detect/ChangeResult.java:96-103`
- merge-on-read hint：`pangrova-planner/.../PangrovaPlanner.java:145-172`、`DynamicTablePlanningSupport.java:44-67`
- MV matcher/replace：`pangrova-planner/.../mv/StructuralSubsumptionMatcher.java`、`DigestPlanRewriter.java:37-61`、`MaterializedViewTableRef.java:34-100`
- MV persistence/recovery：`pangrova-coordinator/.../metadata/MetadataBootstrap.java:80-90`、`MaterializedViewRecovery.java:44-75`、`pangrova-metadata/.../JdbcMaterializedViewPersistenceHook.java`
