# Pangrova 动态表与物化视图深度调研

调研日期：2026-09-30。状态：研究与目标设计输入，非已实现能力说明。

本文回答两个问题：动态表和物化视图为什么值得成为数据库的一等对象；成熟系统如何把它们实现为可解释、可恢复、可运营的能力。调研中的采用决策已经落地为统一物化内核，当前实现以 [查询优化架构](../architecture/query-optimization.md) 和 [Dynamic Table 刷新语义](../architecture/dynamic-table-manual-full-refresh.md) 为准。

结论是：**物化的价值是把可重复的派生计算变成可复用的数据产品；动态表进一步把依赖、刷新、新鲜度和恢复交给系统。Pangrova 应共享一个版本化物化内核，先完成正确的分布式全量刷新，再逐步增加增量维护和查询改写。** 产品名称不能替代一致性契约。

## 1 研究方法与证据边界

- 以厂商官方文档、作者论文和开源规范为事实来源。选型依据是代表不同实现路线，不是市场份额或速度排名。
- 官方文档说明对外行为；论文说明特定发表版本的架构。未公开的存储或调度细节保持未知，不用产品行为推测内部源码。
- 动态网页代表本次访问所见。特别检查了 Snowflake 2026-07-30 ADAPTIVE GA 公告和 Databricks 2026 年 Enzyme 论文，避免沿用旧版刷新结论。
- 当前仓库 POM 为 Calcite 1.42.0、Iceberg 1.11.0、Paimon 1.4.1；Paimon 1.4 文档页显示 1.4.2，因此概念可参考，具体 API 必须用仓库依赖版本编译和集成验证。
- 下文“事实”指外部来源直接支持的行为；“对 Pangrova 的判断”与成本例子是本项目的推导，不是厂商性能承诺。

## 2 它们真实存在的意义

### 2.1 物化视图保存的究竟是什么

对于查询定义 Q、基表版本向量 V，物化结果应满足：

```text
M(g) = Q(B1[V1], B2[V2], ..., Bn[Vn])
g = (定义版本, 源版本向量, 输出 schema, 物理结果版本)
```

普通视图保存查询定义，读取时计算；物化视图保存已经计算出来的结果。可靠的 MV 还要保存“这个结果由哪些输入版本、哪个定义计算而来”。只有结果而没有 provenance，无法解释陈旧程度，也无法可靠增量、回放和查询改写。

MV 不必总是最新。PostgreSQL 的手动 REFRESH、StarRocks 的异步刷新、Materialize 的持续维护，拥有不同的新鲜度与维护契约；不能把某一家“读取总是当前”的行为写成 MV 的通用定义。[PostgreSQL REFRESH](https://www.postgresql.org/docs/18/sql-refreshmaterializedview.html)，[StarRocks 异步 MV](https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/)，[Materialize 架构](https://materialize.com/blog/materialize-architecture/)

### 2.2 动态表增加了什么

Snowflake 式 DT 是声明式派生表：用户定义 SELECT 与目标新鲜度，系统管理依赖和刷新。它的核心价值包括中间层复用、管道编排、资源安排、失败恢复与新鲜度解释；FULL 刷新也可以是 DT，DT 不等于 CDC 或增量算法。[Snowflake 刷新模式](https://docs.snowflake.com/en/user-guide/dynamic-tables/refresh-modes)，[Target lag](https://docs.snowflake.com/en/user-guide/dynamic-tables/target-lag)

“动态表”存在术语差异：Flink 的 dynamic table 是流与持续变化关系之间的模型，其 continuous query 持续更新结果表；Snowflake DT 是具备托管刷新产品接口的持久对象。本文讨论 Pangrova 应提供的后一种产品能力，增量语义参考前一种关系模型。[Flink Dynamic Tables](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/concepts/sql-table-concepts/dynamic_tables/)

### 2.3 业务价值如何转成工程价值

| 真实需求 | 物化带来的改变 | 系统必须承担的责任 |
|---|---|---|
| 大量 BI 查询反复计算同一聚合或 JOIN | 共享中间结果，降低查询扫描与尾延迟 | 正确的结果复用、统计和版本绑定 |
| bronze 到 silver 到 gold 管道 | 用 SELECT 描述依赖，减少手写 MERGE 与定时脚本 | DAG、刷新切面、失败传播和重建 |
| 数据产品要解释“更新到哪里” | 暴露输入版本与数据覆盖时间 | durable provenance 和系统表 |
| 维度变更需要重算历史关联 | 明确维护所有输入变化的结果 | 双侧 JOIN delta 或受影响键重算 |
| 删除、纠错、聚合回撤 | 结果等价于当前源状态下的原 SQL | before image、retract、target delete |
| 计算昂贵但读取频繁的确定性特征 | 预计算并共享结果 | 函数版本、成本预算和定义重建 |
| 多团队复用派生数据 | 逻辑对象稳定，物理版本可替换 | 权限、生命周期和兼容 schema |

物化不保证一切查询都加速。输出接近原表、读取很少、变化很大、JOIN 扇出很高或者状态维护很昂贵时，维护成本可能超过节省。大量小文件也会把“少算一些行”的收益抵消。

### 2.4 经济模型

以下是设计估算，不是厂商 benchmark。给定同一观察窗口：

```text
净收益 = 被替代的查询计算成本
       - 刷新计算成本 - 物化读取成本 - 存储成本
       - checkpoint/compaction/小文件/运维成本
```

假设 1000 次查询每次逻辑扫描 1 TB，物化后每次读 10 MB，窗口内刷新扫描合计 20 GB，那么重复扫描从约 1000 TB 降到约 30 GB；还需计入首次 1 TB 计算、输出存储与维护。这个例子只说明复用的数量级，实际收益必须测量读放大、JOIN、CPU、I/O、压缩和缓存。

增量优势取决于**受影响结果**，不能只看输入变化比例。修改一条维度记录可能影响亿级事实行；删除一个 MIN 值可能需要重新搜索整个 group。应比较 full、partition replace、affected-key recompute 与 row delta 四种实际计划。

### 2.5 不同对象的选择

| 对象 | 主目的 | 读取契约 | 维护契约 |
|---|---|---|---|
| 普通 VIEW | 封装 SQL | 按查询绑定的源版本计算 | 无独立结果维护 |
| CTAS/普通表 | 保存一次计算或人工维护的数据 | 当前提交的表内容 | 系统不自动保证它等于某个 SELECT |
| 查询结果缓存 | 重复请求复用 | 依缓存有效性和安全上下文 | 通常按请求/计划缓存，无独立管道生命周期 |
| MV | 持久复用 SELECT 结果 | 最近提交结果或厂商定义的补偿结果 | 手动、事件、周期或同步维护 |
| 托管 DT | 声明式数据产品/管道 | 最近完成且有来源证明的结果 | 依赖感知的新鲜度调度 |
| 流式表/日志 | 接收变化与事件处理 | 依源和流式语义 | offset、watermark、checkpoint |

对于不保留源历史、只允许消费一次、带不可重复副作用的流程，不能把任意 full rebuild 当成通用恢复方式。先落可重放基础表，再做物化，通常更适合 Pangrova 的 lakehouse 路线。Databricks 也明确区分 MV 的批等价刷新与一次性处理的 streaming table。[Databricks 增量刷新语义](https://docs.databricks.com/aws/en/ldp/incremental-refresh)

## 3 代表性系统的真实架构

### 3.1 Snowflake DT 和 MV

**事实。** DT 论文公开了 compiler、catalog、scheduler、transaction manager、warehouse 的协作：DDL log 驱动依赖图；调度给定 DT 和 data timestamp；编译器展开定义并生成 full/delta 计划；事务机制发布结果与来源进度。2025 年论文的 Delayed View Semantics 把异步派生结果的来源纳入隔离讨论。[Snowflake SIGMOD 2025 论文](https://arxiv.org/html/2504.10438v1)

DT 管道一次 refresh cycle 使用协调后的输入时间点，上游失败阻止下游前进；临时 ad-hoc SELECT 读取各对象最新已提交版本并不自动成为整条 DAG 的共同快照。[数据一致性与边界](https://docs.snowflake.com/en/user-guide/dynamic-tables/data-consistency)

TARGET_LAG 是新鲜度目标，调度器按依赖安排刷新，同一表不并发刷新；DOWNSTREAM 从消费方需求推导刷新时机。它不是承诺每隔固定时间执行一次。[Target lag](https://docs.snowflake.com/en/user-guide/dynamic-tables/target-lag)

2026-07-30 ADAPTIVE 已 GA，可在大量变化使增量更贵时重初始化，再恢复增量。AUTO 和 ADAPTIVE 不能混为“每批都自动选择”的同一个策略；产品细节应以当前模式文档为准。[GA 公告](https://docs.snowflake.com/en/release-notes/2026/other/2026-07-30-dynamic-tables-adaptive-refresh-mode-ga)

Snowflake MV 支持透明加速，但目前只允许单基表、不能 JOIN；读取陈旧物化部分时会读取新源数据或更新结果以保证当前结果。这是其 MV 特定契约，不能推出 DT 都会自动 rewrite 或读最新。[Snowflake MV](https://docs.snowflake.com/en/user-guide/views-materialized)

**对 Pangrova 的判断。** 最值得采用的是版本化来源、native 内部刷新命令、DAG 新鲜度与事务发布。Snowflake 共享事务时间线不能直接复制到独立 Iceberg/Paimon catalogs。

### 3.2 Databricks MV 和 Enzyme

**事实。** 2026 Enzyme 论文公开六段架构：normalization → fingerprinting → decomposition → incremental plan generation → costing → execution。它利用表格式能力生成关系 delta、受影响 group 重算和分区覆盖等方案，使用已有批执行基础设施，并将来源信息纳入提交。论文描述的架构不是对 Pangrova 的性能预测。[Enzyme 论文](https://arxiv.org/html/2603.27775v1)

官方文档说明增量与 full 输出应符合批查询语义，当前刷新使用 serverless pipeline；源类型和部分算子对 row tracking 等能力有要求。REFRESH POLICY 提供 AUTO、INCREMENTAL、INCREMENTAL STRICT、FULL；严格模式在无法增量时失败，避免不可接受的全量成本。[Databricks 增量刷新](https://docs.databricks.com/aws/en/ldp/incremental-refresh)

**对 Pangrova 的判断。** 这是最贴近现有 Java/Calcite + Native/Velox 批执行引擎的路线。先生成普通批计划完成 affected-key/partition 重算，比立即引入永久驻留的通用流式状态引擎更容易贯通。语义指纹与优化后 plan digest 应分开，避免升级造成全仓无必要重建。

### 3.3 Materialize

**事实。** 公开架构区分 Adapter、Storage/Persist、Compute。Persist 表示带时间与正负 multiplicity 的持久集合，Compute 将 IR 编译为 Timely/Differential Dataflow；frontier 确定可读时间与历史压缩范围，operator 将输入差分转成输出差分。资料来自 2023 年架构文章，只用于机制说明，不代表今天部署拓扑完全未变。[Materialize 架构](https://materialize.com/blog/materialize-architecture/)

其一致性说明将差分维护、时间线与查询读取时机结合讨论；持续计算仍需明确“某时间的结果是否完整”，不能把最近收到的记录当成已完成一致结果。[Materialize 一致性](https://materialize.com/blog/strong-consistency-in-materialize/)

**对 Pangrova 的判断。** 应采用 signed delta、typed key 与 frontier 的语义；持续 state backend 是高投入能力。当前 Velox 普通 batch aggregation 不会因为添加一个 delta 标记就自动具备 differential 状态、重放和扩缩容能力。

### 3.4 RisingWave

**事实。** 官方 2026 架构说明包含 SQL frontend、streaming actors/fragments、meta、Hummock state store 和 compactor。Barrier 按 epoch 协调多输入状态刷出，meta 收齐确认后提交 epoch；失败从提交的 epoch/offset 恢复。状态落对象存储并通过本地缓存、LSM 和远程 compaction 管理。[RisingWave 官方架构](https://risingwave.com/blog/architecture-distributed-streaming-database/)

**对 Pangrova 的判断。** 可借鉴 state 与输出同 epoch 的可见性和 rescale barrier；不能只实现 checkpoint API 而省略多输入对齐、持久状态 manifest 和恢复执行。若未来确需亚秒持续维护，应单独完成这整组能力。

### 3.5 StarRocks

**事实。** 异步 MV 可定义在多表、外部 catalog、已有 MV 上，支持手动/异步刷新、分区刷新与透明 rewrite。默认 catalog 下会排除与基表不一致的 MV；外部源与具体 rewrite 支持受 connector 和版本限制。同步 rollup 与异步 MV 的覆盖范围不同。[StarRocks 异步 MV](https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/)，[数据建模和分区刷新](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/data_modeling_with_materialized_views/)

**对 Pangrova 的判断。** 分区映射是 lakehouse 中非常实用的增量边界；需要证明 query 不跨被替换分区。自动改写应在 freshness guard、语义证明和真实 catalog scan 之后开放。

### 3.6 BigQuery

**事实。** BigQuery 可组合物化结果和新数据；append 与修改的处理不同。基表 update/delete 或 JOIN 右侧变化使某些增量路径不再可用，转回原查询；对齐分区允许逐分区处理。Smart tuning 要求物化结果覆盖所读表、列和行，并有明确不支持范围。[BigQuery MV 使用](https://docs.cloud.google.com/bigquery/docs/materialized-views-use)

**对 Pangrova 的判断。** “安全不用 MV”是正确性机制。原查询 fallback 与刷新 full rebuild 是两个不同操作，成本、触发者和状态都应分别解释。MOR 补偿计划要证明 bag 语义及快照覆盖，不能简单 UNION ALL。

### 3.7 ClickHouse 的两类 MV

**事实。** Incremental MV 在源 INSERT block 上运行，JOIN 右侧变化本身不会触发视图；它不能被当作任意 UPDATE/DELETE 的通用 IVM。聚合中间状态由目标引擎维护。[ClickHouse Incremental MV](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view)

Refreshable MV 周期性重跑定义，默认替换输出，也可显式 APPEND 保存快照；可声明依赖并观察 system.view_refreshes。[ClickHouse Refreshable MV](https://clickhouse.com/docs/concepts/features/materialized-views/refreshable-materialized-view)

**对 Pangrova 的判断。** append-only 维护可以先交付，但必须显式限定源变化与算子。某次写入可以追加，并不证明某个 MV 永久具备 append-only 语义。

### 3.8 Oracle 和 PostgreSQL 的基础经验

**事实。** Oracle real-time MV 可在 query rewrite 时通过 on-query computation 补偿陈旧结果，受 freshness、嵌套依赖和 rewrite integrity 控制。[Oracle Advanced MV](https://docs.oracle.com/en/database/oracle/oracle-database/21/dwhsg/advanced-materialized-views.html)

PostgreSQL REFRESH 重新执行定义；CONCURRENTLY 避免阻塞读取，要求合适的 UNIQUE index，同一 MV 仍只能同时执行一个 refresh。它不等于源 CDC 的增量计算。[PostgreSQL REFRESH](https://www.postgresql.org/docs/18/sql-refreshmaterializedview.html)，[PostgreSQL 实现](https://doxygen.postgresql.org/matview_8c.html)

**对 Pangrova 的判断。** 先发布完整新结果、再切换读取，是可信 MVP 的基础。查询可读期间后台更新与“增量读取源数据”是独立能力，不应混称。

## 4 架构路线对照

以下表格是对上述事实的研究归纳；“适合度”是 Pangrova 的架构判断。

| 路线 | 代表系统 | 执行与状态 | 主要优势 | 成本与适合度 |
|---|---|---|---|---|
| 批引擎上编译增量关系计划 | Snowflake、Databricks | full/delta/重算，存储版本承担持久结果 | 复用现有 engine，覆盖复杂 SQL | 最适合作为 Pangrova 主线 |
| 常驻 differential dataflow | Materialize | 正负差分、arrangement、frontier | 低延迟、多输入持续维护 | 要完整状态引擎，后续独立里程碑 |
| barrier 与持久分布式 state | RisingWave、Flink 机制 | keyed state、epoch checkpoint | 恢复与扩缩可系统化 | Native state、存储、barrier 均缺一不可 |
| 分区/结果替换与透明改写 | StarRocks、BigQuery | partition coverage、snapshot guard | OLAP 加速，粗粒度增量实用 | Pangrova 第二阶段优先 |
| INSERT trigger 与状态合并 | ClickHouse incremental | 输入 block 与 target engine | 单调写入高效 | 有边界子集，不能承诺通用 CDC |
| 全量结果重建 | PostgreSQL、refreshable MV | 完整结果和可读性切换 | 语义简单，容易验证 | 正确闭环首阶段；大表成本需预算 |

## 5 Pangrova 必须补齐的技术基础

### 5.1 湖表格式能力不等于增量 SQL 能力

Iceberg 用原子 metadata pointer 更新发布一致 snapshot；但 snapshot ID 不是按数值递增的时间，规范参考实现用 UUID 派生伪随机 ID。必须用 connector 的 ancestry/ref/sequence 解释版本先后。[Iceberg Reliability](https://iceberg.apache.org/docs/latest/reliability/)，[Iceberg Specification](https://iceberg.apache.org/spec/)

Paimon changelog-producer=none 的 merged changes 不能天然给出完整旧值。input 要求输入本身完整；lookup 与 full-compaction 有不同生成时机和成本。因此支持 Paimon 并不意味着任意表配置都支持可回撤聚合。[Paimon 1.4 Changelog Producer](https://paimon.apache.org/docs/1.4/primary-key-table/changelog-producer/)

**设计推导。** capability 必须绑定 table incarnation、ref、配置、schema 和请求变化区间，并产出包含 before image、稳定身份、重放边界与 retention 的证书。一个 connector-wide boolean 只能作为粗筛。

### 5.2 查询改写与刷新增量是两个不同证明

Calcite 提供 substitution 和 MaterializedViewRule 等规则，可处理谓词补偿、JOIN、聚合 rollup、partial/union rewrite；需要约束元数据，也有复杂视图枚举和候选筛选限制。[Calcite MV](https://calcite.apache.org/docs/materialized_views.html)

**设计推导。** “这个定义可增量维护”不能推出“这个 MV 可回答另一条 SQL”。正式 rewrite 之外，还需 tenant、policy、输出类型与 query snapshot 检查。候选索引只能筛选，不能成为等价证明。

### 5.3 Exactly once 是可见效果契约

Flink 将 fault recovery 建立在一致 state checkpoint 与可重放输入上；checkpoint 不代替 sink 的事务/幂等语义。[Flink Stateful Processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)

**设计推导。** Pangrova 必须联结 source position、operator state、target receipt 和 publication。重复执行可以存在，但重复或未完成结果不能对用户可见。湖表 commit 与控制库 transaction 不共享天然原子性，设计需要可重放 receipt 与单一可见性发布点。

## 6 采用决策

1. 用统一 Materialization 领域承载 MV/DT 的不可变定义、来源、结果 generation、attempt、publish 与恢复；两个产品保留不同默认刷新策略和用户语义。
2. 主线采用 Calcite 编译 full/partition/affected-key/delta 关系计划，走现有 v2 → Worker/JNI → Native → distributed write。Coordinator 仅保存控制元数据。
3. 首次发布先支持 full rebuild + 原子逻辑 generation 发布；复杂 SQL 可以使用 full，但仍受真实 native operator 能力限制。
4. 第二阶段优先分区替换和受影响键重算；持久 signed-state IVM 与 MOR 是独立的后续能力，不以 hint 或 JVM HashMap 宣称实现。
5. 增量模式、source change 方式、触发策略、读取一致性分成四个维度。严格增量失败时明确失败；AUTO 的 full 是重新编译和执行完整计划，不是异常后继续返回 delta。
6. 默认透明 rewrite 必须保持原查询 snapshot 语义。无法证明 source vector 等价时使用原计划；允许陈旧读取必须由产品/API 明确表达。
7. 每个工作包同时列出 SPI、实现、装配、调用、持久化、验证和恢复，不允许只新增接口或只让测试 fixture 接通。

本文只保留外部资料与设计判断；当前代码事实、提交协议和发布门槛以 [查询优化架构](../architecture/query-optimization.md) 为准。
