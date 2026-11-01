# pangrova-metadata 深度 Review（历史基线，2026-10-31）

- 评审对象：`pangrova-metadata/`（仓库内模块）
- 评审日期：2026-10-31
- 当前分支：`codex/ai-native-semantic-closure`（工作区干净）
- 代码基线：296 个主代码文件、38,959 行；141 个测试文件、26,666 行、852 个 `@Test`；92 个 Flyway 迁移；114 张表
- 评审方式：全量静态阅读（主代码逐包、逐文件）+ 跨模块 import/表/SQL 扫描 + 与既有架构文档对照；未执行完整构建与运行时压测

> **修订说明（2026-10-31）**：本文档是历史静态 review。§6–§7 的目标架构/拆分计划已被 [metadata-governance-and-split-plan-2026-10-31.md](./metadata-governance-and-split-plan-2026-10-31.md) 取代；新项目无外部部署/生产数据，不保留旧 API/旧表/旧迁移/兼容层。本文档保留问题证据，目标命名与迁移假设以治理计划为准。

**目录**：§0 执行摘要 · §1 评审范围 · §2 模块现状 · §3 业务域与全量文件清单 · §4 横切审计 · §5 问题清单 · §6 目标边界（摘要） · §7 验收标准 · 附录 B/C/D

**目标架构与实施计划**：[metadata-governance-and-split-plan-2026-10-31.md](./metadata-governance-and-split-plan-2026-10-31.md)

**阅读路径**：决策者看 §0/§5 与治理计划；代码评审看 §3/§4；实施者看治理计划 §5。

---

## 0. 执行摘要

### 0.1 一句话结论

`pangrova-metadata` 目前不是一个“元数据模块”，而是一个**共享 PostgreSQL schema + 控制面持久化聚合模块**：它同时承载 catalog、表解析、缓存、同步、表控制面、写提交日志、执行锚点、租约、节点注册、索引、artifact、物化、CDC、文件空间、租户、质量规则、Feature/Semantic/Evaluation 等至少 12 个事实域。它“大”本身不是问题，问题是**所有权、失败半径和迁移边界没有拆开**：任一域的 schema 变更或发布都可能影响查询恢复、索引构建、流式摄入和 AI 元数据。

### 0.2 关键数字

| 指标 | 数值 | 说明 |
|---|---:|---|
| 主代码文件 / 行数 | 296 / 38,959 | 其中约 43 个 interface、168 个 record、169 个 class、36 个 enum（含嵌套类型） |
| test 源码文件 / 行数 / `@Test` | 141（134 个 `*Test.java` + 7 个 fixture/helper）/ 26,666 / 852 | 但默认 `excludedGroups=integration`，15 个 integration 测试默认不跑 |
| Flyway 迁移 | 92 | 版本 V1–V101，缺 V11–13、V16、V76–77、V79–81 |
| 数据库表 | 114 | 含 `public` 与 `materialization` 两个 schema |
| 单文件 > 500 行 | 12 个 | 2 个 > 800 行（907、827） |
| `UnsupportedOperationException` | 16 处 | 多数是 SPI/default 方法，未标 `@Experimental` |
| 带 SPI 注解的文件 | 3 个（共 296 个） | `@Stable`/`@Experimental`/`@Internal` 基本缺位 |
| 构造函数内 `CREATE TABLE IF NOT EXISTS` | 8 处 / 4 个类 | 与 Flyway 形成双 DDL 源 |
| 跨模块同名类 | 4 个 | `L2CacheStore`、`InMemoryL2CacheStore`、`JdbcCdcRegistrationStore`、`JdbcWatermarkStore` |
| Coordinator 对 metadata 的 import | 1,114 处 | 说明 Coordinator 直接依赖 metadata 内部包，而非稳定 facade |

### 0.3 最高风险（按严重度）

| ID | 风险 | 证据 | 影响 |
|---|---|---|---|
| P0-01 | 依赖/所有权失守：metadata 依赖 `pangrova-materialization`（`architecture.md` 当前允许），且在自身实现 index/streaming/governance/tenant/materialization 的 JDBC 适配器；metadata 未纳入依赖 gate | `pangrova-metadata/pom.xml:29`；`materialization/JdbcMaterializationRepository.java:15-40`；各域表映射见 §4.4.1 | 问题不是单条 POM 越界，而是 adapter 所有权、失败半径和依赖规则未执行；详细边界见治理计划 |
| P0-02 | 默认使用内存控制面：`CatalogRegistry` 构造器直接装配 `InMemoryTableControlStore` / `InMemoryTableControlStatisticsStore` / `InMemoryTableControlGovernanceBindingStore` | `catalog/CatalogRegistry.java:63-64, 87-92` | 任何未显式调用 `setTableControlService` 的嵌入方式都会静默丢失 ownership 决策；文档明确 in-memory 仅限测试/无元数据模式 |
| P0-03 | 事件可静默丢弃：`MetadataEventDispatcher` 队列满时只 `WARN` 并丢弃事件 | `event/MetadataEventDispatcher.java:41-44` | 缓存失效事件丢失 → 跨 coordinator 读到旧 schema/旧表；`eventsDropped` 指标无人消费 |
| P0-04 | 缓存无界 + 双缓存：`TieredMetadataCache.tableVersions` 只增不减；`DefaultMetadataService` 另有独立的 `resolvedTableCache` | `cache/TieredMetadataCache.java:84, 175`；`service/DefaultMetadataService.java:80-130, 228-261` | 长跑 coordinator 内存增长；两条缓存互不知情，失效语义分裂 |
| P0-05 | 双 DDL + 迁移债：4 个 Store 在构造函数建表；92 个迁移/114 表混在一条 history | 8 处 `CREATE TABLE IF NOT EXISTS`；§4.4.2 | 新项目无外部部署，旧迁移全部删除、重建 per-owner baseline；构造 DDL 仍会造成启动/维护风险，必须删除 |
| P0-06 | COPY 写入意图与 claim 未接线：`JdbcWriteCommitJournal.recordIntent` 要求 `copy_file_claims` 行，但 `JdbcCopyFileClaimStore` 被标 `@DeadCode` 且无生产注入 | `coordination/JdbcWriteCommitJournal.java:248`；`coordination/JdbcCopyFileClaimStore.java:19` | COPY 写意图在缺少 claim 行时无法记录，恢复/幂等链路断裂 |
| P0-07 | catalog revision 栅栏无更新路径：`saveCatalogDescriptor` 要求 `revision = current + 1`，唯一生产调用方 `register` 只创建新 catalog（revision 1），没有更新/自增路径 | `store/JdbcMetadataStore.java:102-133`；`catalog/CatalogRegistry.java:115-133` | catalog 更新能力实际不可用；未来一旦启用会直接抛 `MetastoreException` |
| P0-08 | watermark/offset 可回退：`JdbcWatermarkStore`、`JdbcStreamOffsetStore` 无条件覆盖 | `streaming/JdbcWatermarkStore.java:31-34`；`streaming/JdbcStreamOffsetStore.java:54-57` | 旧写/重试可把 watermark 或 committed offset 写回更小值，CDC 语义被破坏 |
| P0-09 | 事务内做外部 I/O：materialization 的 `collectGeneration` 在 `inTransaction` 内执行 `physicalDrop.run()` | `materialization/JdbcMaterializationRepository.java:565-604`（583） | 长事务持锁做物理删除，违反“锁内不做 I/O”；失败时事务与外部副作用不一致 |
| P0-10 | 复杂度信号：907 行 `JdbcMaterializationRepository`、827 行 `TableControlService`、768 行 `JdbcCopySourceReadAdmissions`、766 行 `JdbcManagedSourceCatalog`、721 行 `JdbcWriteCommitJournal` | `wc -l` 见 §4.2.1 | 12 个 >500 是复杂度信号；2 个 >800 触发 code-standards review 诊断；500 是内部拆分目标，不是 build gate |

> 说明：P0 表示“发布前应关闭”。P0-06/P0-07 的运行时影响取决于 COPY / catalog 更新是否启用；P0-10 是结构债，但会放大其余风险的修复成本。

### 0.4 建议路线（结论先行）

1. **不要**立刻把 metadata 拆成 10+ 个 Maven module。现有 `metadata-center-redesign.md` 的判断仍然成立：第一阶段应保持单一 artifact，先把**包边界、schema 所有权、失败半径**拆清楚。
2. **先做 Phase 0 止血**（1–2 周）：去掉内存默认、事件不丢、缓存有界、构造函数 DDL、COPY claim、revision 栅栏、watermark 单调、事务内 I/O。
3. **再做 Phase 1 包内重构**（3–6 周）：把 `MetadataService`、`TableControlService`、`JdbcMaterializationRepository`、`JdbcWriteCommitJournal`、`JdbcIndexDescriptorStore`、`JdbcArtifactBuildStore` 拆成 <500 行的协作类；统一 JDBC/错误/JSON/事务模板。
4. **Phase 2 按 record owner 迁出**：query.transaction、coordinator.coordination、indexing.control（含 artifact）、streaming.control、materialization.control、governance、security、semantic、`pangrova-ai`；metadata 只保留技术事实。详见治理计划 §5。
5. **Phase 3 重建 schema**：新项目不保留 V1–V101；删除旧迁移/旧表，按 owner 建 per-owner baseline + 独立 Flyway history；不做兼容/dual-write；禁止构造 DDL。

---

## 1. 评审范围与方法

### 1.1 范围

- 主代码：`pangrova-metadata/src/main/java` 全部 296 个文件，按包逐个阅读；每个文件的职责见 §3 的完整清单。
- 测试：141 个 test 源码文件（含 7 个 fixture/helper；134 个 `*Test.java`），按包统计数量、行数、`@Test` 和 integration 标签；未逐行评审所有测试实现。
- SQL：92 个迁移脚本，解析 `CREATE TABLE` 与 `ALTER TABLE`，建立“表 → 迁移 → 包”的归属图。
- 跨模块：扫描 `pangrova-coordinator`、`pangrova-testing`、`pangrova-connector` 对 metadata 的 import；扫描根 POM 的模块清单与 `ModuleDependencyPropertyTest` 的约束。
- 对照文档：`docs/architecture/metadata.md`、`metadata-center-redesign.md`、`metadata-target-architecture-research.md`、`long-term-modular-architecture.md`、`.pangrova/rules/*`。

### 1.2 方法

```text
find/wc 统计规模
grep import 统计跨模块耦合
grep/解析 SQL 建立表归属
逐包阅读主代码（4 个并行只读评审 + 人工复核关键文件）
与架构规则/目标文档做差异比对
```

### 1.3 证据边界

- 本报告中的“问题”均来自静态代码证据；涉及运行时行为的结论会注明“静态证据”或“需运行验证”。
- 未执行 `mvn test`、E2E、Testcontainers 或数据库迁移，因此不宣称任何测试通过。
- 新项目无外部部署/生产数据；旧迁移不保留，按 per-owner baseline 重建，因此不存在生产 upgrade 兼容验证。

---

## 2. 模块现状总览

### 2.1 包级规模与业务域

| 包 | 文件 | 行数 | 承载业务 | 目标归属（建议） |
|---|---:|---:|---|---|
| `adapter` | 7 | 356 | 外部 catalog 联邦 SPI/适配 | metadata-core（联邦 adapter） |
| `ai` | 1 | 51 | AI 基础设施 Store 装配 | 删除/移入 coordinator bootstrap |
| `artifact` | 7 | 246 | artifact 发布视图与类型路由 | `indexing.control` |
| `artifact/build` | 14 | 1,052 | artifact 构建控制面（job/page/lease/graph） | `indexing.control` |
| `artifact/build/template` | 2 | 86 | stage 执行模板 | `indexing.control` |
| `artifact/contract` | 8 | 343 | artifact 格式/类型/依赖契约 | `indexing.control` / `spi` |
| `artifact/jdbc` | 14 | 2,690 | 按类型原子发布、授权/fence、exchange 证据 | `indexing.control` |
| `artifact/jdbc/build` | 13 | 2,731 | PostgreSQL 构建状态机 | `indexing.control` |
| `artifact/jdbc/feature` | 1 | 80 | catalog 导出链接 | `indexing.control` |
| `cache` | 11 | 1,180 | L1/L2 缓存、失效轮询 | metadata-core |
| `catalog` | 12 | 919 | catalog 生命周期/健康/handle | metadata-core |
| `checkpoint` | 1 | 111 | checkpoint JDBC | `query.transaction` / `streaming` |
| `control` | 34 | 3,574 | 表身份/所有权/准入/治理绑定/统计/维护 | metadata-core（`control` 子包） |
| `coordination` | 16 | 4,166 | 写日志/锚点/租约/注册/查询日志/血缘 outbox | `query.transaction` / `coordinator.coordination` |
| `dependency` | 10 | 1,108 | 对象依赖图 | metadata-core |
| `evaluation` | 15 | 1,434 | 评估数据集/run/指标/发布门禁 | `pangrova-ai` |
| `event` | 3 | 194 | 元数据事件与分发 | metadata-core |
| `exception` | 7 | 190 | 元数据异常 | metadata-core |
| `exception/catalog` | 6 | 120 | catalog 异常 | metadata-core |
| `feature` | 2 | 616 | Feature View / Training Dataset catalog | `pangrova-ai` |
| `file` | 4 | 870 | Volume/Stage 命名空间、目录索引、出票授权 | `security` |
| `governance` | 1 | 390 | 质量规则 JDBC | `pangrova-governance` |
| `index` | 5 | 1,017 | 索引元数据门面/映射/schema 演进 | `pangrova-indexing` |
| `index/jdbc` | 12 | 2,893 | 描述符/代际/映射/GC/统计/commit queue | `pangrova-indexing` |
| `index/model` | 10 | 416 | 索引模型/事件 | `pangrova-indexing` / `spi` |
| `materialization` | 9 | 3,187 | 物化内核的 JDBC 仓储 | `materialization.store.jdbc` |
| `model` | 4 | 95 | 表/列/类型/引用值对象 | metadata-core / `spi` |
| `model/asset` | 8 | 535 | 通用 Asset 模型 | metadata-core |
| `schema` | 4 | 241 | schema evolution 校验 | metadata-core |
| `semantic` | 8 | 631 | Semantic View 版本/发布 | `semantic` |
| `serde` | 1 | 39 | TableMetadata JSON | metadata-core |
| `service` | 17 | 1,930 | MetadataService/表解析/DDL/tag/生命周期 | metadata-core |
| `source/jdbc` | 5 | 1,290 | managed source 发布状态机 | `metadata.core.source` |
| `storage` | 2 | 983 | 存储 profile、COPY 源读准入 | `security` / `query.transaction` |
| `store` | 7 | 826 | MetadataStore/JDBC/连接池/迁移 | metadata-core |
| `streaming` | 8 | 1,120 | CDC 注册/offset/watermark/state | `pangrova-streaming` |
| `sync` | 6 | 722 | 增量同步/版本观测 | metadata-core |
| `tenant` | 1 | 437 | 租户 JDBC | `pangrova-security` |

行数为各包子包 `wc -l` 汇总；§0.2 的 38,959 为逐文件 `count('\n')+1` 统计，二者相差每文件 ≤1 行。

### 2.2 对 Coordinator 的耦合

`pangrova-coordinator` 对 `org.pangrova.metadata.*` 有 **1,114 处 import**，分布如下（按包，前 12）：

| 包 | import 数 | 说明 |
|---|---:|---|
| `catalog` | 375 | Coordinator 直接构造/使用 `CatalogRegistry`、`CatalogHandle`、`CatalogDescriptor` |
| `artifact.build` | 122 | 构建 job/page/lease 契约直接进入 Coordinator |
| `control` | 89 | 表控制面 API 直接进入 DDL/DML/维护路径 |
| `model` | 73 | 表/列模型 |
| `store` | 55 | `MetadataStore`、`JdbcMetadataStore`、迁移 runner |
| `artifact.contract` | 51 | artifact 格式/类型常量 |
| `streaming` | 41 | `StreamState`/`StreamStateStore` 等直接使用 metadata 实现 |
| `service` | 38 | `MetadataService`、`DefaultMetadataService` |
| `index` | 37 | 索引接口/模型 |
| `sync` | 31 | `MetadataSyncService`、`TableVersionObserver` |
| `index.model` | 24 | 索引模型 |
| `coordination` | 18 | journal/anchor/lease |

结论：Coordinator 不是通过一个稳定 facade 使用 metadata，而是直接依赖 metadata 的内部实现包。任何“拆包/移类”都会产生大量调用方改动；这也是为什么拆分计划必须先提供 facade/契约，再做物理迁移。

### 2.3 数据库现状

- 114 张表，集中在 `public` schema；`materialization` 是唯一独立 schema（V68）。
- `V1__init_schema.sql` 一次创建了 **29 张表**，覆盖 catalog、coordinator/worker 注册、租约、索引、CDC、质量规则、审计、tenant、watermark、journal、schema/table projection 等至少 10 个域。
- 92 个迁移中，大量是“补列/补约束/补 fencing”的增量修复（V14、V15、V20/V30、V33、V36–V38、V44、V49、V55、V67、V74、V78、V82、V91 等），说明模型在短期内经历了高频演进。
- 迁移版本不连续：缺 V11–13、V16、V76–77、V79–81。`SchemaMigrationRunner` 使用 `baselineOnMigrate(true)` + `baselineVersion("0")`，但没有 `ignoreMigrationPatterns` 或 tombstone；对空库可自动 baseline，对已执行过缺失版本的环境会直接失败。

### 2.4 既有目标文档与当前代码的偏差

| 文档 | 既有结论 | 当前代码偏差 |
|---|---|---|
| `docs/architecture/metadata-center-redesign.md` | 第一阶段保持单一 metadata artifact；内部包按 `api/domain/catalog/resolution/binding/mutation/statistics/cache/event/store/system` 组织；Flyway 是唯一 DDL owner；删除未使用的 Iceberg 依赖 | 当前包结构仍是 `artifact/index/control/coordination/materialization/...` 的大聚合；4 个 Store 构造函数建表；Iceberg API/Core 仍在 POM |
| `docs/architecture/metadata-target-architecture-research.md` | 目标四平面、SPI 重构、可靠变更传播、全量 reconcile、分阶段路线 | 当前 `MetadataService` 仍是 17 方法大接口；事件仍 best-effort；cache 仍两套 |
| `docs/architecture/long-term-modular-architecture.md` | metadata 实际是共享数据库模块；数据库共享 ≠ schema ownership 共享；依赖测试没有覆盖 metadata | 当前 `ModuleDependencyPropertyTest` 仍未把 metadata 纳入 `CONSTRAINED_MODULES`，且文档仍写“18 modules”而根 POM 已有 25 个 module entry |
| `docs/architecture/metadata.md` | 详细描述 table control plane、缓存、DDL、持久化 | 第 11 节 module structure 仍只列旧包（V1 单迁移），与 296 文件/92 迁移现状严重脱节 |

---

## 3. 业务域与全量文件清单

> 以下 §3.1–§3.18 的表格覆盖 `src/main/java/org/pangrova/metadata` 下 **293 个业务类**；另有 3 个 `package-info.java`，合计 296 个主代码文件。每行给出文件、行数、类型和代码 Javadoc/结构摘要；每节开头的领域说明给出完整业务解释。

### 3.0 阅读方式

- **契约/值对象**：`record`/`enum`/纯接口，通常应下沉到 `spi` 或 `metadata-model`。
- **实现**：`Jdbc*`/`InMemory*`，承载持久化或测试替身；重点看事务、锁、SQL、错误处理。
- **门面/装配**：`*Service`/`*Registry`/`*Bootstrap`，重点看职责是否混杂、默认值是否 fail-closed。
- **目标归属**统一在 §6.1 owner 摘要与治理计划 §3.2 中给出，不在每个文件行重复。


### 3.1 模型、事件、异常与序列化

这一组是 metadata 的词汇表：`TableMetadata`/`ColumnMetadata`/`ColumnType`/`QualifiedTableRef` 是跨 catalog 的通用表模型；`Asset` 是 6 类 catalog 资产的多态模型；`SchemaChange` 是 sealed 的 5 种演进操作；`MetadataEvent` 是 sealed 的 9 种生命周期事件；异常层是 13 个只带构造器的薄类。注意两套模型并存：`TableAsset` 与 `TableMetadata` 都能表达表；两套 JSON 配置：`TableMetadataSerializer` 与 `MetadataJsonMapper`。事件 dispatcher 是单线程有界队列，队列满丢弃（§4.3 C-02）。


**`model/` — 4 files, 99 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `ColumnMetadata.java` | 24 | record ColumnMetadata | Immutable column metadata with name, type, nullability, comment, and tags. |
| `ColumnType.java` | 26 | record ColumnType | Parameterized column type with precision, scale, and length. |
| `QualifiedTableRef.java` | 16 | record QualifiedTableRef | Three-part table reference: catalog.schema.table. |
| `TableMetadata.java` | 33 | record TableMetadata | Unified table metadata: catalog/schema/table identity, columns, partitioning, sort order, and properties. |

**`model/asset/` — 8 files, 535 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `Asset.java` | 52 | interface Asset | Common contract for any catalog-managed asset. |
| `AssetType.java` | 34 | enum AssetType | Catalog-managed asset kind. |
| `FunctionAsset.java` | 90 | record FunctionAsset | {@link AssetType#FUNCTION} asset: a user-defined function (UDF) registered in |
| `MaterializedViewAsset.java` | 68 | record MaterializedViewAsset | {@link AssetType#MATERIALIZED_VIEW} asset: a SQL view whose result is |
| `ModelAsset.java` | 75 | record ModelAsset | {@link AssetType#MODEL} asset: a machine-learning model registered in the |
| `TableAsset.java` | 74 | record TableAsset | {@link AssetType#TABLE} asset: catalog-managed relational table. |
| `ViewAsset.java` | 77 | record ViewAsset | {@link AssetType#VIEW} asset: a logical SQL view. |
| `VolumeAsset.java` | 65 | record VolumeAsset | {@link AssetType#VOLUME} asset: a named handle to a directory of files |

**`schema/` — 4 files, 241 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `SchemaChange.java` | 25 | interface SchemaChange | Sealed interface representing possible schema evolution operations. |
| `SchemaEvolutionValidator.java` | 161 | class SchemaEvolutionValidator | Validates schema evolution changes against safe promotion rules and nullability constraints. |
| `SchemaValidationResult.java` | 23 | record SchemaValidationResult | Result of schema evolution validation: compatibility flag with errors and warnings. |
| `StrictnessLevel.java` | 32 | enum StrictnessLevel | Controls how strictly schema evolution changes are validated. |

**`serde/` — 1 files, 39 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `TableMetadataSerializer.java` | 39 | class TableMetadataSerializer | JSON serializer/deserializer for TableMetadata using Jackson. |

**`exception/` — 7 files, 190 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `IndexAlreadyExistsException.java` | 16 | class IndexAlreadyExistsException | Thrown when attempting to register an index that already exists |
| `MetadataException.java` | 19 | class MetadataException | Base exception for all metadata-related errors. |
| `MetadataResolutionException.java` | 70 | class MetadataResolutionException | Thrown when metadata resolution fails due to backend unavailability or connector errors. |
| `MetadataSerializationException.java` | 18 | class MetadataSerializationException | Thrown when metadata serialization or deserialization fails. |
| `MetadataSyncException.java` | 31 | class MetadataSyncException | Thrown when metadata synchronization fails after all retry attempts are exhausted. |
| `MetastoreException.java` | 18 | class MetastoreException | Thrown when metastore persistence operations fail. |
| `TableNotFoundException.java` | 18 | class TableNotFoundException | Thrown when a referenced table does not exist. |

**`exception/catalog/` — 6 files, 120 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CatalogAccessDeniedException.java` | 25 | class CatalogAccessDeniedException | Thrown when a tenant attempts to access a catalog they are not authorized for. |
| `CatalogAlreadyExistsException.java` | 19 | class CatalogAlreadyExistsException | Thrown when attempting to create a catalog that already exists. |
| `CatalogBackendException.java` | 19 | class CatalogBackendException | Thrown when communication with the catalog backend fails. |
| `CatalogNotFoundException.java` | 19 | class CatalogNotFoundException | Thrown when a referenced catalog does not exist. |
| `CatalogProviderNotFoundException.java` | 19 | class CatalogProviderNotFoundException | Thrown when the specified catalog provider cannot be found. |
| `CatalogSuspendedException.java` | 19 | class CatalogSuspendedException | Thrown when accessing a catalog that is in suspended state. |

**`event/` — 3 files, 194 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `MetadataEvent.java` | 99 | interface MetadataEvent | Sealed interface for all metadata lifecycle events. |
| `MetadataEventDispatcher.java` | 84 | class MetadataEventDispatcher | Asynchronous dispatcher for metadata events with bounded queue and fault isolation per listener. |
| `MetadataEventListener.java` | 11 | interface MetadataEventListener | Functional interface for receiving metadata lifecycle events. |

### 3.2 Catalog 目录生命周期与联邦适配

`CatalogRegistry` 是 catalog 生命周期的唯一入口：`initialize` 从 store 加载全部 descriptor；`register` 做 check-then-act + save + load + 补偿；`unregister` 移除并 shutdown；`reconcileFromStore` 在 leader 选举后补齐/删除本地 catalog；`CatalogHealthMonitor` 每 30s 检查。`CatalogHandle` 是统一读/写/健康接口，按 type 分 Internal/Connector/Federation 三种实现。`adapter` 包是外部 catalog 联邦 SPI，`CachingFederatedCatalog` 是未被生产引用的缓存装饰器。


**`catalog/` — 12 files, 919 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CatalogDescriptor.java` | 56 | record CatalogDescriptor | Immutable descriptor for a registered catalog. |
| `CatalogHandle.java` | 62 | interface CatalogHandle | Unified handle for accessing a registered catalog regardless of its type. |
| `CatalogHealthMonitor.java` | 80 | class CatalogHealthMonitor | Periodic health checker for registered catalogs. |
| `CatalogHealthStatus.java` | 16 | record CatalogHealthStatus | Health check result for a catalog handle. |
| `CatalogRegistry.java` | 423 | class CatalogRegistry | Central registry for catalog lifecycle: register, unregister, lookup, health monitoring. |
| `CatalogState.java` | 13 | enum CatalogState | Lifecycle states of a catalog. |
| `CatalogType.java` | 11 | enum CatalogType | Classification of catalog types in the metadata system. |
| `ConnectorCatalogHandle.java` | 91 | class ConnectorCatalogHandle | CatalogHandle backed by a Connector SPI implementation. |
| `ConnectorHandleFactory.java` | 10 | interface ConnectorHandleFactory | Factory for creating connector-backed catalog handles. |
| `FederationCatalogHandle.java` | 82 | class FederationCatalogHandle | CatalogHandle backed by a {@code FederatedCatalog} for external catalogs. |
| `FederationHandleFactory.java` | 10 | interface FederationHandleFactory | Factory for creating federation-backed catalog handles. |
| `InternalCatalogHandle.java` | 65 | class InternalCatalogHandle | CatalogHandle for the internal system catalog. Always healthy, no external delegation. |

**`adapter/` — 7 files, 356 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CachingFederatedCatalog.java` | 182 | class CachingFederatedCatalog | Decorator that adds {@link MetadataCache}-backed memoization to a |
| `ChangeListener.java` | 13 | interface ChangeListener | Functional interface for push-based metadata change notifications. |
| `FederatedCatalog.java` | 32 | interface FederatedCatalog | SPI for external catalog federation. Adapts an external catalog system |
| `FederatedCatalogFactory.java` | 14 | interface FederatedCatalogFactory | Factory for creating FederatedCatalog instances from configuration properties. |
| `FederationColumnHandle.java` | 10 | record FederationColumnHandle | Lightweight column handle for federation catalogs, carrying the column name and type. |
| `FederationConnectorMetadata.java` | 95 | class FederationConnectorMetadata | Read-only ConnectorMetadata that delegates to a {@link FederatedCatalog}. |
| `FederationTableHandle.java` | 10 | record FederationTableHandle | Lightweight table handle for federation catalogs. |

### 3.3 缓存与失效传播

缓存层由 L1 Caffeine + 可插拔 L2 组成；`JdbcInvalidationPoller` 轮询 `invalidation_log` 的 id 水位，把 TABLE/CATALOG/RESULT_CACHE 事件应用到本地缓存；`DdlPropagationWaiter` 试图等待所有 coordinator 追平，但零生产引用且实现与注释不符。当前还有 `DefaultMetadataService` 的独立 `resolvedTableCache`，形成第二套缓存（§4.3 C-04）。


**`cache/` — 11 files, 1180 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CacheStats.java` | 8 | record CacheStats | Statistics for a metadata cache instance. |
| `DdlPropagationWaiter.java` | 101 | class DdlPropagationWaiter | Waits for DDL invalidation to propagate to all coordinators. |
| `DisabledL2CacheStore.java` | 35 | class DisabledL2CacheStore | No-op L2 cache store when Redis is unavailable. |
| `InMemoryL2CacheStore.java` | 72 | class InMemoryL2CacheStore | In-process L2 cache store backed by Caffeine. Replaces the former |
| `InvalidationEntry.java` | 39 | record InvalidationEntry | Immutable record representing a single cache invalidation event persisted in the |
| `InvalidationMetrics.java` | 113 | class InvalidationMetrics | Metrics collector for DDL invalidation propagation observability. |
| `JdbcInvalidationPoller.java` | 412 | class JdbcInvalidationPoller | Polls the {@code invalidation_log} JDBC table at a fixed interval and applies |
| `L2CacheStore.java` | 20 | interface L2CacheStore | Pluggable L2 cache store (e.g., Redis in production). |
| `MetadataCache.java` | 22 | interface MetadataCache | Cache interface for table metadata lookups. |
| `TableInvalidationListener.java` | 23 | interface TableInvalidationListener | Callback fired by {@link JdbcInvalidationPoller} after applying a TABLE |
| `TieredMetadataCache.java` | 335 | class TieredMetadataCache | Two-tier metadata cache: L1 Caffeine (in-process) + pluggable L2 (e.g., Redis). |

### 3.4 持久化基础与 MetadataStore

持久化基础：`MetadataStore` 是表/目录/同步历史/schema 的混合端口；`JdbcMetadataStore` 用 PostgreSQL UPSERT 实现，并在写表/删除表时向 `invalidation_log` 追加失效记录；`EmbeddedDataSource`/`PooledDataSourceFactory` 提供 DataSource；`SchemaMigrationRunner` 用 Flyway 从 `classpath:db/migration` 执行迁移。注意 store→cache 的反向依赖（`JdbcMetadataStore` import `InvalidationType`），以及 `updateSyncTime` 空实现。


**`store/` — 7 files, 826 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `EmbeddedDataSource.java` | 93 | class EmbeddedDataSource | Minimal {@link DataSource} backed by {@link DriverManager} for bootstrap-time connections. |
| `JdbcMetadataStore.java` | 423 | class JdbcMetadataStore | JDBC-backed MetadataStore for PostgreSQL persistence. |
| `JdbcSupport.java` | 78 | class JdbcSupport | Shared JDBC functional interfaces for metadata store implementations. |
| `MetadataJsonMapper.java` | 64 | class MetadataJsonMapper | Canonical JSON serialization boundary for metadata stores and definitions. |
| `MetadataStore.java` | 47 | interface MetadataStore | Persistent store for metadata: tables, catalog descriptors, and sync tracking. |
| `PooledDataSourceFactory.java` | 73 | class PooledDataSourceFactory | Creates a production-grade connection-pooled {@link DataSource} backed by HikariCP. |
| `SchemaMigrationRunner.java` | 48 | class SchemaMigrationRunner | Applies versioned SQL migrations to the metadata database schema. |

### 3.5 MetadataService 查询/DDL 入口

`MetadataService` 是查询/DDL 的统一入口。`resolveTable` 的路径是：control 校验（catalog 当前性/RENAMING 拒绝）→ `resolvedTableCache` → connector `getTableHandle`/`getTableMetadata`/`getTableStatistics` → control.describe → 缓存。DDL 路径：`createTable` 调 connector 后注册 MANAGED；`dropTable` 先 `requireDroppable` 再物理删除并 `markDropped`；`alterTable` 在全局锁内做 schema 校验、逐条应用变更、重读、失效、发事件。tag 操作写入 `table_control_governance_binding`，并同步 `ColumnTagService`。query lifecycle 负责 begin/cleanup 与 4 个计数器。


**`service/` — 17 files, 1930 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `DefaultConnectorSession.java` | 85 | class DefaultConnectorSession | Converts engine MetadataSessionContext to connector-facing ConnectorSession. |
| `DefaultMetadataService.java` | 571 | class DefaultMetadataService | Default metadata service: catalog lifecycle, table resolution with caching, |
| `MetadataCleanupConfig.java` | 35 | record MetadataCleanupConfig | Configuration for periodic metadata cleanup tasks. |
| `MetadataCleanupScheduler.java` | 166 | class MetadataCleanupScheduler | Schedules periodic invalidation-log pruning and stale-cache eviction. |
| `MetadataColumnDeclarations.java` | 53 | class MetadataColumnDeclarations | Preserves scalar declarations crossing from Metadata's model to the connector creation contract. |
| `MetadataColumnTagTarget.java` | 59 | record MetadataColumnTagTarget | Resolves a tag's canonical column under the caller's connector session and captured control owner. |
| `MetadataQueryLifecycles.java` | 128 | class MetadataQueryLifecycles | Indexes actual query owners and projects their catalog lifecycle outcomes for service metrics. |
| `MetadataResolutionCacheKey.java` | 43 | record MetadataResolutionCacheKey | Authenticated cache scope; secrets and connection properties are stored only as a digest. |
| `MetadataService.java` | 151 | interface MetadataService | Top-level metadata operations: catalog lifecycle, schema browsing, table resolution, and DDL. |
| `MetadataSessionContext.java` | 42 | record MetadataSessionContext | Engine-level session representation carrying user identity, scope, and properties. |
| `MetadataTagOperations.java` | 151 | class MetadataTagOperations | Column tags have one durable authority: governance bindings keyed by stable table identity. |
| `QueryLifecycleReport.java` | 64 | class QueryLifecycleReport | Immutable report of a query lifecycle operation (BEGIN or CLEANUP) across catalogs. |
| `ResolvedTable.java` | 29 | record ResolvedTable | Result of table resolution: combines handle, metadata, statistics, and control |
| `TableControlColumnMaskStore.java` | 109 | class TableControlColumnMaskStore | Binds policy persistence and actual foreground mask reads to the live stable table identity. |
| `TableControlDeferredPolicy.java` | 67 | class TableControlDeferredPolicy | Uses the metadata owner's current stable identity and canonical tags during durable authorization. |
| `TableControlQualityRuleStore.java` | 104 | class TableControlQualityRuleStore | Enforces stable ownership on actual quality-rule reads and writes, including scheduled reloads. |
| `TableControlRowPolicyStore.java` | 73 | class TableControlRowPolicyStore | Reads and persists row policies under their stable table identity; names are diagnostic. |

### 3.6 增量同步与版本观测

`MetadataSyncService` 负责外部 catalog → 本地 store 的增量/全量同步：读取 `lastSyncTime`、`adapter.getChangesSince`、逐条 `applyChange`（save/delete + invalidate + event + schema diff）、10% 随机漂移抽样、`finishSync`。`TableVersionObserver` 在 leader 的 sync 节奏上对 LINKED/MANAGED 表做版本对账：新 `version_token` 推进 `metadata_revision`、写 observation、invalidation、发 `ExternalCommitObservedEvent`；catalog 不可达则整轮跳过（不把发现失败当删除）。


**`sync/` — 6 files, 722 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `MetadataChange.java` | 17 | record MetadataChange | Represents a single metadata change detected during synchronization. |
| `MetadataSyncMetrics.java` | 83 | class MetadataSyncMetrics | Metrics collector for metadata synchronization, cache freshness, |
| `MetadataSyncService.java` | 373 | class MetadataSyncService | Orchestrates incremental and full metadata synchronization between external catalogs and the local store. |
| `SyncResult.java` | 22 | record SyncResult | Result of a metadata synchronization operation. |
| `TableVersionObservationReport.java` | 35 | record TableVersionObservationReport | Summary of one catalog's version-observation pass. |
| `TableVersionObserver.java` | 192 | class TableVersionObserver | Reconciles controlled tables against what the connector currently reports. |

### 3.7 表控制面（所有权/准入/治理绑定/统计/维护）

`control` 是 metadata 的“所有权/准入”域：OBSERVED 是 catalog 可见但无控制记录；LINKED 是显式 adopt 的外部表；MANAGED 是 Pangrova 创建/写入的表。`TableControlRecord` 保存稳定 `table_id`、format UUID、`version_token`、`metadata_revision`、catalog incarnation/revision；`TableControlService` 负责 observe/recover/register/adopt/unlink/rename/drop 和写/DDL 准入；`TableControlMaintenanceService` 解析 `ALTER ... ADOPT WITH` 白名单并记录 run；`TableControlStatisticsService` 持久化 ANALYZE 结果；`TableControlPolicyBinder` 把 tag/mask/row filter/quality/lineage 绑定到 `table_id`；四个 Store 都有 JDBC/InMemory 双实现。


**`control/` — 34 files, 3574 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CatalogDeploymentIdentity.java` | 14 | record CatalogDeploymentIdentity | Catalog authority captured by a controlled table, independent of catalog implementation details. |
| `GovernanceBinding.java` | 44 | record GovernanceBinding | One governance artifact bound to a stable {@code table_id}. |
| `GovernanceBindingType.java` | 26 | enum GovernanceBindingType | Governance artifacts that can be bound to a controlled table. |
| `InMemoryTableControlGovernanceBindingStore.java` | 55 | class InMemoryTableControlGovernanceBindingStore | Process-local binding store used when no durable metadata database is configured. |
| `InMemoryTableControlMaintenanceStore.java` | 54 | class InMemoryTableControlMaintenanceStore | Process-local maintenance policy and history. |
| `InMemoryTableControlStatisticsStore.java` | 28 | class InMemoryTableControlStatisticsStore | Process-local statistics store for tests and metadata-less deployments. |
| `InMemoryTableControlStore.java` | 112 | class InMemoryTableControlStore | Process-local control store used when no metadata datasource is configured. |
| `JdbcTableControlGovernanceBindingStore.java` | 199 | class JdbcTableControlGovernanceBindingStore | Durable governance bindings keyed by {@code table_id}. |
| `JdbcTableControlMaintenanceStore.java` | 224 | class JdbcTableControlMaintenanceStore | JDBC-backed maintenance policy and run history. |
| `JdbcTableControlStatisticsStore.java` | 172 | class JdbcTableControlStatisticsStore | Durable ANALYZE statistics keyed by the stable {@code table_id}. A peer coordinator |
| `JdbcTableControlStore.java` | 359 | class JdbcTableControlStore | JDBC-backed control store. Ownership decisions and maintenance admission must |
| `MaintenanceOperation.java` | 44 | enum MaintenanceOperation | Maintenance actions the control plane can authorize for a controlled table. |
| `MaintenanceRunState.java` | 14 | enum MaintenanceRunState | Terminal or in-progress state of one maintenance run. |
| `ObservationOutcome.java` | 18 | enum ObservationOutcome | Result of reconciling one controlled table against the connector's live state. |
| `OwnershipMode.java` | 17 | enum OwnershipMode | How Pangrova relates to a table. |
| `TableControlGovernanceBindingStore.java` | 21 | interface TableControlGovernanceBindingStore | Durable authority for governance bindings keyed by {@code table_id}. |
| `TableControlMaintenanceService.java` | 229 | class TableControlMaintenanceService | Authorizes maintenance and records its history. |
| `TableControlMaintenanceStore.java` | 24 | interface TableControlMaintenanceStore | Durable policy and history for controlled-table maintenance. |
| `TableControlPolicyBinder.java` | 148 | class TableControlPolicyBinder | Binds tags, masks, row filters, quality rules, and lineage to a stable |
| `TableControlProvenance.java` | 12 | enum TableControlProvenance | How a control record came into existence. |
| `TableControlRecord.java` | 248 | record TableControlRecord | Durable management state for a LINKED or MANAGED table. |
| `TableControlRenameIntent.java` | 48 | class TableControlRenameIntent | Durable source-name reservation for one admitted physical rename. |
| `TableControlService.java` | 827 | class TableControlService | Ownership, identity, and admission decisions for the table control plane. |
| `TableControlSettings.java` | 56 | record TableControlSettings | Deployment switches for the table control plane. |
| `TableControlState.java` | 18 | enum TableControlState | Lifecycle state of a persisted table-control record. |
| `TableControlStatistics.java` | 53 | record TableControlStatistics | Statistics collected by {@code ANALYZE TABLE}, bound to the stable table and the |
| `TableControlStatisticsService.java` | 179 | class TableControlStatisticsService | Persists statistics collected by {@code ANALYZE TABLE} for controlled tables. |
| `TableControlStatisticsStore.java` | 16 | interface TableControlStatisticsStore | Durable storage for statistics collected by {@code ANALYZE TABLE}. |
| `TableControlStore.java` | 37 | interface TableControlStore | Durable store for LINKED and MANAGED table control records. |
| `TableControlView.java` | 45 | record TableControlView | Read model for table resolution: ownership plus the identity/version evidence |
| `TableMaintenancePolicy.java` | 130 | record TableMaintenancePolicy | Authorization and safety limits for maintenance of one controlled table. |
| `TableMaintenanceRun.java` | 62 | record TableMaintenanceRun | One maintenance attempt against a controlled table. |
| `TableVersionObservation.java` | 34 | record TableVersionObservation | Durable record of one observed version transition for a controlled table. |
| `package-info.java` | 7 | — | Metadata-owned table identity, ownership, admission and governance binding authority. |

### 3.8 协调、恢复、租约、注册与血缘 outbox

`coordination` 是崩溃恢复/互斥的基础设施：`JdbcWriteCommitJournal` 记录写意图/发布/中止/重放/清理；`JdbcExecutionAnchorStore` 用 DB 时钟租约 + 单调 token 做执行所有权 fencing；`JdbcDistributedLease`/`ZooKeeperDistributedLease` 提供租约；`JdbcCoordinatorRegistry`/`JdbcWorkerRegistry` 维护成员与心跳；`JdbcQueryStateJournal`/`JdbcQueryLineageOutbox` 记录查询状态与血缘 outbox；`JournalPendingWrites`/`JournalFlushOwner`/`JournalSqlOperation` 管理待提交事件、单 flush 线程和 SQL 连接生命周期；`checkpoint` 是任务检查点 JDBC。


**`coordination/` — 16 files, 4166 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcCoordinatorRegistry.java` | 263 | class JdbcCoordinatorRegistry | JDBC-backed coordinator registry using a {@code coordinator_registry} table. |
| `JdbcCopyFileClaimStore.java` | 146 | class JdbcCopyFileClaimStore | PostgreSQL authority for immutable COPY source-file admissions; awaiting URI execution wiring. |
| `JdbcDistributedLease.java` | 399 | class JdbcDistributedLease | JDBC-backed distributed lease using a single {@code distributed_lease} table. |
| `JdbcExecutionAnchorStore.java` | 517 | class JdbcExecutionAnchorStore | PostgreSQL execution ownership with database-clock leases and monotonic per-execution fencing. |
| `JdbcQueryLineageOutbox.java` | 194 | class JdbcQueryLineageOutbox | PostgreSQL outbox shared with the authoritative journal; callers retain ownership of the DataSource. |
| `JdbcQueryStateJournal.java` | 161 | class JdbcQueryStateJournal | PostgreSQL journal with one query-fair, per-query ordered commit owner and retryable actual SQL drain. |
| `JdbcWorkerRegistry.java` | 349 | class JdbcWorkerRegistry | JDBC-backed worker registry using a {@code worker_registry} table. |
| `JdbcWriteCommitJournal.java` | 721 | class JdbcWriteCommitJournal | JDBC-backed write commit journal for crash-recovery idempotency. |
| `JournalEventSql.java` | 123 | class JournalEventSql | Single SQL encoding and exact event reconciliation for the two authoritative journal tables. |
| `JournalFlushOwner.java` | 150 | class JournalFlushOwner | One actual flush thread owns admitted writes; close joins it rather than cancelling its future. |
| `JournalPendingWrites.java` | 275 | class JournalPendingWrites | Owns admitted events and entered SQL operations until their actual completion. |
| `JournalSqlOperation.java` | 79 | class JournalSqlOperation | Retains an entered SQL connection until the body exits and actual close succeeds. |
| `QueryLineageFactSql.java` | 74 | class QueryLineageFactSql | Exact typed reconciliation used by both the query journal transaction and the outbox producer. |
| `ZooKeeperCoordinatorRegistry.java` | 291 | class ZooKeeperCoordinatorRegistry | ZooKeeper-backed {@link CoordinatorRegistry} (A-HA-01): replicates coordinator membership across a |
| `ZooKeeperDistributedLease.java` | 309 | class ZooKeeperDistributedLease | ZooKeeper-backed {@link DistributedLease} (A-HA-01): removes the single-JDBC coordination SPOF by |
| `ZooKeeperLeaseProvider.java` | 115 | class ZooKeeperLeaseProvider | Builds and owns the {@link CuratorFramework} client backing a {@link ZooKeeperDistributedLease} |

**`checkpoint/` — 1 files, 111 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcCheckpointStore.java` | 111 | class JdbcCheckpointStore | JDBC-backed {@link CheckpointStore} persisting task checkpoint state |

### 3.9 租户

`JdbcTenantStore` 实现 security 的 `TenantStore`：`tenants` + `tenant_user_memberships` 的 CRUD、成员校验、`initSchema`（构造函数/显式建表，和 Flyway 双源）。


**`tenant/` — 1 files, 437 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcTenantStore.java` | 437 | class JdbcTenantStore | JDBC-backed implementation of {@link TenantStore}. |

### 3.10 文件空间、存储 profile、COPY 准入与 managed source

`file/storage/source` 是存储控制面：`JdbcFileSpaceRegistry` 管理 Volume/Stage 命名空间与 location/profile/directory 的 CAS；`JdbcFileDirectoryIndex` 管理目录刷新令牌、pending 页、已发布索引；`JdbcFileSpaceAuthority` 是出票授权闸门；`JdbcStorageProfileRegistry` 管理当前/历史 storage profile；`JdbcCopySourceReadAdmissions` 签发/兑换 COPY 源读票据（nonce 只存哈希，绑定 anchor/lease/tenant/profile）；`source/jdbc` 的 `JdbcManagedSourceCatalog` 是 managed immutable source 的 reserve→prepare→register→accept/abort 状态机，配合 namespace/registration/invocation/cleanup 四个协作类。


**`file/` — 4 files, 870 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `FileSpaceAuthority.java` | 16 | interface FileSpaceAuthority | Current tenant projection and an authority read boundary covering complete ticket issuance. |
| `JdbcFileDirectoryIndex.java` | 311 | class JdbcFileDirectoryIndex | Durable directory observations. Pages remain private until a terminal page is |
| `JdbcFileSpaceAuthority.java` | 79 | class JdbcFileSpaceAuthority | Keeps file-ticket issuance ordered with durable role, tenant, namespace and profile mutations. |
| `JdbcFileSpaceRegistry.java` | 464 | class JdbcFileSpaceRegistry | PostgreSQL authority for Volume and Stage namespace definitions. |

**`storage/` — 2 files, 983 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcCopySourceReadAdmissions.java` | 768 | class JdbcCopySourceReadAdmissions | Durable Coordinator authority for one exact COPY source read. The random |
| `JdbcStorageProfileRegistry.java` | 215 | class JdbcStorageProfileRegistry | PostgreSQL authority for current storage profiles and immutable historical definitions. |

**`source/jdbc/` — 5 files, 1290 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcManagedSourceCatalog.java` | 766 | class JdbcManagedSourceCatalog | Sole metadata authority for managed immutable source manifests and their durable write intents. |
| `JdbcSourceNamespace.java` | 70 | class JdbcSourceNamespace | Checks old immutable source objects against the actual current catalog storage namespace. |
| `JdbcSourceObjectCleanup.java` | 149 | class JdbcSourceObjectCleanup | Fenced bounded cleanup of SDK objects, retaining every uncertain invocation and referenced object. |
| `JdbcSourceObjectRegistration.java` | 200 | class JdbcSourceObjectRegistration | Bounded exact page receipts and streamed object-membership sealing on the locked candidate transaction. |
| `JdbcSourcePreparationInvocations.java` | 105 | class JdbcSourcePreparationInvocations | Durable SDK-call issuance and exact join receipts, on the already fenced catalog transaction. |

### 3.11 Artifact 构建与发布

`artifact` 是通用不可变产物的构建/发布控制面：`ArtifactBuildJob`/`Page`/`Lease`/`Split`/`Output`/`StageGraph`/`StageTemplate` 描述 job、页、租约、split、输出和 DAG；`JdbcArtifactBuildStore` 推进状态机（`createJob`→`claimNextPage`→`completePage`→`READY_TO_PUBLISH`→`claimReadyForPublication`→`publish`）；`ArtifactPublicationRouter` 按 artifactType 路由到 generated-value/index/tabular-export 三个原子发布器；`JdbcArtifactStageExecutionStore`/`JdbcArtifactStageExchangeStore` 处理外部 exchange 证据；`ArtifactExecutionFence`/`CurrentAuthority` 做同事务授权与 anchor 校验。


**`artifact/` — 7 files, 253 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `ArtifactGenerationManifest.java` | 78 | record ArtifactGenerationManifest | Canonical immutable publication view shared by every artifact consumer. |
| `ArtifactPublicationRouter.java` | 35 | class ArtifactPublicationRouter | Strict artifact-type router for independent atomic publication adapters. |
| `ArtifactPublicationStore.java` | 22 | interface ArtifactPublicationStore | Atomic catalog publication adapter for one generic artifact type. |
| `GeneratedValueArtifactDescriptor.java` | 43 | record GeneratedValueArtifactDescriptor | Durable identity of one generated-column sidecar independent of its generations. |
| `GeneratedValueArtifactMetadataStore.java` | 20 | interface GeneratedValueArtifactMetadataStore | Metadata sidecar for generated-column artifact identity and snapshot selection. |
| `GeneratedValueArtifactReference.java` | 27 | record GeneratedValueArtifactReference | Exact immutable generated-value generation selected for a source snapshot. |
| `GeneratedValueBuildSubmission.java` | 28 | record GeneratedValueBuildSubmission | One immutable generated descriptor and its canonical durable graph, persisted as part of the write outbox. |

**`artifact/build/` — 14 files, 1066 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `ArtifactBuildDispatchSpec.java` | 69 | record ArtifactBuildDispatchSpec | Immutable, replayable control-plane payload for dispatching Native artifact work. |
| `ArtifactBuildJob.java` | 91 | record ArtifactBuildJob | Immutable identity plus mutable progress snapshot for one artifact generation. |
| `ArtifactBuildJobKey.java` | 32 | record ArtifactBuildJobKey | Tenant-scoped durable artifact job identity. |
| `ArtifactBuildJobState.java` | 12 | enum ArtifactBuildJobState | Durable lifecycle of a generic immutable artifact build. |
| `ArtifactBuildLease.java` | 91 | record ArtifactBuildLease | Fenced lease granted to one Coordinator for dispatching a work page. |
| `ArtifactBuildOutput.java` | 44 | record ArtifactBuildOutput | Immutable object metadata emitted directly by a Native background task. |
| `ArtifactBuildPage.java` | 80 | record ArtifactBuildPage | Stable work-page identity and exact source coverage. |
| `ArtifactBuildSplit.java` | 57 | record ArtifactBuildSplit | Exact canonical DataSplit bytes assigned to one durable artifact work page. |
| `ArtifactBuildStore.java` | 116 | interface ArtifactBuildStore | Durable, lease-fenced control plane for generic immutable artifact builds. |
| `ArtifactExchangeFinalization.java` | 77 | record ArtifactExchangeFinalization | Validated, real external-store finalize response with its exact capability-free RPC identity. |
| `ArtifactExchangeRegistration.java` | 101 | record ArtifactExchangeRegistration | Immutable external allocation identity. This contract deliberately contains no access capabilities. |
| `ArtifactExecutionOwner.java` | 19 | record ArtifactExecutionOwner | Exact backend-issued execution identity carried by a Native task completion. |
| `ArtifactPageCompletion.java` | 31 | record ArtifactPageCompletion | Fenced, immutable completion report for one artifact work page. |
| `ArtifactStageGraph.java` | 246 | record ArtifactStageGraph | Canonical durable stage topology. Metadata stores bytes and identities, never planner nodes or row data. |

**`artifact/build/template/` — 2 files, 88 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `ArtifactStageTemplate.java` | 85 | record ArtifactStageTemplate | Versioned, immutable wire-neutral stage bindings and logical result types; never provider credentials. |
| `package-info.java` | 3 | — | Immutable wire-neutral stage execution templates and exact published input references. |

**`artifact/contract/` — 8 files, 351 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `ArtifactBuildOutputScope.java` | 9 | enum ArtifactBuildOutputScope | Distribution and publication scope of one immutable artifact object. |
| `ArtifactBuildPageKind.java` | 11 | enum ArtifactBuildPageKind | Data-plane role of one durable artifact work page. |
| `ArtifactDependency.java` | 47 | record ArtifactDependency | One version-pinned input to an immutable artifact generation. |
| `ArtifactDependencySet.java` | 96 | record ArtifactDependencySet | Canonically ordered dependency set and its stable SHA-256 identity. |
| `ArtifactEmptyPartitionPolicy.java` | 8 | enum ArtifactEmptyPartitionPolicy | Explicit zero-row publication policy; omission is never interpreted as permission. |
| `ArtifactExecutionKind.java` | 8 | enum ArtifactExecutionKind | Canonical control-plane purpose of one durable artifact job. |
| `ArtifactFormatContracts.java` | 158 | class ArtifactFormatContracts | Executable work-page, scope and role contracts for immutable artifacts. |
| `ArtifactTypeIds.java` | 14 | class ArtifactTypeIds | Canonical catalog identifiers for durable artifacts. |

**`artifact/jdbc/` — 14 files, 2704 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `ArtifactBuildRevocation.java` | 72 | class ArtifactBuildRevocation | Cancels only the revoked generation, never a different subject's build of the same artifact. |
| `ArtifactCurrentAuthority.java` | 48 | class ArtifactCurrentAuthority | Bridges current security and canonical tenant authority on one metadata transaction. |
| `ArtifactExecutionFence.java` | 122 | class ArtifactExecutionFence | Same-transaction PostgreSQL execution fence. Lock after authority and before job/descriptor locks. |
| `ArtifactPublicationJob.java` | 31 | interface ArtifactPublicationJob | Package-local view shared by type-specific atomic publication adapters. |
| `ArtifactPublicationManifestDigest.java` | 68 | class ArtifactPublicationManifestDigest | Canonical content digest of a published index artifact generation. |
| `ArtifactPublicationOutputValidator.java` | 322 | class ArtifactPublicationOutputValidator | Validates complete artifact output bundles before atomic publication. |
| `CurrentDeferredExecutionContext.java` | 49 | class CurrentDeferredExecutionContext | Process-local current authority evidence; neither a credential nor a persisted identity snapshot. |
| `GeneratedModelCurrentAuthority.java` | 72 | class GeneratedModelCurrentAuthority | Current PostgreSQL technical model contracts and invocation-role authority for durable publication. |
| `JdbcArtifactStageExchangeStore.java` | 315 | class JdbcArtifactStageExchangeStore | Immutable external exchange evidence, gated by current security and the actual execution anchor. |
| `JdbcGeneratedValueArtifactPublicationStore.java` | 451 | class JdbcGeneratedValueArtifactPublicationStore | Atomic publication and snapshot lookup for generated-column value sidecars. |
| `JdbcGeneratedValueDescriptorStore.java` | 242 | class JdbcGeneratedValueDescriptorStore | JDBC persistence and fencing for generated-value descriptor targets. |
| `JdbcGeneratedWriteOutbox.java` | 77 | class JdbcGeneratedWriteOutbox | Atomic canonical generated graph creation and COMMITTED redo-outbox acknowledgement. |
| `JdbcIndexArtifactPublicationStore.java` | 460 | class JdbcIndexArtifactPublicationStore | PostgreSQL/H2 implementation of atomic generic-artifact index publication. |
| `JdbcTabularExportPublicationStore.java` | 375 | class JdbcTabularExportPublicationStore | Atomic PostgreSQL publisher for generic immutable tabular exports. |

**`artifact/jdbc/build/` — 13 files, 2744 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcArtifactBuildCompletion.java` | 118 | class JdbcArtifactBuildCompletion | Applies one fenced artifact page completion inside its caller's transaction. |
| `JdbcArtifactBuildJobInsertion.java` | 78 | class JdbcArtifactBuildJobInsertion | Inserts a new durable artifact job and its ordered dependency fence. |
| `JdbcArtifactBuildOutputQueries.java` | 258 | class JdbcArtifactBuildOutputQueries | Batched JDBC queries for immutable artifact outputs and canonical splits. |
| `JdbcArtifactBuildPageClaim.java` | 69 | class JdbcArtifactBuildPageClaim | Claims one already-locked page in the current-authority transaction. |
| `JdbcArtifactBuildPageLoader.java` | 209 | class JdbcArtifactBuildPageLoader | Loads durable artifact pages and compares immutable job identities. |
| `JdbcArtifactBuildPageSelection.java` | 138 | class JdbcArtifactBuildPageSelection | One page-selection contract for ordinary polling and exact durable stage partitions. |
| `JdbcArtifactBuildReader.java` | 163 | class JdbcArtifactBuildReader | PostgreSQL-backed implementation of the durable artifact-build state machine. |
| `JdbcArtifactBuildStore.java` | 533 | class JdbcArtifactBuildStore | PostgreSQL-backed implementation of the durable artifact-build state machine. |
| `JdbcArtifactBuildStoreMappings.java` | 317 | class JdbcArtifactBuildStoreMappings | PostgreSQL-backed implementation of the durable artifact-build state machine. |
| `JdbcArtifactBuildStoreSupport.java` | 424 | class JdbcArtifactBuildStoreSupport | PostgreSQL-backed implementation of the durable artifact-build state machine. |
| `JdbcArtifactPublishedInputs.java` | 115 | class JdbcArtifactPublishedInputs | Same-connection immutable publication reads, shared by query resolution and durable claim preparation. |
| `JdbcArtifactStageExecutionStore.java` | 315 | class JdbcArtifactStageExecutionStore | Same-authority transaction boundary for preparing one exact partition in the shared stage engine. |
| `package-info.java` | 7 | — | Internal PostgreSQL artifact-build state machine, owned by pangrova-metadata. |

**`artifact/jdbc/feature/` — 1 files, 81 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcCatalogExportJobInsertion.java` | 81 | class JdbcCatalogExportJobInsertion | Inserts or validates the immutable catalog-to-artifact export link. |

### 3.12 Index 元数据与代际发布

`index` 是索引元数据/代际发布：`IndexDescriptor`/`IndexGenerationManifest`/`IndexFileMapping`/`IndexFileSelection` 是核心模型；`JdbcIndexDescriptorStore` 做 descriptor CRUD、generation 预留、授权/advisory lock；`JdbcIndexPublicationStore` 在事务内 retire 旧 ACTIVE、插入新 ACTIVE（`epoch=MAX+1`）、更新 descriptor/build 状态；`JdbcIndexCommitQueue` 维护 per-table commit log 与 fenced head；`JdbcIndexFileMappingStore`/`JdbcIndexGcStore`/`JdbcIndexStatisticsStore` 分别负责映射、GC、文件列统计；`JdbcIndexMetadataStore` 是 60+ 方法的委托门面。


**`index/` — 5 files, 1022 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CommitEventRequest.java` | 37 | record CommitEventRequest | Parameter object for recording a commit event in the shared index commit log. |
| `IndexArtifactSelectionSupport.java` | 45 | class IndexArtifactSelectionSupport | Validation helpers for exact generation-pinned artifact selection. |
| `IndexMetadataMapper.java` | 213 | class IndexMetadataMapper | Handles JDBC ↔ domain object mapping for index metadata. |
| `IndexMetadataStore.java` | 537 | interface IndexMetadataStore | Persistent store for index metadata: descriptors and file mappings. |
| `IndexSchemaEvolutionHandler.java` | 190 | class IndexSchemaEvolutionHandler | Handles schema change events and updates affected indexes accordingly. |

**`index/jdbc/` — 12 files, 2905 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `GenerationReservation.java` | 16 | record GenerationReservation | Immutable arguments for reserving one fenced index generation. |
| `JdbcIndexCommitQueue.java` | 342 | class JdbcIndexCommitQueue | Maintains the durable per-table commit-event queue and its fenced head. |
| `JdbcIndexDescriptorReadStore.java` | 58 | class JdbcIndexDescriptorReadStore | Read-only descriptor queries shared by index scheduling and maintenance. |
| `JdbcIndexDescriptorStore.java` | 627 | class JdbcIndexDescriptorStore | JDBC operations for {@link IndexDescriptor} CRUD, schema evolution, |
| `JdbcIndexFileMappingStore.java` | 395 | class JdbcIndexFileMappingStore | JDBC operations for {@link IndexFileMapping} CRUD and queries. |
| `JdbcIndexGcStore.java` | 143 | class JdbcIndexGcStore | JDBC operations for GC-related queries: orphan marking, stale file mappings, |
| `JdbcIndexMetadataStore.java` | 465 | class JdbcIndexMetadataStore | JDBC-backed implementation of {@link IndexMetadataStore}. |
| `JdbcIndexPublicationStore.java` | 404 | class JdbcIndexPublicationStore | Transactional validation and activation of immutable index generations. |
| `JdbcIndexSql.java` | 46 | class JdbcIndexSql | SQL construction helpers shared by JDBC index stores. |
| `JdbcIndexStateStore.java` | 70 | class JdbcIndexStateStore | JDBC operations for index build claim coordination. |
| `JdbcIndexStatisticsStore.java` | 247 | class JdbcIndexStatisticsStore | JDBC operations for file column statistics (L0 pruning support). |
| `JdbcStoreSupport.java` | 92 | class JdbcStoreSupport | Shared JDBC infrastructure for index metadata sub-stores. |

**`index/model/` — 10 files, 426 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `BreakerState.java` | 15 | record BreakerState | Persistent circuit breaker state for distributed index evaluation. |
| `CacheEvent.java` | 17 | record CacheEvent | Represents a cache invalidation event for cross-coordinator coordination. |
| `CommitLogEntry.java` | 27 | record CommitLogEntry | Represents a single entry in the shared index commit log. |
| `IndexArtifactFormat.java` | 35 | record IndexArtifactFormat | Versioned cross-runtime identity of the bytes stored in an index artifact. |
| `IndexFileMapping.java` | 83 | record IndexFileMapping | Tracks the mapping between a data file and its corresponding index file. |
| `IndexFileSelection.java` | 25 | record IndexFileSelection | Exact immutable index generation and optional source snapshot selected for a query. |
| `IndexGenerationManifest.java` | 65 | record IndexGenerationManifest | Immutable aggregate identity published for one complete index generation. |
| `IndexPublicationRequest.java` | 47 | record IndexPublicationRequest | Compare-and-set request that atomically activates one complete index generation. |
| `SchemaChangeEvent.java` | 98 | record SchemaChangeEvent | Immutable event describing a schema change that may affect indexes. |
| `SchemaChangeType.java` | 14 | enum SchemaChangeType | Types of schema changes that may affect index validity. |

### 3.13 Streaming CDC 状态存储

`streaming` 持久化 CDC 摄入状态：`CdcRegistrationStore` 保存 stream 注册/心跳；`StreamOffsetStore` 保存 per-partition committed offset；`WatermarkStore` 保存单列 watermark；`StreamStateStore` 保存 19 字段的 `StreamState`（含 epoch），用 `(tenant, checkpoint, epoch)` CAS，失败抛 `StaleEpochException`；`StreamLifecycleState` 是生命周期枚举。注意 offset/watermark 覆盖写可回退（§4.3 C-17）。


**`streaming/` — 8 files, 1120 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcCdcRegistrationStore.java` | 191 | class JdbcCdcRegistrationStore | JDBC-backed {@link CdcRegistrationStore} that persists CDC stream registrations |
| `JdbcStreamOffsetStore.java` | 136 | class JdbcStreamOffsetStore | JDBC-backed implementation of {@link StreamOffsetStore}. |
| `JdbcStreamStateStore.java` | 336 | class JdbcStreamStateStore | JDBC-backed implementation of {@link StreamStateStore}. |
| `JdbcWatermarkStore.java` | 78 | class JdbcWatermarkStore | JDBC-backed {@link WatermarkStore} persisting watermark positions |
| `StreamLifecycleState.java` | 21 | enum StreamLifecycleState | Lifecycle states for an ingestion stream. |
| `StreamOffsetStore.java` | 44 | interface StreamOffsetStore | Persistent store for committed Kafka offsets per ingestion stream. |
| `StreamState.java` | 233 | record StreamState | Immutable state record for a CDC ingestion stream. |
| `StreamStateStore.java` | 81 | interface StreamStateStore | Persistent store for CDC ingestion stream state. |

### 3.14 Feature / Semantic / Evaluation

Feature/Semantic/Evaluation 是 AI 治理元数据：`FeatureCatalogStore` 保存 Feature View/Training Dataset 的不可变版本与 active pointer；`SemanticViewStore` 保存 Semantic View 的不可变版本、验证状态与 active pointer；`EvaluationStore` 保存 dataset version、metric policy、run、metric evidence、release decision；三者都用 epoch CAS/active pointer 做发布，release gate 在 `JdbcEvaluationStore` 内嵌实现（§4.3 C-23）。


**`feature/` — 2 files, 616 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `FeatureCatalogStore.java` | 234 | interface FeatureCatalogStore | Tenant-scoped immutable Feature View and Training Dataset catalog. |
| `JdbcFeatureCatalogStore.java` | 382 | class JdbcFeatureCatalogStore | PostgreSQL implementation of the governed feature and training catalog. |

**`semantic/` — 8 files, 631 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcSemanticViewStore.java` | 342 | class JdbcSemanticViewStore | PostgreSQL implementation of immutable Semantic View metadata and fenced publication. |
| `SemanticViewId.java` | 34 | record SemanticViewId | Tenant-scoped identity of a governed Semantic View. |
| `SemanticViewPublication.java` | 41 | record SemanticViewPublication | Fenced active-version pointer for one Semantic View identity. |
| `SemanticViewStore.java` | 53 | interface SemanticViewStore | Durable metadata contract for immutable Semantic View versions and their active pointer. |
| `SemanticViewValidation.java` | 36 | class SemanticViewValidation | Package-private validation helper: definition SHA-256, version/status and structural checks (no class Javadoc). |
| `SemanticViewValidationState.java` | 25 | enum SemanticViewValidationState | Result of validating a versioned Semantic View definition. |
| `SemanticViewVersion.java` | 54 | record SemanticViewVersion | Immutable, validated definition version of a governed Semantic View. |
| `SemanticViewVersionStatus.java` | 46 | record SemanticViewVersionStatus | One immutable Semantic View version plus its tenant-scoped publication state. |

**`evaluation/` — 15 files, 1434 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `EvaluationDatasetId.java` | 28 | record EvaluationDatasetId | Tenant-scoped identity of an immutable-versioned evaluation dataset. |
| `EvaluationDatasetVersion.java` | 51 | record EvaluationDatasetVersion | Immutable manifest and source snapshot for one evaluation dataset version. |
| `EvaluationMetric.java` | 37 | record EvaluationMetric | Immutable metric evidence emitted by one completed evaluation run. |
| `EvaluationMetricDirection.java` | 25 | enum EvaluationMetricDirection | Whether a larger or smaller metric value represents better quality. |
| `EvaluationMetricPolicy.java` | 47 | record EvaluationMetricPolicy | Tenant-scoped, immutable metric rules selected by an evaluation run's policy version. |
| `EvaluationMetricRule.java` | 44 | record EvaluationMetricRule | Immutable numerical release rule for one named metric. |
| `EvaluationReleaseDecision.java` | 42 | record EvaluationReleaseDecision | Immutable release-gate decision tied to a completed, version-pinned run. |
| `EvaluationRun.java` | 77 | record EvaluationRun | Version-pinned evaluation execution and its single terminal outcome. |
| `EvaluationRunState.java` | 31 | enum EvaluationRunState | Durable lifecycle of an evaluation run. |
| `EvaluationStore.java` | 70 | interface EvaluationStore | Durable contract for evaluation datasets, runs, evidence, and release gates. |
| `EvaluationSubjectType.java` | 30 | enum EvaluationSubjectType | Governed infrastructure subject that can be evaluated. |
| `EvaluationValidation.java` | 42 | class EvaluationValidation | Package-private validation helper: dataset/metric policy/run evidence checks and SHA-256 validation (no class Javadoc). |
| `JdbcEvaluationReleaseGate.java` | 164 | class JdbcEvaluationReleaseGate | Database-local validation for release evidence pinned to a baseline run. |
| `JdbcEvaluationStore.java` | 626 | class JdbcEvaluationStore | PostgreSQL implementation of version-pinned evaluation metadata and release gates. |
| `JdbcFeatureViewEvaluationEvidence.java` | 120 | class JdbcFeatureViewEvaluationEvidence | Reads only atomically published Feature View output metadata for evaluation. |

### 3.15 Materialization 内核持久化

`materialization` 是统一 Dynamic Table/Materialized View 内核的 JDBC 实现：`JdbcMaterializationRepository` 负责 object/definition/attempt/generation/schedule/command/reader-pin/GC 的写路径；`JdbcMaterializationQueries`/`Writes`/`Rows`/`Sql`/`Publications`/`ReaderPins` 分别负责读、写、行映射、连接/事务、发布证据、reader pin；`JdbcMaterializationCatalog` 是已发布读模型；`MaterializationJson` 是稳定 JSON 编解码。依赖 `pangrova-materialization` 的 domain/port（§4.1.1）。


**`materialization/` — 9 files, 3187 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcMaterializationCatalog.java` | 155 | class JdbcMaterializationCatalog | Durable read model of published materializations. |
| `JdbcMaterializationPublications.java` | 435 | class JdbcMaterializationPublications | Commit-evidence and publication authority of the materialization kernel. |
| `JdbcMaterializationQueries.java` | 336 | class JdbcMaterializationQueries | Read-side JDBC queries of the materialization repository. |
| `JdbcMaterializationReaderPins.java` | 191 | class JdbcMaterializationReaderPins | Lease and reader-pin statements of the materialization repository. |
| `JdbcMaterializationRepository.java` | 907 | class JdbcMaterializationRepository | PostgreSQL authority of the unified materialization kernel. |
| `JdbcMaterializationRows.java` | 515 | class JdbcMaterializationRows | Column projections, row mapping, and state-code translation for the |
| `JdbcMaterializationSql.java` | 94 | class JdbcMaterializationSql | Connection, statement, and transaction plumbing shared by the materialization |
| `JdbcMaterializationWrites.java` | 314 | class JdbcMaterializationWrites | Insert and update statements shared by the materialization repository. |
| `MaterializationJson.java` | 240 | class MaterializationJson | Stable JSON codecs for materialization persisted structures. |

### 3.16 对象依赖图

`dependency` 是 catalog 对象依赖图：`ObjectRef`/`ObjectKind`/`ObjectDependency` 定义四段引用、对象类型和有向边；`ObjectDependencyStore` 有 JDBC/InMemory 实现；`ObjectDependencyService` 提供记录/替换/删除策略（RESTRICT/CASCADE/WARN）和 `MetadataMaintenanceHook` 回调。


**`dependency/` — 10 files, 1108 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `CascadePolicy.java` | 36 | enum CascadePolicy | Policy applied when a parent object with dependents is dropped. |
| `DependencyViolationException.java` | 43 | class DependencyViolationException | Thrown when an attempted DROP would orphan one or more dependent objects |
| `InMemoryObjectDependencyStore.java` | 197 | class InMemoryObjectDependencyStore | In-memory {@link ObjectDependencyStore}. |
| `JdbcObjectDependencyStore.java` | 257 | class JdbcObjectDependencyStore | JDBC-backed {@link ObjectDependencyStore} persisting edges to the |
| `MetadataMaintenanceHook.java` | 57 | interface MetadataMaintenanceHook | Callback interface for metadata maintenance operations triggered by |
| `ObjectDependency.java` | 30 | record ObjectDependency | A directed dependency edge: {@code child} depends on {@code parent}. |
| `ObjectDependencyService.java` | 283 | class ObjectDependencyService | High-level facade over {@link ObjectDependencyStore} that adds policy |
| `ObjectDependencyStore.java` | 96 | interface ObjectDependencyStore | Persistent store for the catalog's object-dependency graph. |
| `ObjectKind.java` | 58 | enum ObjectKind | Categorization of catalog objects participating in a dependency edge. |
| `ObjectRef.java` | 51 | record ObjectRef | Four-part identifier for a catalog object that may participate in a |

### 3.17 质量规则持久化

`JdbcQualityRuleStore` 实现 governance 的 `IdentityQualityRuleStore`：`quality_rules`（规则 upsert + ruleId 跨表复用防护）、`quality_check_results`（追加写 + 数据库发号）、`quality_rule_identity`（身份表）。它在 metadata 中，但接口/领域在 governance。


**`governance/` — 1 files, 390 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `JdbcQualityRuleStore.java` | 390 | class JdbcQualityRuleStore | JDBC-backed implementation of {@link QualityRuleStore}. |

### 3.18 AI 基础设施装配

`AiInfrastructureStores` 是 AI 基础设施 Store 的三元工厂，被 coordinator 的 materialization wiring 和 E2E 使用；它没有业务逻辑，拆域后应移到 bootstrap 或对应模块。


**`ai/` — 1 files, 51 lines**

| 文件 | 行数 | 类型 | 职责（代码 Javadoc / 结构） |
|---|---:|---|---|
| `AiInfrastructureStores.java` | 51 | record AiInfrastructureStores | Production metadata entry point for governed AI infrastructure. |


---

## 4. 横切审计

### 4.1 依赖与模块边界

#### 4.1.1 POM 依赖 vs 架构矩阵

`pangrova-metadata/pom.xml` 当前依赖：

| 依赖 | 是否在 `architecture.md` metadata 允许列表 | 说明 |
|---|---|---|
| `pangrova-spi` | 是 | 契约层 |
| `pangrova-common` | 是 | 基础工具 |
| `pangrova-security` | 是 | 授权/租户模型 |
| `pangrova-governance` | 是 | 质量规则/标签接口 |
| `pangrova-materialization` | **是（architecture.md 当前允许）** | 但 metadata 承载 materialization 的 JDBC adapter；问题不是 POM 越界，而是 adapter 所有权/失败半径，详见治理计划 |
| `iceberg-api` / `iceberg-core` | **否** | P0 C1：Iceberg 只允许存在于自己的 connector 模块；主代码零 import，属于未使用依赖 |
| Jackson/HikariCP/Flyway/PostgreSQL/Caffeine/Curator/ZooKeeper | 基础设施 | 合理，但 Curator/ZooKeeper 为 optional，需确认打包边界 |

`pangrova-materialization` 自身的 POM 只依赖 `common/spi`，因此不存在 Maven 循环；当前矩阵也允许 `metadata → materialization`。问题不是矩阵越界，而是层次与宿主：domain kernel 的 JDBC adapter 放在 metadata，使 metadata 成为所有域 adapter 的宿主。正确方向是 adapter 依赖 port，归 materialization/各自 owner。

#### 4.1.2 依赖规则没有被执行

`ModuleDependencyPropertyTest`：

- `PACKAGE_TO_MODULE` 包含 `org.pangrova.metadata -> pangrova-metadata`；
- 但 `ALLOWED_DEPS` **没有 `pangrova-metadata` 条目**；
- `CONSTRAINED_MODULES = ALLOWED_DEPS.keySet()`，所以 metadata 的 import 不被任何规则约束；
- 文档注释仍写 “18 modules post-consolidation”，而根 POM 已有 25 个 `<module>` entry。
- `.pangrova/modules.yaml` 当前不存在，`exports/friendExports/allowedImports` 的 default-deny 规则没有落地。

结论：metadata 是当前仓库最大的依赖 hub 之一，却恰好不在依赖门禁覆盖范围内。这解释了 `pangrova-materialization`、Iceberg 和跨域实现为何能长期存在。

#### 4.1.3 域实现放错模块

| 域 | 领域模块中已有 | metadata 中承载 | 结果 |
|---|---|---|---|
| Index | `pangrova-indexing`：`IndexLifecycleListener`、`IndexEngineRegistry`、`IndexStorageManager`、`BreakerStateStore` 等 64 个类 | `index/`、`index/jdbc/`、`index/model/` 共 27 个文件、4,353 行 | 索引的**元数据/发布/commit queue**在 metadata，索引的**执行/存储/评估**在 indexing；两个模块各自维护一半 |
| Streaming | `pangrova-streaming`：`JdbcCdcRegistrationStore`、`JdbcCdcCommitStateStore`、CDC/聚合等 87 个类 | `streaming/` 8 个文件、1,120 行 | `JdbcCdcRegistrationStore`、`JdbcWatermarkStore` 在两个模块同名；生产实际 wire 的是 metadata 版本 |
| Governance | `pangrova-governance`：`QualityRuleStore`、`IdentityQualityRuleStore`、`InMemoryQualityRuleStore`、`JdbcColumnTagStore` | `governance/JdbcQualityRuleStore` 390 行 | 接口在 governance，JDBC 实现在 metadata；`JdbcColumnTagStore` 又在 governance 且无生产构造 |
| Tenant | `pangrova-security`：`TenantStore`、`InMemoryTenantStore`、租户模型/配额 | `tenant/JdbcTenantStore` 437 行 | JDBC 适配器在 metadata，领域模型在 security |
| Materialization | `pangrova-materialization`：port/domain/extension 共 113 个类 | `materialization/` 9 个文件、3,187 行 | 领域内核在 materialization，持久化在 metadata；metadata POM 因此依赖 materialization |
| Artifact | 无独立领域模块 | `artifact/` 全部 59 个文件、7,287 行 | artifact 归 `indexing.control`（index generation 的一部分），不新建独立 artifact |
| File/Storage/Source | 无独立存储领域模块 | `file/`、`storage/`、`source/jdbc/` 共 11 个文件、3,143 行 | file access/storage profile → security；COPY admission → query.transaction；managed source identity → metadata |

#### 4.1.4 跨域表访问（共享数据库，不是共享 schema ownership）

通过 SQL 字符串解析得到的跨包表引用（节选）：

| 读取方包 | 访问的其他域表 | 证据 |
|---|---|---|
| `coordination` | `index_commit_log`、`materialization.attempts` | `JdbcWriteCommitJournal.java:144, 383-384` |
| `artifact.jdbc` | `index_descriptors`、`index_generation_builds`、`index_generation_manifests`、`write_commit_journal`、`pangrova_embedding_models`、`execution_anchor` | `JdbcIndexArtifactPublicationStore`、`JdbcGeneratedValue*`、`ArtifactExecutionFence` |
| `artifact.jdbc.build` | `artifact_stage_exchanges`、`execution_anchor`、`artifact_build_*` | `JdbcArtifactBuildStoreSupport`、`JdbcArtifactStageExecutionStore` |
| `evaluation` | `tabular_export_generations`、`catalog_export_requests`、`feature_view_versions`、`artifact_build_jobs` | `JdbcFeatureViewEvaluationEvidence.java:37-54` |
| `storage` | `distributed_lease`、`pangrova_authorization_revision` | `JdbcCopySourceReadAdmissions` |
| `source.jdbc` | `catalog_definitions` | `JdbcSourceNamespace.java:19` |
| `service` | `table_control_registry`、`catalog_definitions`、`table_control_name_reservation` | `TableControlDeferredPolicy.java:43-49` 等 |

这些引用目前是“同库直读”，没有 SPI/read model 隔离。拆分 schema 时，每一处都必须改为接口或投影，否则拆不动。

#### 4.1.5 命名与契约违规

| 规则 | 违规 | 证据 | 处理 |
|---|---|---|---|
| C1 `Arrow` banned in metadata | 格式 ID `"pangrova.tabular-export.arrow-stream"` | `artifact/contract/ArtifactFormatContracts.java:40` | 改为 `columnar-stream` 或 `tabular-stream` |
| C1 `Iceberg` banned outside connector | POM 声明 `iceberg-api/core`，主代码零 import | `pangrova-metadata/pom.xml:42-47` | 删除依赖 |
| C2 共享简单类名 | `L2CacheStore`、`InMemoryL2CacheStore`（metadata vs planner）；`JdbcCdcRegistrationStore`、`JdbcWatermarkStore`（metadata vs streaming） | 同名类扫描 | 保留 SPI 侧名称，实现侧改名/移动；删除重复实现 |
| C11 GoF 后缀 | `TableVersionObserver` | `sync/TableVersionObserver.java` | 改为 `TableVersionReconciler` |
| C11 注释自称 Decorator | `CachingFederatedCatalog` Javadoc “Decorator that adds ...” | `adapter/CachingFederatedCatalog.java:18` | 改名 `CachingFederatedCatalogView` 或删除（生产零引用） |
| SPI 注解 | 296 个文件中仅 3 个带 `@Stable/@Experimental/@Internal` | `grep -rl` | 为 43 个 interface 补齐注解；`@Stable` 需有生产实现 |
| 无 `@Experimental` 的桩 | 16 处 `UnsupportedOperationException` | `IndexMetadataStore` 8 处、`CatalogHandle` 2 处、`MetadataService` 2 处、`JdbcStorageProfileRegistry` 1 处等 | 要么实现，要么 `@Experimental` + tracking issue，要么删除 |
| 无过程引用 | `F-META-01`、`F-META-002`、`F-META-03`、`G-META-02`、`G-META-06`、`A-HA-01`×3、`Task 38`、`BUG-09`、`BUG-005` | `grep -rnoE` | 删除任务号/阶段标签，改为行为说明 |
| 注释与规则不符 | `CatalogDescriptor` 注释声称“≤4 构造参数豁免”，Java 规则是构造依赖 ≤6/方法参数 ≤7 | `catalog/CatalogDescriptor.java:11-16` | 修正注释或拆分值对象 |
| Javadoc 缺失 | `EvaluationValidation`、`SemanticViewValidation` 无类级 Javadoc | 对应文件 | 补齐 |

### 4.2 体量与复杂度

#### 4.2.1 超 500 行类（12 个，复杂度信号）

> code-standards 的 file 800 行是 review 诊断；≤500 是内部拆分目标，不是 build gate。2 个 >800 的行触发 review 诊断。

| 文件 | 行数 | 主要职责 | 拆分方向 |
|---|---:|---|---|
| `materialization/JdbcMaterializationRepository.java` | 907 | 生命周期写、GC、publish、schedule、reader pin、查询委托 | 拆为 Object/Definition、Attempt/Generation、Publication、Schedule/ReaderPin、GC 五个协作类；Repository 只做门面 |
| `control/TableControlService.java` | 827 | 身份观测、注册/adopt/unlink、rename、写/DDL 准入、catalog 校验 | 拆为 `TableIdentityService`、`TableAdmissionService`、`TableMaintenanceService`、`TableControlQueryService` |
| `storage/JdbcCopySourceReadAdmissions.java` | 768 | COPY 源读签发/兑换、frozen input、staged source、profile 校验 | 拆为 `CopyReadTicketIssuer`、`CopyReadTicketRedeemer`、`FrozenInputLineage`、`StagedSourceRecorder` |
| `source/jdbc/JdbcManagedSourceCatalog.java` | 766 | reserve/issuePreparation/registerObjects/accept/abort | 拆为 `ManagedSourceReservation`、`ManagedSourcePreparation`、`ManagedSourceRegistration`、`ManagedSourcePublication` |
| `coordination/JdbcWriteCommitJournal.java` | 721 | intent/publication/abort/replay/purge/copy publication | 拆为 `WriteCommitIntentStore`、`CopyPublicationStore`、`WriteCommitReplay`、`JournalSqlMapper` |
| `index/jdbc/JdbcIndexDescriptorStore.java` | 627 | descriptor CRUD、generation 预留、授权、advisory lock | 拆为 `IndexDescriptorCrud`、`IndexGenerationReservation`、`IndexBuildAuthority` |
| `evaluation/JdbcEvaluationStore.java` | 626 | dataset/policy/run/metric/release gate | 拆为 `EvaluationDatasetStore`、`EvaluationRunStore`、`EvaluationPolicyStore`、`ReleaseGateStore` |
| `service/DefaultMetadataService.java` | 571 | catalog 生命周期、表 DDL、解析、tag、查询生命周期、指标 | 拆为 `MetadataGateway` use-case ports（见治理计划 §3.6） |
| `index/IndexMetadataStore.java` | 537 | 50+ 方法 + 8 个默认 UOE | 拆为 Descriptor/Mapping/Maintenance/CommitQueue/Statistics/Publication 六个接口 |
| `artifact/jdbc/build/JdbcArtifactBuildStore.java` | 533 | 20 方法构建控制面 | 拆为 Job、Page/Lease、Completion、Reader 四个存储 |
| `coordination/JdbcExecutionAnchorStore.java` | 517 | 获取/续租/释放/requireCurrent | 拆为 `ExecutionAnchorLease` 与 `ExecutionAnchorQuery` |
| `materialization/JdbcMaterializationRows.java` | 515 | 行映射、状态码、投影 | 按 aggregate 拆为 Object/Attempt/Generation/Schedule 映射器 |

#### 4.2.2 接口规模

| 接口 | 方法数（约） | 问题 |
|---|---:|---|
| `service/MetadataService.java` | 17 | 混合 catalog 生命周期、schema 浏览、表解析、DDL、query lifecycle、tag、rename；7 个 default 返回空/UOE |
| `index/IndexMetadataStore.java` | 50+ | 8 个 UOE；descriptor/mapping/GC/stats/commit queue 混在一起；default 方法内实现业务逻辑 |
| `artifact/build/ArtifactBuildStore.java` | 20+ | 创建/领取/完成/发布/取消/查询混在一起 |
| `catalog/CatalogHandle.java` | 11 | 读/写/健康/统计混合；2 个 default UOE |
| `store/MetadataStore.java` | 13 | 表、catalog、sync history、schema 四个聚合混在一起 |
| `feature/FeatureCatalogStore.java` | 多个内嵌 record + 方法 | 接口内嵌全部记录类型，契约与数据模型耦合 |
| `control/TableControlService` | 约 40 个 public 方法 | 不是接口，但同样是事实上的 God class |

#### 4.2.3 复杂度分布

- 296 个主文件中，61 个 >200 行、43 个 >300 行、23 个 >400 行、12 个 >500 行、2 个 >800 行。
- 大部分值对象很小，复杂度集中在 12 个持久化/控制面类；这意味着拆分收益高、范围可控。
- 大量 `Jdbc*` 类同时承担 SQL 拼接、行映射、事务、授权、状态机、业务校验六种职责；典型如 `JdbcMaterializationRepository`、`JdbcManagedSourceCatalog`、`JdbcCopySourceReadAdmissions`。

### 4.3 正确性、并发与事务

| 编号 | 发现 | 证据 | 影响 | 修复方向 |
|---|---|---|---|---|
| C-01 | `CatalogRegistry` 默认装配内存控制面/统计/治理绑定 | `CatalogRegistry.java:63-64, 87-92` | 未显式覆盖的部署静默丢失 ownership；文档要求生产必须用 `JdbcTableControlStore` | 构造器强制注入或 fail-closed；仅在显式 `metadata-less` 模式允许 InMemory |
| C-02 | 事件队列满即丢弃 | `MetadataEventDispatcher.java:41-44` | 缓存失效丢失；`eventsDropped` 无人导出 | 改为阻塞/背压或持久化 outbox；至少暴露指标并告警 |
| C-03 | `TieredMetadataCache.tableVersions` 无界，失效不清 | `TieredMetadataCache.java:84, 175`；`invalidateCatalog` 只清 `catalogIndex` | 长跑内存增长；版本号残留 | 失效时同步 remove；加最大容量/过期；补并发测试 |
| C-04 | 双缓存 | `DefaultMetadataService` 的 `resolvedTableCache` + `TieredMetadataCache` | 两条失效路径；一致性难以推理 | 只保留一个版本化缓存，另一个降为同源的 L1 视图 |
| C-05 | `resolveTable` 连续两次 `control.find` | `DefaultMetadataService.java:229-240` | 额外 DB 往返；两次结果可能不同 | 一次读取，复用结果 |
| C-06 | 全局 `alterTableLock` 包住连接器 I/O | `DefaultMetadataService.java:80, 332-344` | 所有表的 ALTER 串行化；锁内做 I/O | 按 tableId 分段锁；或交给 connector 的 schema 锁 |
| C-07 | 多变更 ALTER 非原子 | `applySchemaChangesToConnector` 逐条执行 | 第二条失败时第一条已生效，无补偿 | 引入 change-set 事务/补偿；至少记录已完成变更并明确失败语义 |
| C-08 | `dropCatalog` 忽略 `cascade` | `DefaultMetadataService.java:198` | API 契约与行为不符 | 实现 cascade 或删除参数并在接口文档声明 |
| C-09 | `updateSyncTime` 空实现，水位实际由 `recordSyncHistory`/`sync_history` 维护 | `JdbcMetadataStore.java:181-183`；`getLastSyncTime` 读 `MAX(sync_time)` | 两套语义；调用方若依赖 `updateSyncTime` 会静默无效，未来替换实现易出错 | 删除该 SPI 方法，或让它成为唯一写入口 |
| C-10 | 逐条同步吞异常 | `MetadataSyncService.java:122-126, 223-227` 捕获后只存 `e.getMessage()`；仅重试耗尽路径记录 cause | 失败项缺少堆栈/错误码；批量同步难以定位 | 每项带 `ErrorCode` + cause；失败项可重试 |
| C-11 | 10% 随机漂移抽样 | `MetadataSyncService.java:144` | 非确定；90% 漂移漏检 | 改为确定性分片/全量 reconcile 窗口 |
| C-12 | 不兼容 schema 只 `LOG.error` 仍派发事件 | `MetadataSyncService.java:269` | 下游可能消费到不兼容变更 | fail closed；先验证再派发 |
| C-13 | `DdlPropagationWaiter` 类级 Javadoc 说超时“returns success anyway”，方法 Javadoc/实现返回 false；targetId 取本地 `lastProcessedId` 而非本次写入 id | `DdlPropagationWaiter.java:19-20, 54-74` | 等待逻辑可能无效且零生产引用 | 删除；若保留则修正目标 id 和返回值 |
| C-14 | `CatalogHealthMonitor.stop()` 不 await；failureCounts 注销不清 | `CatalogHealthMonitor.java:20-21, 48` | 线程泄漏/状态泄漏 | 加 awaitTermination；注销时清理计数 |
| C-15 | `CatalogState.CREATING/DROPPING` 从未赋值 | `CatalogState.java` | 生命周期状态机不完整 | 实现过渡态或删除枚举值 |
| C-16 | `JdbcCoordinatorRegistry.mapRow` 捕获 SQLException 后默认 -1 | `JdbcCoordinatorRegistry.java:226-235` | 掩盖 schema 漂移；leader 判定错误 | 缺少列应 fail fast 或显式迁移 |
| C-17 | watermark/offset 无条件覆盖 | `JdbcWatermarkStore.java:31-34`；`JdbcStreamOffsetStore.java:54-57` | 旧写可回退 | 改为 `GREATEST(existing, excluded)` 或 CAS + 拒绝回退 |
| C-18 | `TableControlStatisticsService` 读-改-写 statsVersion | `TableControlStatisticsService.java:112-126` | 并发 ANALYZE 丢更新 | 原子 `UPDATE ... SET version=version+1 RETURNING` |
| C-19 | `JdbcFileSpaceAuthority.read` 持锁后回调可能另开连接 | `JdbcFileSpaceAuthority.java:29-53` | “覆盖完整出票”并非同一快照 | 把回调改为同连接参数，或明确只保证锁序 |
| C-20 | `collectGeneration` 事务内执行 `physicalDrop.run()` | `JdbcMaterializationRepository.java:565-604`（583） | 长事务/锁内 I/O；外部副作用不可回滚 | 先提交 DB 状态，再执行物理删除；失败进入补偿表 |
| C-21 | `JdbcFeatureCatalogStore` 直接 `json.get("schema").toString()` | `JdbcFeatureCatalogStore.java:224, 242` | 缺字段即 NPE | 显式校验并抛带 ErrorCode 的异常 |
| C-22 | `JdbcSemanticViewStore` 用 `IllegalStateException` 表达版本非法/行缺失 | `JdbcSemanticViewStore.java:213-216` | 错误模型不一致，调用方无法按错误码处理 | 统一 `MetadataException`/`PangrovaException` + ErrorCode |
| C-23 | `JdbcEvaluationStore` 内嵌完整 release gate 逻辑 | `JdbcEvaluationStore.java:457-505` | 业务规则下沉到仓储，难测试/复用 | 抽出 `ReleaseGateEvaluator` 领域服务 |
| C-24 | artifact 发布三套复制 SQL + 三份 manifest digest；tabular 的 `manifestDigest` 未包含 `output.encryptionKeyRef()`（generated 版本包含） | `JdbcGeneratedValueArtifactPublicationStore.java:313-322`；`JdbcTabularExportPublicationStore.java:325-334`；`ArtifactPublicationManifestDigest.java:59` | 同名字段语义分叉；加密输出摘要不完整 | 统一泛型发布模板 + 单一 digest 实现 |
| C-25 | 两个 index 发布实现 | `index/jdbc/JdbcIndexPublicationStore.java:32` 与 `artifact/jdbc/JdbcIndexArtifactPublicationStore.java:33` | 同一代际发布逻辑两套 | 合并为一个，另一处改为委托 |
| C-26 | `IndexMetadataStore` default 方法实现 copy/batchGet/selection，JDBC 实现又覆写 | `IndexMetadataStore.java:230-252, 290-325`；`JdbcIndexFileMappingStore.java:180-199` | 三套选择语义 | 删除 default 业务逻辑；接口只留契约 |
| C-27 | `TableMetadata` 转换丢失 nullable/tags/partition/sort | `DefaultMetadataService.java:540-547` | 通用模型信息丢失；下游误判 | 使用无损 descriptor 或补齐字段 |
| C-28 | `FederationConnectorMetadata` 未校验直接强转 | `FederationConnectorMetadata.java:50` | ClassCastException | `instanceof` 校验 + 明确异常 |
| C-29 | `InternalCatalogHandle` 3 个方法恒抛 | `InternalCatalogHandle.java:42, 47, 52` | 半实现 | 实现或声明 `@Experimental` |
| C-30 | `JdbcStorageProfileRegistry.register` 恒抛 | `JdbcStorageProfileRegistry.java:43-45` | SPI 契约不完整 | 删除 `register` 或改为 `createIfAbsent` 的唯一入口 |
| C-31 | `JdbcCopyFileClaimStore` `@DeadCode`，但 journal 依赖 `copy_file_claims` | `JdbcCopyFileClaimStore.java:19`；`JdbcWriteCommitJournal.java:248` | COPY 写意图无法记录 | 接线 claim store，或移除 journal 的 copy 绑定分支 |
| C-32 | `JournalFlushOwner.awaitCommit` 1 秒超时后无限重试 | `JournalFlushOwner.java:40-51` | 调用方可能永久阻塞 | 增加总截止时间/可中断策略 |
| C-33 | `ZooKeeperCoordinatorRegistry` 用 `replace('/', '_')` 构造路径；`ZooKeeperDistributedLease` 用 Base64 防别名 | `ZooKeeperCoordinatorRegistry.java:260-262`；`ZooKeeperDistributedLease.java:246-254` | coordinatorId 含 `/` 时路径冲突；两套编码不一致 | 统一 Base64/URL-safe 编码 |
| C-34 | ZK 路径全部 fail-safe 返回空，JDBC 路径抛异常 | `ZooKeeperCoordinatorRegistry` | 调用方无法区分“无 leader”与“ZK 故障” | 明确结果类型（Optional vs error） |
| C-35 | `JdbcTableControlGovernanceBindingStore` 多个写路径无 rollback | `JdbcTableControlGovernanceBindingStore.java:64-85, 135-149, 152-164` | 部分写留痕 | 统一事务模板 |
| C-36 | `JdbcQualityRuleStore`、`JdbcTenantStore`、`JdbcWriteCommitJournal`、`JdbcCdcRegistrationStore` 构造函数建表 | 8 处 `CREATE TABLE IF NOT EXISTS` | 与 Flyway 双源；旧结构不更新 | 删除构造 DDL，迁移为唯一 owner |
| C-37 | `JdbcColumnTagStore`（governance）无生产构造，metadata tag 权威在 control binding | `grep new JdbcColumnTagStore` = 0；`MetadataTagOperations` 使用 `table_control_governance_binding` | 两套 tag 概念，易误用 | 删除未接线实现，或明确 legacy 并迁移 |
| C-38 | `GovernanceWiring` 自述 “Legacy thin wrapper ... retained because tests still construct one” | `governance/GovernanceWiring.java` | 违反“无向后兼容 shim” | 迁移调用方后删除 |

### 4.4 数据与迁移

#### 4.4.1 迁移与表归属

| 目标 schema | 代表表 | 当前迁移来源（含重叠） | 目标 owner |
|---|---|---|---|
| `metadata` | `catalog_definitions`、`catalog_descriptor_revisions`、`table_metadata`、`schema_metadata`、`sync_history`、`invalidation_log`、`table_control_*`、`pangrova_object_dependencies` | V1、V9、V34、V35、V42、V86–V92、V97–V101 | `pangrova-metadata` core |
| `coordination` | `coordinator_registry`、`worker_registry`、`distributed_lease`、`execution_anchor`、`execution_anchor_token_seq`、`query_state_journal`、`task_state_journal`、`query_lineage_outbox`、`write_commit_journal`、`write_commit_quarantine`、`copy_file_claims`、`copy_file_publications`、`checkpoints` | V1、V3、V4、V8、V23、V24、V33、V44、V49、V50、V52、V57、V58、V61、V78、V82、V91 | `query.transaction` / `coordinator.coordination` |
| `index` | `index_descriptors`、`index_file_mappings`、`index_generation_builds`、`index_generation_manifests`、`index_commit_log`、`index_commit_table_heads`、`index_build_claims`、`index_cache_events`、`index_eval_breaker_state`、`file_column_statistics` | V1、V17、V18、V28、V31、V36–V38、V54、V55、V64 | `pangrova-indexing` |
| `artifact` | `artifact_build_*`、`artifact_exchange_*`、`artifact_stage_exchanges`、`artifact_execution_routes`、`generated_value_*`、`tabular_export_generations`、`catalog_export_requests` | V14、V19–V22、V25、V27、V29、V40、V41、V43、V46、V47 | `indexing.control` |
| `materialization` | `materialization.*`、`dynamic_table_*` | V60、V66、V68–V72、V83–V85 | `materialization.store.jdbc` |
| `streaming` | `cdc_registrations`、`cdc_commit_state`、`ingestion_stream_state`、`ingestion_stream_offsets`、`watermark_positions` | V1、V5、V6、V15、V39、V73、V75 | `pangrova-streaming` |
| `governance` | `quality_rules`、`quality_check_results`、`quality_rule_identity` | V1、V2、V53、V67、V74、V101 | `pangrova-governance` |
| `ai` | `feature_view_*`、`training_dataset_versions`、`semantic_view_*`、`evaluation_*` | V26、V29、V32、V45、V48、V62、V63、V67、V74 | `pangrova-ai` / `semantic` |
| `storage` | `file_space_*`、`file_directory_*`、`storage_profile_*`、`copy_source_read_admissions`、`managed_source_*` | V51、V56、V59、V65、V93–V96 | `security` / `query.transaction` |
| `tenant` | `tenants`、`tenant_user_memberships` | V1、V4 | `pangrova-security` |
| `security` | `pangrova_authorization_revision`、`pangrova_embedding_models` | V10、V32、V45、V48、V59 | `security` / `pangrova-ai` |
| `observability` | `query_audit_records` | V7 | `pangrova-observability` |

注意：V1/V2/V4/V7 等迁移横跨多个域，不能简单按“文件版本段”归属；必须先按表做一次所有权盘点（见治理计划 §5.3、§5.4）。

#### 4.4.2 迁移完整性风险

- 版本缺口：V11–13、V16、V76–77、V79–81，共 9 个版本。
- `SchemaMigrationRunner` 配置 `baselineOnMigrate(true)` + `baselineVersion("0")`，没有 `ignoreMigrationPatterns`、`outOfOrder`、`cleanDisabled` 等显式策略。
- 新项目无外部部署/生产数据：旧 92 个迁移和 history 全部删除，按 owner 重建 baseline；不存在 upgrade 兼容问题。版本缺口只作为 churn 证据保留。
- 没有 per-domain history；所有域共享 `flyway_schema_history`。Clean-cut 后改为 per-owner baseline + 独立 history。
- 没有 repeatable migration（`R__`），视图/函数/触发器只能靠版本迁移维护。
- 构造函数 DDL 与 Flyway 并存：`JdbcQualityRuleStore` 3 处、`JdbcTenantStore` 2 处、`JdbcWriteCommitJournal` 2 处、`JdbcCdcRegistrationStore` 1 处。`IF NOT EXISTS` 不会更新旧表结构，后续 INSERT 列顺序/列名变化会直接失败。

### 4.5 测试

#### 4.5.1 测试分布

| 包 | 测试文件 | 行数 | 备注 |
|---|---:|---:|---|
| `coordination` | 21 | 4,496 | 覆盖最好；含 Postgres/并发/时钟测试 |
| `artifact` + `artifact/jdbc` + `artifact/jdbc/build` | 16 | 3,183 | 契约与 JDBC 发布测试较多 |
| `migration` | 15 | 1,145 | 迁移 baseline/升级历史测试；默认 integration 排除 |
| `service` | 10 | 2,059 | 覆盖 resolve/DDL/control/cache |
| `control` | 10 | 2,145 | 覆盖 ownership/maintenance/statistics |
| `catalog` | 4 | 605 | catalog 生命周期/健康/handle |
| `index` + `index/jdbc` + `index/model` | 8 | 1,198 | 主要是 JDBC/发布 |
| `sync` | 7 | 1,712 | 覆盖 sync/invalidation/drift |
| `governance` | 6 | 828 | 质量规则/身份迁移 |
| `cache` | 6 | 1,242 | 缓存/失效 |
| `store` | 4 | 919 | JDBC store/invalidation |
| `streaming` | 4 | 700 | 含 2 个 property test |
| `storage` | 3 | 1,279 | COPY 准入/profile |
| `file` | 3 | 643 | 文件空间/目录 |
| `dependency` | 3 | 461 | 依赖图 |
| `evaluation` | 2 | 690 | 仅 2 个测试文件 |
| `adapter` | 2 | 507 | 联邦适配 |
| `semantic` | 1 | 197 | 仅 1 个 |
| `feature` | 1 | 155 | 仅 1 个 |
| `materialization` | 1 | 656 | 3,187 行主代码仅 1 个测试文件 |
| `tenant` | 1 | 192 | 仅 1 个 |
| `schema` | 1 | 289 | 仅 1 个 |
| `serde` | 1 | 182 | 仅 1 个 |
| `event` | 1 | 38 | 仅 1 个 |
| `model`（含 asset） | 1 | 225 | 仅 1 个 |
| `property/streaming` | 2 | 390 | 流式状态 property test |
| 合计（`*Test.java`） | 134 | 26,136 | 另有 7 个 fixture/helper，合计 141 个 test 源码文件、26,666 行 |

#### 4.5.2 覆盖缺口

- **默认不跑 integration**：`pangrova-metadata/pom.xml` 的 surefire `excludedGroups=${pangrova.metadata.excludedTestGroups}`，根 POM 默认值为 `integration`；只有 `full-regression` profile 设为 `none`。因此 15 个 `@Tag("integration")` 文件和 86 个使用 Testcontainers/Postgres 的测试在普通 `mvn test` 中不执行。
- **AI 域明显偏薄**：materialization 3,187 行主代码 / 1 个测试文件；semantic 631 / 1；feature 616 / 1；evaluation 1,434 / 2。
- **状态机缺少 property test**：artifact build、index generation publication、materialization publish、journal ordering、copy read admission 都没有 property-based 测试；只有 streaming 有 2 个。
- **约 228 个主类没有同名 `*Test`/`*IntegrationTest`**（粗略匹配，很多是值对象，不能直接等同于未覆盖）；但结合上一条，持久化/状态机类的覆盖不足是明确的。
- **缺少 schema ownership 测试**：没有测试断言“某个包只能访问自己 schema 的表”；跨域 SQL 直读没有任何门禁。
- **缺少迁移 owner/baseline 测试**：没有测试断言表 owner、per-owner baseline、空库启动。

### 4.6 文档与流程

| 问题 | 证据 | 影响 |
|---|---|---|
| `metadata.md` 第 11 节 module structure 仍是旧包/单 V1 迁移 | `docs/architecture/metadata.md:2342-2405` | 新成员按文档会找不到 `artifact/index/control/...` |
| `architecture.md` metadata 矩阵已包含 `pangrova-materialization`，但 `ModuleDependencyPropertyTest` 未约束 metadata，且 `.pangrova/modules.yaml` 不存在 | `.pangrova/rules/architecture.md`；`ModuleDependencyPropertyTest` | 规则存在但未执行；需要 manifest + ArchUnit default-deny |
| `ModuleDependencyPropertyTest` 仍写 “18 modules”，根 POM 已 25 个 module entry | `ModuleDependencyPropertyTest.java:26-57` | 门禁描述失真；metadata 不在约束内 |
| 代码中保留任务号/阶段标签 | `F-META-01`、`A-HA-01`、`Task 38` 等 11 处 | 违反 code-standards“no spec/design process references” |
| 模块无 README | `pangrova-metadata/` 下无 README | 缺少模块入口说明；只有 `docs/architecture/metadata.md` |
| 既有目标文档与代码脱节 | `metadata-center-redesign.md` 的 `api/domain/...` 结构未落地 | 设计与实现双轨 |

### 4.7 关键代码片段（证据）

**1) 默认 InMemory 控制面** — `catalog/CatalogRegistry.java:87-92`

```java
this.tableControl = new TableControlService(
        new InMemoryTableControlStore(),
        catalogIdentities,
        TableControlSettings.fromSystemProperties());
this.tableStatistics = new TableControlStatisticsService(
        this.tableControl, new InMemoryTableControlStatisticsStore());
```

生产 `CoordinatorBootstrap:300-311` 会覆盖为 JDBC 实现；但任何未覆盖的嵌入方式（测试、standalone、未来新 bootstrap）都会静默使用内存权威。

**2) 事件队列满丢弃** — `event/MetadataEventDispatcher.java:41-44`

```java
(r, pool) -> {
    long dropped = eventsDropped.incrementAndGet();
    LOG.warn("event=metadata_event_rejected droppedTotal={}", dropped);
});
```

`ArrayBlockingQueue` 的拒绝策略只计数 + WARN，没有背压、持久化或失败返回。

**3) 写日志要求 COPY claim 行** — `coordination/JdbcWriteCommitJournal.java:243-249`

```java
try (PreparedStatement ps = connection.prepareStatement(
        "SELECT 1 FROM copy_file_claims WHERE claim_key=? AND load_generation=?")) {
    ...
}
```

而唯一写入 `copy_file_claims` 的 `JdbcCopyFileClaimStore` 被标注 `@DeadCode`（`JdbcCopyFileClaimStore.java:19`），生产未注入。

**4) watermark 无条件覆盖** — `streaming/JdbcWatermarkStore.java:31-34`

```sql
INSERT INTO watermark_positions (source_id, position, updated_at)
VALUES (?, ?, ?) ON CONFLICT (source_id) DO UPDATE SET
position=EXCLUDED.position, updated_at=EXCLUDED.updated_at
```

没有 `GREATEST`/版本比较，旧写可覆盖新值；`JdbcStreamOffsetStore` 的 `committed_offset` 同理。

**5) 事务内物理删除** — `materialization/JdbcMaterializationRepository.java:568-583`

```java
return sql.inTransaction("collectGeneration", connection -> {
    ...
    physicalDrop.run();
    ...
});
```

`physicalDrop` 是外部副作用（对象存储/文件系统），不在数据库事务的回滚范围内；失败时事务与外部状态可能分叉。

### 4.8 安全与多租户（专项检查）

| 检查项 | 现状 | 评价/动作 |
|---|---|---|
| 稳定身份与所有权 | `TableControlService.requireReadIdentity/requireManagedForWrite` 以 `table_id` + format UUID + catalog incarnation/revision 校验；不匹配抛 `AccessDeniedException` | 方向正确；但默认 InMemory 控制面（C-01）会让该保护静默失效，必须先修 |
| 会话转换 | `DefaultConnectorSession` 从 `MetadataSessionContext` 重建 `ConnectorSession`，先移除再写入 `tenant_id`，并携带 timezone/locale | 基本正确；`tenant_id` 是魔法字符串，建议提取常量并加测试防止属性注入 |
| 解析缓存键 | `MetadataResolutionCacheKey` 对 securityToken + connector properties 做 SHA-256，按 tenant/user/digest/tableRef 隔离 | 方向正确；SHA-256 不可用时抛 `IllegalStateException`，应改为启动期校验 |
| COPY 读票据 | `JdbcCopySourceReadAdmissions` 只存 nonce 哈希，绑定 execution anchor、lease epoch、tenant、profile、READ 权限 | 设计良好；需补过期/重放/跨租户测试（现有 3 个测试文件） |
| Storage profile | `JdbcStorageProfileRegistry` 拒绝携带 credentials 的 profile，要求 Worker-local secret reference | 正确；需确保历史 revision 不泄露 secret ref |
| Tag/策略权威 | `MetadataTagOperations` 以 `table_control_governance_binding` 为唯一权威；governance 的 `JdbcColumnTagStore` 未接线 | 权威清晰，但存在未接线实现（META-025）；`TableControlColumnMaskStore`/`RowPolicyStore`/`QualityRuleStore` 放在 `service/`，建议移到 `control/policy/` |
| 异常语义 | `CatalogAccessDeniedException`、`TableNotFoundException` 零引用；部分路径用 `IllegalStateException` | 统一错误模型（META-026/META-031），避免调用方按异常类型分支 |

---

## 5. 问题清单（可执行索引）

> 严重度：P0 = 发布前必须修；P1 = 架构债/正确性风险；P2 = 规范/可维护性。编号与 §4 对应，便于改造时逐项关闭。

| ID | 严重度 | 问题 | 证据 | 修复动作 | 验收 |
|---|---|---|---|---|---|
| META-001 | P0 | metadata 承载 materialization JDBC adapter（矩阵允许，但 adapter 所有权/失败半径错位） | `pom.xml:29`；`JdbcMaterializationRepository.java:15-40` | 把 adapter 移到 `materialization.store.jdbc`；metadata 不依赖实现；补 manifest/依赖 gate | metadata POM 无 materialization 实现依赖；依赖 gate 覆盖 metadata |
| META-002 | P0 | index/artifact/streaming/materialization/tenant/governance/file/storage/query audit 的适配器放在 metadata | §4.1.3、§4.4.1 | 移回各自 owner（indexing/streaming/materialization/security/governance/observability/query.transaction） | 各域拥有自己的 store；metadata 只留核心 |
| META-003 | P0 | 默认 InMemory 控制面 | `CatalogRegistry.java:63-64, 87-92` | 构造器强制注入或 fail-closed；增加 bootstrap 测试 | 未配置 JDBC 时启动失败（除非显式 metadata-less） |
| META-004 | P0 | 事件可丢弃 | `MetadataEventDispatcher.java:41-44` | 阻塞/背压或持久化 outbox；导出 `eventsDropped` | 队列满不丢事件；指标可观测 |
| META-005 | P0 | 缓存无界 + 双缓存 | `TieredMetadataCache.java:84, 175`；`DefaultMetadataService` | 单一版本化缓存；失效清理版本号；补并发测试 | 长跑内存有界；失效后无旧值 |
| META-006 | P0 | 双 DDL + 旧迁移债 | 8 处构造 DDL；§4.4.2 | 删除构造 DDL 和旧 V1–V101；重建 per-owner baseline | 空库可启动；0 构造 DDL；无旧 history |
| META-007 | P0 | 跨域表直读 | §4.1.4 | 改为 SPI/read model/outbox | 0 跨 schema SQL |
| META-008 | P0 | COPY claim 断链 | `JdbcWriteCommitJournal.java:248`；`JdbcCopyFileClaimStore.java:19` | 接线 claim store 或移除 copy 分支 | COPY 写意图可记录/恢复 |
| META-009 | P0 | catalog revision 栅栏无更新路径 | `JdbcMetadataStore.java:102-133`；`CatalogRegistry.register` | 实现 revision 自增更新路径，或删除更新能力 | 更新 catalog 有测试且成功 |
| META-010 | P0 | watermark/offset 可回退 | `JdbcWatermarkStore.java:31-34`；`JdbcStreamOffsetStore.java:54-57` | 单调 upsert 或 CAS 拒绝回退 | property test：旧值不会覆盖新值 |
| META-011 | P0 | 事务内物理删除 | `JdbcMaterializationRepository.java:583` | 提交后执行物理删除 + 补偿 | 无锁内 I/O；失败可恢复 |
| META-012 | P1 | 12 个超 500 行类（2 个 >800） | §4.2.1 | 按表拆协作类 | >800 = 0；新代码 ≤500；存量只降不升 |
| META-013 | P1 | `MetadataService` 17 方法 + 7 个静默默认（空返回/UOE） | `MetadataService.java:17-148` | 拆 5 个接口 + Gateway | 每接口 ≤7 方法；无静默默认 |
| META-014 | P1 | `IndexMetadataStore` 50+ 方法/8 UOE | `IndexMetadataStore.java` | 拆 6 个接口；删除 default 业务逻辑 | 每接口 ≤7 方法；0 UOE |
| META-015 | P1 | 4 个跨模块同名类 | §4.1.5 | 重命名/移动；删除重复实现 | 0 C2 冲突 |
| META-016 | P1 | `Arrow`/`Iceberg` 命名与依赖 | `ArtifactFormatContracts.java:40`；`pom.xml:42-47` | 改格式 ID；删 Iceberg 依赖 | 0 C1 违规；依赖干净 |
| META-017 | P1 | SPI 注解缺失 | 3/296 | 为所有 interface 加注解；`@Stable` 配生产实现 | SPI gate 通过 |
| META-018 | P1 | `TableMetadata` 转换丢失信息 | `DefaultMetadataService.java:540-547` | 无损 descriptor 或补齐字段 | nullable/tags/partition/sort 保留 |
| META-019 | P1 | ALTER 非原子 + 全局锁 | `DefaultMetadataService.java:80, 332-344` | per-table 锁；change-set 补偿 | 并发测试；失败语义明确 |
| META-020 | P1 | sync 吞异常/随机抽样/不兼容只 log | `MetadataSyncService.java:124, 144, 226, 269` | 错误码+cause；确定性 reconcile；fail closed | 漂移可复现；失败可重试 |
| META-021 | P1 | LENIENT 未实现 | `SchemaEvolutionValidator` 与 `StrictnessLevel` | 实现或删除枚举值 | 文档/代码一致 |
| META-022 | P1 | artifact 发布 SQL/digest 三份复制 | §4.3 C-24 | 统一泛型模板 + 单一 digest | 一份实现；字段一致 |
| META-023 | P1 | 两个 index 发布实现 | `JdbcIndexPublicationStore` vs `JdbcIndexArtifactPublicationStore` | 合并/委托 | 单一路径 |
| META-024 | P1 | 死代码 5 类 | `DdlPropagationWaiter`、`InvalidationMetrics`、`MetadataSyncMetrics`、`CachingFederatedCatalog`、`TableInvalidationListener`；`MetadataCleanupScheduler` 仅测试 | 删除或接线 | `detect-dead-stubs.sh` 0 发现 |
| META-025 | P1 | tag 权威歧义 | `MetadataTagOperations` vs governance `JdbcColumnTagStore` | 删除未接线实现；明确 control binding 唯一权威 | 0 未接线 tag store |
| META-026 | P1 | 错误模型不一致 | `JdbcSemanticViewStore:213-216`、`JdbcFeatureCatalogStore`、`JdbcEvaluationStore` | 统一 `MetadataException`/`PangrovaException` + ErrorCode | 0 裸 `IllegalStateException` |
| META-027 | P1 | 测试默认不跑 integration；AI 域薄 | §4.5 | CI 增 full-regression；补 property/E2E | integration 在 CI 必跑；关键状态机 property test |
| META-028 | P1 | 文档/依赖门禁漂移 | §4.6 | 更新 metadata.md/architecture.md；把 metadata 纳入依赖测试 | 文档与代码一致；门禁覆盖 metadata |
| META-029 | P2 | 命名/过程引用/Javadoc | §4.1.5 | 重命名、删任务号、补 Javadoc | 规范扫描通过 |
| META-030 | P2 | 硬编码阈值/魔法串 | `tenant_id`、`-1`、`auto_pii_`、`DEGRADED_THRESHOLD` 等 | 提取常量/配置 | 代码扫描无魔法串 |
| META-031 | P2 | 异常类死代码 | `TableNotFoundException`、`CatalogAccessDeniedException`、`CatalogAlreadyExistsException` 零引用 | 删除或使用 | 0 未引用异常 |
| META-032 | P2 | `MetadataStore` 四聚合混合 | `MetadataStore.java:17-45` | 拆 Catalog/TableProjection/SyncHistory | 每接口 ≤7 方法 |
| META-033 | P2 | `CatalogDescriptor` 9 组件 + 随机 UUID 副作用 | `CatalogDescriptor.java:11-56` | 工厂方法显式生成 id | 值对象无隐藏副作用 |
| META-034 | P2 | 构造函数 DDL 的 `IF NOT EXISTS` 静默保留旧结构 | §4.4.2 | 删除，统一 Flyway | 0 构造 DDL |
| META-035 | P2 | `GovernanceWiring` legacy shim | `governance/GovernanceWiring.java` | 迁移调用方后删除 | 0 legacy wrapper |

---

## 6. 目标边界与拆分（以治理计划为准）

> 本节旧版本的目标模块/拆分细节已被 [metadata-governance-and-split-plan-2026-10-31.md](./metadata-governance-and-split-plan-2026-10-31.md) 取代。以下只保留结论摘要，避免本文档与权威计划出现第二套命名。

### 6.1 目标 owner（摘要）

| 事实/能力 | 目标 owner |
|---|---|
| Catalog/namespace/ObjectId/name binding/schema/version/statistics/table control/managed source identity | `metadata` `.api/.core/.store.jdbc` |
| Query/task state、write commit、execution anchor、COPY admission、query lineage outbox | `query.transaction` |
| Coordinator/worker registry、distributed lease、ZK | `coordinator.coordination` |
| Index descriptor/generation/commit queue/statistics + artifact build/publication | `indexing.control/.store.jdbc` |
| CDC registration/commit/offset/watermark/state/checkpoint | `streaming.control/.store.jdbc` |
| Materialization definition/generation/attempt/schedule/pin/GC | `materialization.control/.store.jdbc` |
| Quality/lineage/audit/catalog tags | `governance` |
| Tenant/policy/file access/storage profile/secret | `security` |
| Query audit records | `observability.audit` |
| Semantic model/relationship/ontology/metric/compiler | `semantic` |
| Evaluation/grounding/model-prompt registry/feature view/training dataset/AI worker | `pangrova-ai` |

### 6.2 Clean-Cut 顺序（摘要）

| Phase | 内容 |
|---|---|
| Phase 0 | 删除死代码/重复实现/未使用依赖/构造 DDL/旧迁移；建 `modules.yaml` 与依赖 gate；表级 owner 清单 |
| Phase 1 | 建 `metadata.api/.core/.store.jdbc`；删除 `MetadataService`/旧 `service/store/model`；Coordinator 1,114 import 切到 API；拆 12 个超大类的 metadata 侧 |
| Phase 2 | 按 owner 迁出 tenant/governance/index+artifact/streaming/materialization/semantic/feature+evaluation/query+coordinator coordination/file+storage+source/query audit；每批删除旧包/旧表/旧测试 |
| Phase 3 | 删除旧 V1–V101，按 owner 重建 baseline + 独立 Flyway history；Testcontainers 从 baseline 启动 |
| Phase 4 | 删除旧 package/class/test/fixture/配置/兼容层；更新文档；跑 full-regression + no-residue checklist |

### 6.3 不再保留

- 不新建独立的 metadata-coordination/metadata-artifact/AI-metadata/storage 模块；目标 owner 见上表。
- 不保留旧 `MetadataService`/旧 JDBC store/旧 `service/store/model` 顶层包。
- 不保留 V1–V101 旧迁移、旧表、旧列、兼容视图、dual-write、`@Deprecated`/legacy adapter。
- 不保留顶层 AI control 模块；只保留 `pangrova-ai` 一个 owner artifact，`control/worker` 为内部 package。

## 7. 验收标准

### 7.1 结构

| 指标 | 目标 |
|---|---|
| 单类行数 | 新代码 ≤500；存量 >800 = 0；800 是 code-standards review 诊断，500 是内部拆分目标 |
| 单方法行数/分支/缩进 | ≤50 行 / ≤8 分支 / ≤4 缩进 |
| 接口方法数 | ≤7（超出需拆分并记录理由） |
| 构造依赖/方法参数 | ≤6 / ≤7 |
| `UnsupportedOperationException` | 0 处无 `@Experimental` + issue |
| 构造函数 DDL | 0 |
| 死代码/未引用类 | 0（`detect-dead-stubs.sh`） |
| C1/C2/C11 | 0 违规 |
| 过程引用（任务号/阶段） | 0 |

### 7.2 依赖与 schema

| 指标 | 目标 |
|---|---|
| metadata POM | 只依赖 `spi/common`（+ 可选 `security.api`、`pangrova-jdbc`）；不依赖 materialization/governance/security 实现 |
| 依赖测试 | metadata + 新模块全部在 `ALLOWED_DEPS` 中；根 POM 模块数 = 文档 |
| 跨域 SQL | 0 直接跨 schema 表访问；全部 SPI/read model/outbox |
| 迁移 owner | 每个 owner 一个 baseline + 独立 Flyway history；0 构造 DDL；无旧 V1–V101 |
| 表所有权 | baseline 中每张表有唯一 owner；无 orphan/旧表 |

### 7.3 正确性

| 场景 | 验收 |
|---|---|
| 未配置 JDBC 的启动 | 显式 metadata-less 才允许 InMemory；否则 fail-fast |
| 事件队列满 | 不丢关键失效事件；指标可见 |
| 缓存长跑 | 版本 map 有界；失效后无旧值 |
| catalog 更新 | revision 自增成功；并发更新有 CAS 测试 |
| COPY 写入 | claim 行存在/不存在都有明确行为；恢复幂等 |
| watermark/offset | 旧值不能覆盖新值；property test |
| materialization 物理删除 | 不在 DB 事务内；失败可补偿 |
| ALTER 多变更 | 失败语义明确；有补偿/回滚测试 |
| sync 漂移 | 确定性可复现；不兼容 fail closed |

### 7.4 测试

| 指标 | 目标 |
|---|---|
| 单元测试 | 每个新类至少 1 个测试；边界/错误路径覆盖 |
| 集成测试 | CI full-regression 必跑；Postgres/Testcontainers 路径不跳过 |
| property test | 5 个关键状态机各至少 1 个 |
| E2E | 8 条主链路全绿 |
| diff coverage | ≥60% |
| 文档 | architecture.md / metadata.md / 模块 README 同步 |
| 历史残留 | 旧 package/class/API/表/迁移/兼容层 = 0（no-residue checklist） |

---

## 附录 B：测试清单（按包）

> 数据来自 `find src/test/java -name "*Test.java"` 与行数统计；未逐文件列出全部 141 个测试名，避免与主代码清单重复。默认 `excludedGroups=integration`；`full-regression` profile 才启用全部。

| 包 | 测试文件 | 行数 | 备注 |
|---|---:|---:|---|
| `coordination` | 21 | 4,496 | Postgres/时钟/并发/恢复 |
| `artifact`（含 jdbc/build） | 16 | 3,183 | 契约 + 发布 + fence |
| `migration` | 15 | 1,145 | 迁移 baseline/历史测试；默认 integration |
| `service` | 10 | 2,059 | resolve/DDL/control/cache |
| `control` | 10 | 2,145 | ownership/maintenance/statistics |
| `catalog` | 4 | 605 | 生命周期/健康/handle |
| `index`（含 jdbc/model） | 8 | 1,198 | 发布/commit queue/统计 |
| `sync` | 7 | 1,712 | sync/invalidation/drift |
| `governance` | 6 | 828 | 质量规则/身份迁移 |
| `cache` | 6 | 1,242 | L1/L2/失效 |
| `store` | 4 | 919 | JDBC store/invalidation |
| `streaming` | 4 | 700 | 含 2 property test |
| `storage` | 3 | 1,279 | COPY 准入/profile |
| `file` | 3 | 643 | 文件空间/目录 |
| `dependency` | 3 | 461 | 依赖图 |
| `evaluation` | 2 | 690 | 仅 2 个 |
| `adapter` | 2 | 507 | 联邦 |
| `semantic` | 1 | 197 | 仅 1 个 |
| `feature` | 1 | 155 | 仅 1 个 |
| `materialization` | 1 | 656 | 3,187 行主代码仅 1 个 |
| `tenant` | 1 | 192 | 仅 1 个 |
| `schema` | 1 | 289 | 仅 1 个 |
| `serde` | 1 | 182 | 仅 1 个 |
| `event` | 1 | 38 | 仅 1 个 |
| `model`（含 asset） | 1 | 225 | 仅 1 个 |
| `property/streaming` | 2 | 390 | 流式状态 property test |
| 合计（`*Test.java`） | 134 | 26,136 | 另有 7 个 fixture/helper，合计 141 个 test 源码文件、26,666 行 |

## 附录 C：迁移清单（按域）

| 域 | 迁移版本（当前） | 关键表/变更 |
|---|---|---|
| 基础/多域 | V1 | 29 张表：catalog、registry、index、CDC、quality、tenant、watermark、journal、table projection 等 |
| metadata core | V2、V4、V9、V34、V35、V42、V86–V92、V97–V101 | governance audit、tenant/checkpoint 列、object dependencies、catalog incarnation/revision、invalidation log、table control、name reservation、column mask/row policy、quality identity |
| coordination | V3、V8、V23、V24、V33、V44、V49、V50、V52、V57、V58、V61、V78、V82、V91 | coordinator registry、journal epoch、execution anchor、fenced write journal、deferred intent、abort barrier、event identity、lineage outbox、COPY claims/publications、worker TLS/identity、fencing tokens、quarantine |
| artifact | V14、V19–V22、V25、V27、V40、V41、V43、V46、V47 | build snapshot/jobs、dispatch payload、publication fence、generated value metadata、dependencies、native write evidence、auth scope、stage graph、execution fence、exchange barrier |
| index | V17、V18、V28、V31、V36–V38、V54、V55、V64 | artifact identity、generation builds、tenant commit log、UTC leases、fingerprint idempotency、fair polling、table heads、scalar build authority、dropped cleanup identity、scoped file statistics |
| streaming | V5、V6、V15、V39、V73、V75 | CDC registrations、commit state、batch key、retry after rollback、complete source checkpoints、checkpoint identity |
| governance | V2、V53、V67、V74、V101 | audit trail、single quality rule authority、execution result identity、execution identity、stable quality rule identity |
| AI | V10、V26、V29、V32、V45、V48、V62、V63、V67、V74 | embedding registry, semantic/evaluation, feature/training catalog, model execution fence, model role authority/drop, numeric release policy, feature evaluation subject, quality identity |
| materialization | V60、V66、V68–V72、V83–V85 | dynamic table publication/attempts, unified kernel, session properties, dependency guard, rename, command receipt, reclaim, definer groups |
| storage/file/source | V51、V56、V59、V65、V93–V96 | managed source authority, storage profile revisions, COPY read admissions, frozen input lineage, file space registry/grant cleanup/directory index/managed root |
| tenant | V1、V4 | tenants/memberships/checkpoint columns |
| 缺失 | V11–V13、V16、V76–V77、V79–V81 | 仅作为 churn 证据；新项目不保留旧迁移，无兼容要求 |

> 目标 owner 见治理计划 §3.2：index/artifact → indexing；streaming → streaming；materialization → materialization；semantic → semantic；evaluation/feature → pangrova-ai；file/storage → security；COPY → query.transaction；query audit → observability；tenant → security。

## 附录 D：证据与复现命令

```bash
# 规模
find pangrova-metadata/src/main/java -name '*.java' | wc -l
find pangrova-metadata/src/main/java -name '*.java' -exec wc -l {} + | sort -rn | head -20
find pangrova-metadata/src/test/java -name '*Test.java' | wc -l
grep -rh '@Test' pangrova-metadata/src/test/java | wc -l

# 依赖/命名
grep -rn 'org.apache.iceberg' pangrova-metadata/pom.xml
grep -rniE '\b(arrow|iceberg|velox|calcite|flight)\b' pangrova-metadata/src/main/java
grep -rn 'UnsupportedOperationException' pangrova-metadata/src/main/java | wc -l
grep -rl '@Stable\|@Experimental\|@Internal' pangrova-metadata/src/main/java | wc -l
grep -rn 'CREATE TABLE IF NOT EXISTS' pangrova-metadata/src/main/java

# 跨模块
grep -rh '^import org.pangrova.metadata.' pangrova-coordinator/src | wc -l
grep -rn 'ModuleDependencyPropertyTest' -A60 pangrova-testing/pangrova-test-common/src/test/java/org/pangrova/testing/property/ModuleDependencyPropertyTest.java

# 迁移
ls pangrova-metadata/src/main/resources/db/migration | sort -V
```

**关键文件:line 索引**：

| 证据 | 位置 |
|---|---|
| 默认 InMemory 控制面 | `catalog/CatalogRegistry.java:63-64, 87-92` |
| 事件丢弃 | `event/MetadataEventDispatcher.java:41-44` |
| 缓存版本无界 | `cache/TieredMetadataCache.java:84, 175` |
| 双缓存 | `service/DefaultMetadataService.java:80-130, 228-261` |
| 全局 ALTER 锁 | `service/DefaultMetadataService.java:80, 332-344` |
| 转换丢失字段 | `service/DefaultMetadataService.java:540-547` |
| catalog revision 栅栏 | `store/JdbcMetadataStore.java:102-133` |
| sync 空实现 | `store/JdbcMetadataStore.java:181-183` |
| watermark 覆盖 | `streaming/JdbcWatermarkStore.java:31-34` |
| offset 覆盖 | `streaming/JdbcStreamOffsetStore.java:54-57` |
| COPY claim 依赖 | `coordination/JdbcWriteCommitJournal.java:248` |
| dead code claim store | `coordination/JdbcCopyFileClaimStore.java:19` |
| 事务内物理删除 | `materialization/JdbcMaterializationRepository.java:565-604`（583） |
| 两个 index 发布实现 | `index/jdbc/JdbcIndexPublicationStore.java:32`；`artifact/jdbc/JdbcIndexArtifactPublicationStore.java:33` |
| Arrow 格式 ID | `artifact/contract/ArtifactFormatContracts.java:40` |
| UOE 默认 | `index/IndexMetadataStore.java:39,44,53,63,355,443,449,455` |
| ZK 路径编码 | `coordination/ZooKeeperCoordinatorRegistry.java:260-262`；`ZooKeeperDistributedLease.java:246-254` |
| 无 rollback | `control/JdbcTableControlGovernanceBindingStore.java:64-85, 135-149, 152-164` |
| 统计版本 RMW | `control/TableControlStatisticsService.java:112-126` |
| Feature JSON NPE | `feature/JdbcFeatureCatalogStore.java:224, 242` |
| Semantic 错误模型 | `semantic/JdbcSemanticViewStore.java:213-216` |
| Evaluation gate 在仓储 | `evaluation/JdbcEvaluationStore.java:457-505` |
| 依赖测试未覆盖 metadata | `ModuleDependencyPropertyTest` 的 `ALLOWED_DEPS` |
| 文档旧结构 | `docs/architecture/metadata.md:2342-2405` |

---

## 结语

`pangrova-metadata` 的问题不是“文件太多”，而是**事实所有权没有随代码增长而分层**：catalog 核心、控制面、协调恢复、索引、artifact、物化、流式、AI 元数据共用一条迁移流、一个 artifact、一个 JDBC 连接池。建议按本文的 Phase 0→3 推进：先用门禁和 fail-closed 把正确性止血，再用包边界把依赖降下来，最后只抽出真正独立的失败域；不要为了“模块数量”做一次性大爆炸拆分。

如果只能做三件事：**修默认 InMemory 控制面、修事件丢弃/缓存无界、把构造 DDL 与迁移缺口收敛到 Flyway 单一 owner**。这三项直接决定 metadata 能否作为生产控制面可信运行。
