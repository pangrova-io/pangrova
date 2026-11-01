# pangrova-metadata 治理与 Clean-Cut 拆分计划

- 状态：Approved plan（2026-10-31）
- 范围：`pangrova-metadata` 的治理机制、事实边界、影响范围、Clean-Cut 拆分顺序与验收
- 前提：新项目、无外部部署/外部 API/生产数据；不做向后兼容
- 配套：[metadata-module-review-2026-10-31.md](./metadata-module-review-2026-10-31.md)（历史静态 review；本文档修正其目标命名与迁移假设）

## 0. 结论、假设与交付物

### 0.1 结论

`pangrova-metadata` 必须从“共享 PostgreSQL schema + 多域持久化聚合模块”重构为**单一技术事实 owner artifact**：

```text
org.pangrova.metadata.api.*        # 唯一 export
org.pangrova.metadata.core.*       # 领域/应用/策略/port，不 export
org.pangrova.metadata.store.jdbc.* # JDBC repository/mapping/transaction，不 export
```

其他 durable state 按 record owner 迁出：

```text
query.transaction        query/task state、write commit、execution anchor、COPY admission、query lineage outbox
coordinator.coordination coordinator/worker registry、distributed lease、ZooKeeper membership
indexing.control         index descriptor/generation/commit queue/statistics + artifact build/publication
streaming.control        CDC registration/commit state/offset/watermark/stream state/CDC checkpoint
materialization.control  definition/generation/attempt/schedule/pin/GC
governance               quality rules/results、lineage projection、audit、catalog tags
security                 tenant identity/membership、auth decision、row/column policy execution、file access/storage profile、secret
semantic                 semantic model/relationship/ontology/metric/compiler/publication
pangrova-ai              evaluation/grounding/model-prompt registry/feature view/training dataset/AI worker
observability            query audit records
```

### 0.2 新项目假设（决定整个计划形态）

- 项目未发布，**没有任何外部部署、外部 API 消费者或必须保留的生产数据**。
- 因此本计划**不做向后兼容**：
  - 不保留旧 API/facade/`@Deprecated`/兼容 adapter；
  - 不做 dual-read/dual-write/authority flip/兼容视图；
  - 不保留 V1–V101 迁移历史，不写升级脚本；
  - 不保留旧表、旧列、旧 package、旧类、旧测试；
  - 本地/测试数据库直接重建；如有任何真实数据环境，另开任务，不属于本计划。
- 仍然要求：仓库内所有调用方、测试、E2E、文档在同一个重构窗口内更新；每个阶段结束时编译和测试必须是绿的。

### 0.3 权威依据

1. `.pangrova/rules/code-standards.md`：
   - metadata 只拥有 technical object identity、schema、version、binding、statistics references；
   - semantic/business model 归 `pangrova-semantic`，AI evaluation/grounding 归 AI owner；
   - 禁止顶层横向 `service/model/store/util/impl` 包；
   - 跨模块 import 默认拒绝，由 `.pangrova/modules.yaml` 控制；
   - 每个 owned root/exported package 有 `package-info.java`。
2. `docs/architecture/long-term-modular-architecture.md`：24 root / 36 leaf、`.api → .core → .store.jdbc`、M0–M22。
3. `docs/architecture/metadata-center-redesign.md`：Metadata Center V2 的 object identity/name binding/TableBinding/catalog store/outbox。
4. `docs/internal/metadata-module-review-2026-10-31.md`：296 文件/38,959 行、35 项问题与证据。

### 0.4 采纳的用户决策

- **AI 模块统一**：只保留一个 `pangrova-ai` owner artifact；`control`/`worker` 先是内部 package。只有出现独立 provider SDK、独立部署/进程边界或依赖隔离需求时，才升级为 `pangrova-ai` family aggregator + `pangrova-ai-control`/`pangrova-ai-worker` 两个子 module。不再同时存在顶层 `pangrova-ai-control` 与 `pangrova-ai` 两个 code-bearing artifact。
- **不做历史兼容**：旧 API、旧表、旧迁移、旧 package、旧类全部删除；不保留兼容层。

### 0.5 文档关系

- 本文档是 `pangrova-metadata` 治理与拆分的权威计划。
- `docs/internal/metadata-module-review-2026-10-31.md` 是历史静态 review，保留问题证据；其目标模块命名、materialization 矩阵判断、≤500 门禁表述和迁移兼容假设由本文档修正。
- 实施按 §5 的 Phase 0–4 推进；每个 Phase 独立可合并、可回滚，且同一批次删除旧代码/旧表/旧测试。

---

## 1. 实际复核（Deep review，不是纸面规划）

### 1.1 规模与结构

| 指标 | 当前值 |
|---|---:|
| 主代码 | 296 文件 / 38,959 行 |
| 测试 | 141 个 test 源码文件（134 个 `*Test.java` + 7 个 fixture）/ 26,666 行 / 852 个 `@Test` |
| Flyway | 92 个迁移，版本 V1–V101，缺 9 个版本 |
| 表 | 114 张（解析 `CREATE TABLE`，排除注释误报） |
| >500 行类 | 12 个，其中 2 个 >800 |
| UOE | 16 处 |
| 带 SPI 注解文件 | 3 / 296 |
| 构造函数 DDL | 8 处 / 4 类 |
| 跨模块同名类 | 4 个 |
| Coordinator 对 metadata import | 1,114 处 / 146 个文件 / 133 个唯一类 |

### 1.2 内部依赖不是分层，而是一个大环

按 top-level package 解析 `import`，存在一个 **8 包强连通分量**：

```text
adapter ↔ cache ↔ catalog ↔ control ↔ event ↔ service ↔ store ↔ sync
```

具体环包括：

- `catalog → control → store → catalog`：CatalogRegistry 依赖 TableControl，JdbcMetadataStore 又依赖 CatalogDescriptor。
- `sync → service → control/store → sync`：sync 调 `DefaultConnectorSession`，service 经 control/store 又回到 sync history，形成大环的一部分。
- `event → catalog`：CatalogStateChangedEvent 依赖 CatalogDescriptor，事件包反向依赖 catalog。
- `catalog → adapter → sync → catalog`：CatalogHandle 引用 FederatedCatalog，adapter 引用 sync，sync 又回指 catalog，形成大环。
- `artifact → feature`、`ai → evaluation/semantic/feature`：跨域直接 import，不是端口/事件。

结论：**不能靠“把文件移到新包”完成拆分**。必须先定义 API/port，再重写实现；否则只是把环复制到新模块。

### 1.3 消费者影响面

| 消费者 | 文件数 | import 数 | 唯一类 | 说明 |
|---|---:|---:|---:|---|
| `pangrova-coordinator` | 146 | 1,114 | 133 | 最大消费者；`CatalogRegistry` 58、`IndexMetadataStore` 23、`CatalogHandle` 21、`ArtifactBuildStore` 14 |
| `pangrova-testing` | 36 | 119 | 65 | E2E/测试 fixture 直接使用 JDBC store |
| `pangrova-connector-lance` | 2（test） | 11 | 9 | Connector 测试依赖 metadata JDBC；生产 main 不依赖 |
| `pangrova-indexing` / `streaming` / `governance` / `materialization` / `security` / `standalone` | 0 | 0 | 0 | 这些域当前**不 import metadata**，接收迁出代码不会产生反向依赖 |

这个分布决定顺序：

1. 先给 Coordinator 稳定 `metadata.api`，否则 146 个文件无法切换。
2. 先迁“零依赖”的域（tenant、governance quality、index、streaming、materialization），风险最低。
3. testing/connector 测试 fixture 必须在同一阶段改到新 owner，不能留在 metadata。

### 1.4 双权威与重复实现（clean-cut 直接删除）

| 事实 | 当前重复 | 决策 |
|---|---|---|
| Index generation 发布 | `JdbcIndexPublicationStore` + `JdbcIndexArtifactPublicationStore` | 只保留 indexing.control 的一套 |
| Streaming CDC/watermark | metadata `streaming` + `pangrova-streaming` 同名 JDBC | 只保留 streaming.control 的一套 |
| Column tag | metadata control binding + governance `JdbcColumnTagStore`（未接线） | metadata 只留 binding identity；删除未接线 store |
| Materialization 持久化 | materialization port/domain + metadata JDBC | 只保留 materialization.store.jdbc |
| Tenant | security TenantStore + metadata `JdbcTenantStore` | 只保留 security.store.jdbc |
| Artifact 发布 | generated/index/tabular 三套复制 SQL + 三份 digest | 只保留 indexing.control 的泛型模板 + 单一 digest |

### 1.5 与当前规范的硬冲突

| 当前代码 | 规范 | 冲突类型 |
|---|---|---|
| `semantic/`、`evaluation/`、`feature/` | 归 `semantic` / `pangrova-ai`；metadata 不得 import 实现 | 硬冲突 |
| 顶层 `service/`、`store/`、`model/` | 禁止横向 catch-all 包 | 硬冲突 |
| `index/`、`artifact/`、`streaming/`、`materialization/`、`tenant/`、`governance/` | metadata 只拥有 technical facts | 所有权原则冲突 |
| `.pangrova/modules.yaml` 不存在 | 跨模块 import 默认拒绝 | 治理 gate 缺失 |
| metadata 未纳入 `ModuleDependencyPropertyTest` | 依赖矩阵必须执行 | gate 缺失 |
| Iceberg API/Core 在 POM、主代码零 import | Iceberg 只在 connector | 未使用依赖 + C1 |
| `"pangrova.tabular-export.arrow-stream"` | Arrow 禁止出现在 metadata | C1 |

`ModuleDependencyPropertyTest` 的注释仍写 “18 modules”，而根 POM 已有 25 个 module entry；这说明依赖 gate 不只是漏了 metadata，而是整体已与 reactor 漂移。

### 1.6 跨域表消费者（静态扫描）

对 114 张表名在 metadata 之外的 `src/main` Java 中做字符串扫描（可能包含同名模型/变量，最终以表级 owner 盘点为准），发现这些表并不是 metadata 私有：

| 表 | 外部消费者（模块） | 目标 owner | 边界动作 |
|---|---|---|---|
| `tenants` / `tenant_user_memberships` | coordinator、materialization、operations、resource、security | `security` | 其他模块只接收 TenantId/TenantContext 值对象；不读表、不依赖 security 实现 |
| `checkpoints` | coordinator、resilience、spi、streaming | `query.transaction`（generic）/ `streaming`（CDC） | 按 checkpoint 对象拆；`spi.resilience` 只留 port，消费者不读实现表 |
| `invalidation_log` | coordinator、spi | `metadata` | 外部不读表，消费 metadata event/API |
| `execution_anchor` | coordinator、shuffle | `query.transaction` | shuffle 只接收 execution identity/fence token，不读表；coordinator 走 query API |
| `write_commit_journal` / `write_commit_quarantine` | coordinator | `query.transaction` | coordinator 走 query API |
| `materialization.native_commit_capture` | coordinator | `materialization` | 走 materialization API |
| `pangrova_embedding_models` | coordinator、security | `pangrova-ai` | 通过 AI API/model descriptor；security 不直读表 |
| `cdc_registrations` / `cdc_commit_state` | streaming | `streaming` | 表随 streaming 迁出 |
| `file_space_registry` | security、worker | `security`（file access authority） | worker 走 security API，不依赖 metadata |
| `file_directory_index` | worker | `security`（directory observation） | worker 走 security API |
| `storage_profile_definitions` | security | `security` | 表迁 security |
| `query_audit_records` | observability | `observability.audit` | 表迁 observability，不删除 |
| `governance_audit_trail` | governance | `governance.audit` | 表迁 governance |
| `table_control_governance_binding` | security | `metadata` | security 通过注入的 binding/port，不直读 metadata 表 |
| `table_control_maintenance_run` | coordinator | `metadata` | coordinator 走 metadata API |
| `index_eval_breaker_state` | coordinator | `indexing` | 走 indexing API |
| `pangrova_authorization_revision` | security；metadata write journal 也读取 | `security` | 表迁 security；write journal 通过 security API，不直读表 |
| `orphaned_queries` | 无 | 删除 | 零引用表 |

这个扫描改变了两个边界决策：

- **`file/`、`storage/` 不能留在 metadata**：`worker` 需要 file space/directory index，而 worker 不应依赖 metadata；因此 file access authority、directory observation、storage profile 归 `security`，worker 通过 security API 使用；COPY admission 归 `query.transaction`。
- **`query_audit_records` 不是可删除表**：它由 `pangrova-observability` 的 `JdbcAuditStore` 使用，只是错误地放在 metadata 迁移中；应迁到 `observability.audit`。

### 1.7 数据库债务

- 92 个迁移、114 张表中，`V1__init_schema.sql` 一次创建 29 张跨域表。
- 已确认零引用的遗留表例子：`orphaned_queries`；`column_tags` 已在 V97 中 drop。`query_audit_records` 并非零引用，它由 `pangrova-observability` 使用，只是错误地放在 metadata 迁移中。
- 迁移版本不连续（缺 V11–13、V16、V76–77、V79–81），但新项目不保留历史，直接重建，不存在 upgrade 兼容问题。
- 8 处构造函数 `CREATE TABLE IF NOT EXISTS` 与 Flyway 双源；clean-cut 直接删除。
- 目标不是“修复 92 个迁移”，而是**删除它们，按 owner 重建 10–14 个 baseline**。

### 1.8 测试基线

- 141 个 test 源码文件、852 个 `@Test`；15 个 `@Tag("integration")`，86 个使用 Testcontainers/Postgres。
- metadata surefire 默认 `excludedGroups=integration`，普通 `mvn test` 不跑 JDBC/Postgres 路径。
- AI 域偏薄：materialization 3,187 行 / 1 个测试文件；semantic 631 / 1；feature 616 / 1；evaluation 1,434 / 2。
- Clean-cut 后测试必须随代码迁移，不能保留旧包测试；integration 必须在 full-regression 执行。

### 1.9 复核结论

- 当前 metadata 不是“稍微大一点的模块”，而是一个**环状依赖 + 跨域表 + 双权威 + 消费者直接依赖内部包**的共享数据库模块。
- 因为新项目、无兼容负担，最合适的策略不是“strangler + dual write”，而是**clean cut**：定义目标边界 → 重写 metadata 核心 → 把域代码/表一次性迁走 → 删除旧代码/旧迁移/旧测试 → 用 gate 防回流。
- 但 clean cut 不等于“一次性大爆炸提交”；按 owner 分批、每批编译/测试绿、每批删除旧代码，是可控的。

---

## 2. 治理原因（Why now）

1. **规范已经写死，代码没有遵守**：metadata 不得拥有 semantic/evaluation/feature；当前直接拥有。继续加功能会让规范失效。
2. **数据库失败半径 = 全系统**：92 迁移/114 表共用一条 history；任一迁移失败可能阻塞 query recovery、index build、CDC 和 AI 发布。
3. **内部依赖成环**：8 包 SCC 使“按目录搬迁”不可行；必须先建 API/port 再重写，否则环会复制。
4. **消费者绕过 API**：146 个 Coordinator 文件、1,114 处 import 直接依赖 133 个内部类；没有稳定 API，拆分成本会随时间指数上升。
5. **双权威造成不可回答的问题**：index/streaming/tag/materialization/tenant 都有两套实现；“谁是 authority、谁负责恢复、谁负责迁移”无法回答。
6. **质量基线已经需要 ratchet**：12 个 >500 行类、16 UOE、3/296 SPI 注解、8 构造 DDL、4 同名类、5 死类；不治理会在拆分中复制。
7. **新项目是窗口期**：没有外部部署/数据/API 兼容负担，现在 clean cut 的成本最低；错过这个窗口，后续每次拆分都要背兼容包袱。

治理的核心不是“减少文件数”，而是：

- 单一事实所有权；
- 失败半径隔离；
- 消费者只依赖稳定 API；
- 迁移/表/包/测试都有唯一 owner；
- 用 manifest + ArchUnit + migration owner + baseline 防止回流。

---

## 3. 边界（Definitive，不再“待定”）

### 3.1 Metadata 的硬边界

`pangrova-metadata` 只拥有：

- Catalog 定义、incarnation、revision、state；
- namespace/name binding、OBSERVED/LINKED/MANAGED table control、stable ObjectId/FieldId；
- table/view/volume/stage/function/model 的**技术身份**与 catalog-visible descriptor；
- schema/version/statistics 的 projection/reference（connector 是 schema/snapshot 权威）；
- 技术对象依赖边（不含 refresh/lineage projection）；
- metadata cache、invalidation、transactional outbox、metadata event payload；
- managed source identity/publication pointer（仅技术元数据）；file/volume/stage namespace、storage profile、file access authority 归 security。

Metadata 不拥有/不做：

- 不打开 `Page`/`PageSource`，不读数据文件，不做 DML 数据面；
- 不拥有 query/task state、write commit、execution anchor、lease、registry；
- 不拥有 policy execution（mask/row filter/quality/lineage）；只存 binding identity；
- 不解析 SQL、不做 plan rewrite、不持有 RelNode；
- 不调用对象存储 SDK 做数据清理；通过 port 交给 connector/query adapter；
- 不复制 format-native current truth；
- 不拥有 semantic model/ontology/metric、evaluation/grounding、tenant membership/quota。

### 3.2 目标 owner 矩阵（最终决定）

| 域/事实 | 最终 owner | 具体归属 | 明确不做 |
|---|---|---|---|
| Catalog/namespace/ObjectId/name binding | `metadata` | `.api.catalog/.api.object/.core.catalog/.core.resolution/.store.jdbc.catalog` | 不复制 format-native truth |
| Table schema/version/statistics projection | `metadata` | `.api.schema/.core.statistics/.store.jdbc.projection` | 不做 snapshot/manifest 权威 |
| Table control（ownership/admission/rename） | `metadata` | `.api.control/.core.control/.store.jdbc.control` | 不执行 row/column policy |
| 技术对象依赖 | `metadata` | `.core.dependency/.store.jdbc.dependency` | 不含 refresh/lineage projection |
| Metadata cache/outbox/event | `metadata` | `.core.cache/.core.event/.store.jdbc.outbox/.api.event` | 不丢事件；不做通用 event bus |
| File/Volume/Stage namespace、file access authority、directory observation、storage profile definition | `security` | `.core.storage/.store.jdbc.storage` | worker 通过 security API；metadata 不承担文件访问授权 |
| Managed source identity/publication pointer | `metadata` | `.core.source/.store.jdbc.source` | SDK invocation/cleanup 通过 port 外部实现；write/anchor 通过 query API |
| Query/task state、query lineage outbox | `query` | `.core/.transaction/.store.jdbc` | metadata 不拥有 query state |
| Write commit journal、execution anchor | `query.transaction` | `.transaction/.store.jdbc` | 不在 metadata 事务中提交 |
| COPY read admission/claims/frozen lineage | `query.transaction` | `.transaction/.store.jdbc` | metadata 只提供 profile/identity 引用 |
| Coordinator/worker registry、distributed lease、ZK | `coordinator.coordination` | `.coordination/.zookeeper` | 不整体搬成 metadata 子模块 |
| Generic task checkpoint | `query` | `.core.checkpoint` 或 worker runtime | 不保留通用 metadata checkpoint |
| CDC checkpoint | `streaming` | `.control/.store.jdbc` | 不与 query checkpoint 混表 |
| Index descriptor/generation/file mapping/commit queue/statistics | `indexing` | `.control/.store.jdbc` | 关闭 index 不影响基础 SQL |
| Artifact build/publication/fence | `indexing` | `.control/.store.jdbc` | 不独立成 artifact |
| Materialization definition/generation/attempt/schedule/pin/GC | `materialization` | `.control/.store.jdbc` | 不接管基础 Metadata authority |
| CDC registration/commit state/offset/watermark/stream state | `streaming` | `.control/.store.jdbc` | 不保留 metadata 版本 |
| Quality rules/results、lineage projection、audit、catalog tags | `governance` | `.quality/.lineage/.audit/.catalog` | 不在 query-critical 同步链 |
| Query audit records | `observability` | `.audit` | 表从 metadata 迁出；observability 已拥有 JdbcAuditStore |
| Row/column policy execution、tenant identity/membership、secret | `security` | `.core/.store.jdbc` | metadata 只存 binding identity |
| Semantic View/model/relationship/ontology/metric/compiler | `semantic` | `.api/.core/.store.jdbc` | metadata 只留 ObjectId/name binding |
| Evaluation/grounding/model-prompt registry/feature view/training dataset/AI worker | `pangrova-ai` | `.api/.control/.worker/.store.jdbc` | 不拆成两个顶层 artifact |
| Clustering/compaction/snapshot expiration/orphan cleanup | `table`（未来） | `.maintenance` | 当前 metadata 无此代码 |
| Tenant quota/resource admission | `resource`/`scheduler` | `.resource` | 不留在 metadata |

### 3.3 包结构目标

```text
org.pangrova.metadata.api
├── catalog/ object/ schema/ binding/ control/ statistics/ event/ error/
org.pangrova.metadata.core
├── catalog/ resolution/ binding/ mutation/ statistics/ control/ dependency/
├── cache/ event/ source/ internal/
org.pangrova.metadata.store.jdbc
├── catalog/ object/ projection/ control/ dependency/ outbox/ source/
├── transaction/ migration/
```

禁止：顶层 `service/`、`store/`、`model/`、`util/`、`impl/`；`store.jdbc` 只作为 adapter root；`model` 只能作为 capability 下的局部 subgroup。

### 3.4 依赖 DAG 与禁止边

```text
coordinator → metadata.api, query.api, indexing.api, streaming.api, materialization.api,
              governance.api, security.api, pangrova-ai.api, semantic.api
query       → metadata.api, security.api, semantic.api
indexing    → metadata.api
streaming   → metadata.api
materialization → metadata.api
governance  → metadata.api, semantic.api
pangrova-ai → metadata.api, semantic.api, indexing.api, query.api, security.api
semantic    → metadata.api
```

禁止：

- metadata → semantic / pangrova-ai / indexing / streaming / materialization / governance / security 的实现包；
- metadata.core / store.jdbc 被任何其它 leaf import；
- metadata → coordinator / worker；
- 任何 leaf 跨域直读别人的表。
- security 不依赖 metadata；file space/catalog identity 由 assembly 注入 immutable ID/namespace 值对象。
- materialization/streaming/indexing 不依赖 security/metadata 实现；只接收 TenantId/TableId/VersionToken 等 immutable 值对象。
- connector 生产代码依赖 metadata（当前仅 connector-lance test 依赖，测试 fixture 要移走）。

### 3.5 数据库与迁移（Clean baseline）

新项目不保留历史，因此：

- 删除 `src/main/resources/db/migration/V1..V101` 全部旧迁移。
- 每个 owner 建一个 baseline + 独立 Flyway history：

```text
db/metadata/migration/V1__baseline.sql
db/query/transaction/migration/V1__baseline.sql
db/coordinator/coordination/migration/V1__baseline.sql
db/indexing/migration/V1__baseline.sql
db/streaming/migration/V1__baseline.sql
db/materialization/migration/V1__baseline.sql
db/governance/quality/migration/V1__baseline.sql
db/governance/lineage/migration/V1__baseline.sql
db/governance/audit/migration/V1__baseline.sql
db/security/tenant/migration/V1__baseline.sql
db/security/storage/migration/V1__baseline.sql
db/observability/audit/migration/V1__baseline.sql
db/semantic/migration/V1__baseline.sql
db/ai/evaluation/migration/V1__baseline.sql
```

- 每个 baseline 只包含该 owner 当前仍使用的表；`orphaned_queries` 等零引用表直接删除；`query_audit_records` 迁 observability，`governance_audit_trail` 迁 governance。
- 不再有 `public` 大杂烩 schema；使用 per-owner schema 或至少 per-owner migration history。
- 不再有构造函数 DDL、兼容视图、dual-write 表、旧列、旧索引、旧 migration owner。
- 本地/测试 DB 直接重建；Testcontainers 每次从 baseline 启动。
- 若未来需要保留数据，另开数据迁移任务，不在本计划。

#### 3.5.1 表级 owner 盘点（P3-1 执行依据，2026-11-01 生成）

基于旧迁移链 `V1–V101` 全部 `CREATE TABLE` 扫描 + 全仓库引用扫描（除 `db/migration`、`/target/`），
对 ~114 张表逐表登记 owner。P3-1 落地前再跑一次引用扫描复核；无生产引用的表直接删除。

| Owner baseline | 表 |
|---|---|
| `metadata` | catalog_definitions、catalog_descriptor_revisions、schema_metadata、table_metadata、table_control_registry、table_control_name_reservation、table_control_statistics、table_control_version_observation、table_control_maintenance_policy、table_control_maintenance_run、table_control_governance_binding、file_column_statistics、column_tags、pangrova_object_dependencies、invalidation_log、sync_history、managed_source_schemas、managed_source_publications、managed_source_prepare_invocations、managed_source_objects、managed_source_object_pages、managed_source_heads、managed_source_cleanup_batches |
| `query` | query_state_journal、query_lineage_outbox、task_state_journal、execution_anchor、write_commit_journal、write_commit_quarantine（注：coordinator 主代码仍有直读，P3-6 需改为走 query API）、copy_source_read_admissions、copy_file_publications、copy_file_claims、checkpoints（`JdbcCheckpointStore` 尚未物理迁入，随并行重构收敛后补） |
| `coordinator.coordination` | coordinator_registry、worker_registry、distributed_lease |
| `streaming` | ingestion_stream_state、ingestion_stream_offsets、cdc_commit_state、watermark_positions |
| `indexing` | index_descriptors、index_generation_manifests、index_generation_builds、index_file_mappings、index_commit_table_heads、index_commit_log、index_cache_events、index_build_claims、index_maintenance_lease、index_eval_breaker_state、artifact_build_jobs、artifact_build_pages、artifact_build_page_sources、artifact_build_page_dependencies、artifact_build_outputs、artifact_build_dependencies、artifact_build_dispatch_specs、artifact_stage_exchanges、artifact_execution_routes、artifact_exchange_registrations、artifact_exchange_finalizations、generated_value_generation_manifests、generated_value_artifacts |
| `materialization` | materialization（含全部 ALTER 演进后的终态） |
| `governance` | quality_rules、quality_rule_identity、quality_check_results、governance_audit_trail |
| `security` | security_grants、pangrova_roles、pangrova_user_roles、pangrova_group_roles、row_policy、column_mask、tenant_quota、tenant、tenant_membership、tenant_property（以上已由 security 自持 `security-policy-schema.sql`）、tenants、tenant_user_memberships、file_space_registry、file_directory_index、file_directory_refresh、file_directory_pending、storage_profile_definitions、storage_profile_revisions、pangrova_authorization_revision |
| `pangrova-ai` | evaluation_runs、evaluation_metrics、evaluation_metric_policies、evaluation_metric_policy_rules、evaluation_release_decisions、evaluation_dataset_versions、feature_view_versions、feature_view_active_versions、training_dataset_versions、pangrova_embedding_models、catalog_export_requests、tabular_export_generations |
| `semantic` | semantic_view_versions、semantic_view_active_versions |
| `observability` | query_audit_records |
| 删除（零生产引用） | orphaned_queries（仅 test-e2e 引用）、dynamic_table_state、dynamic_table_publications、dynamic_table_refresh_attempts、dynamic_table_refresh_history、dynamic_table_refresh_native_commits（仅 metadata 迁移历史测试引用，materialization 主代码已无动态表代码） |

P3-5 测试重定位清单（随 baseline 迁移，2026-11-01 已执行）：

- metadata 测试树的 3 个 JDBC 域测试已物理迁出并验证：
  - `JdbcSemanticViewStoreTest` → `pangrova-semantic` `org.pangrova.semantic.store.jdbc`，直接构造 `JdbcSemanticViewStore`（不再经 `AiInfrastructureStores`，避免 semantic→ai 反向依赖），用 semantic 自持 baseline（`db/semantic/migration` + `flyway_schema_history_semantic`，模块内 `testinfra/SharedPostgresContainer`）。
  - `JdbcEvaluationStoreTest`、`JdbcFeatureCatalogStoreTest` → `pangrova-ai` `org.pangrova.ai.store.jdbc`，用 ai 自持 baseline（`db/ai/evaluation/migration` + `flyway_schema_history_ai`，模块内 `testinfra/SharedPostgresContainer`）；`evaluation_observations` 仍是测试本地 TEMP 表，不进 baseline。
  - 迁移暴露并修复一处真实依赖缺口：`AiJsonMapper` 使用 `jackson-datatype-jsr310`（此前被 metadata pom 的依赖掩盖），已补为 `pangrova-ai` main 依赖。
  - metadata pom 的 test-scope `pangrova-semantic` 依赖已删除（semantic 经 `pangrova-ai` main 依赖传递，baseline 资源仍在 metadata 测试 classpath 上，`migrateAll` 行为不变）。
- metadata `src/test/.../migration/` 下 15 个迁移历史测试（ArtifactExchangeBarrier/…/WriteCommitIdentity 等）已随旧链删除。

### 3.6 API / Event / SPI / Error

- 只有 `org.pangrova.metadata.api..` export；`core/store.jdbc` 默认不 export。
- `MetadataService` 删除，拆为 ≤7 方法 use-case ports + `MetadataGateway` record（只聚合，不增加方法）。
- Event payload 归 producer；metadata 事件放 `.api.event`；不建通用 event artifact。
- 不再新增 `spi.metadata` 通用命名空间；connector/query/index 用各自 owner API 或独立 SPI。
- 所有 exported 类型必须有 `@Stable`/`@Experimental`/`@Internal`；无 `UnsupportedOperationException` 除非 `@Experimental` + issue。
- public error code 在 `.api.error`；core/store 内部异常不穿透；禁止裸 `IllegalStateException` 表达领域错误。

### 3.7 测试边界

- metadata 只测试自己的 `.api/.core/.store.jdbc`；迁出域的测试随代码迁移。
- 每个 owner 拥有单元、迁移、API contract、Testcontainers 集成测试。
- 关键状态机必须有 property test：artifact build、index publication、materialization publish、journal ordering、copy admission、streaming epoch。
- `full-regression` 必跑 metadata integration；普通 PR 至少跑 owner 模块 JDBC 测试。
- API minimal-classpath test：只带 `common`/允许 API 时，`metadata.api` 可 compile/link/load；不传递 Hikari/Flyway/JDBC/Jackson/Curator。

### 3.8 AI 模块最终命名

```text
pangrova-ai
├── org.pangrova.ai.api.*        # public model/function/provider/action descriptor
├── org.pangrova.ai.control.*    # evaluation/grounding/model-prompt registry/action policy
├── org.pangrova.ai.worker.*     # provider invocation/blob/embedding/rerank/inference
└── org.pangrova.ai.store.jdbc.* # evaluation/grounding/model registry persistence
```

- 当前只建 `pangrova-ai` 一个 code-bearing artifact；`control`/`worker` 是内部 package。
- 只有出现独立 provider SDK、独立部署/进程边界或依赖隔离需求时，才升级为 `pangrova-ai` family aggregator + `pangrova-ai-control`/`pangrova-ai-worker` 两个子 module。
- `.pangrova/rules/code-standards.md` 和 `long-term-modular-architecture.md` 中的 `pangrova-ai-control` 表述在 Wave 0 统一为 `pangrova-ai`（或明确为 `pangrova-ai` 子 module）。
- metadata 只允许依赖 `pangrova-ai.api`（如果未来需要），不得 import `.control/.worker/.store`。

### 3.9 目标 artifact 就绪度

| 目标 artifact | 当前 | 策略 |
|---|---|---|
| `pangrova-metadata` | 有 | 内部 `.api/.core/.store.jdbc` 重建 |
| `pangrova-indexing` | 有 | 接收 index/artifact control/store |
| `pangrova-streaming` | 有 | 接收 CDC/checkpoint store |
| `pangrova-materialization` | 有 | 接收 materialization store |
| `pangrova-governance` | 有 | 接收 quality/lineage/audit |
| `pangrova-security` | 有 | 接收 tenant store/policy execution/file access authority/directory observation/storage profile |
| `pangrova-observability` | 有 | 接收 query audit records |
| `pangrova-coordinator` | 有 | 接收 coordination registry/lease/ZK |
| `pangrova-query` | 未创建 | 新建 `.api/.core/.transaction/.store.jdbc`，接收 query state/write commit/anchor/copy admission |
| `pangrova-semantic` | 未创建 | 新建 `.api/.core/.store.jdbc`，接收 semantic |
| `pangrova-ai` | 未创建 | 新建单 artifact `.api/.control/.worker/.store.jdbc`，接收 evaluation/feature/grounding |
| `pangrova-table` | 未创建 | 维护域；本计划不接收 metadata 代码 |

### 3.10 明确不拆/不保留

- 不新建 `pangrova-metadata-core`/`metadata-api`/`metadata-store` 三个 JAR；同一 artifact 内 package 隔离。
- 不新建 `pangrova-metadata-coordination`；coordination 按 record owner 分到 query/coordinator。
- 不新建 `pangrova-artifact`；artifact 归 indexing。
- 不新建 `pangrova-ai-control` 顶层 artifact；统一 `pangrova-ai`，必要时才做子 module。
- 不保留 `metadata.model/service/store` 顶层包；不保留旧 `MetadataService`/旧 JDBC store 类。
- 不保留旧迁移、旧表、旧列、兼容视图、dual-write。

---

## 4. 影响范围（Impact Scope）

### 4.1 代码

| 范围 | 影响 |
|---|---|
| metadata | 296 文件 / 38,959 行全部进入重构；约 150+ 文件迁出（index/artifact/streaming/materialization/semantic/evaluation/feature/tenant/governance/coordination/checkpoint/file/storage/source/ai），其余重建为 `.api/.core/.store.jdbc`；file/storage 归 security，query audit 归 observability |
| coordinator | 146 个文件、1,114 处 import、133 个类需要切换到 `metadata.api` 或迁出域 API |
| testing | 36 个文件、119 处 import 的 fixture 需要迁到 testkit/owner |
| connector-lance | 2 个测试文件依赖 metadata；改用 testkit 或删 fixture |
| worker | `file_space_registry`/`file_directory_index` 的消费者；改用 security API，不依赖 metadata |
| indexing/streaming/governance/materialization/security/observability | 当前 0 metadata import；接收迁出代码 + 新增 JDBC 依赖 |
| query/semantic/ai | 新建 artifact/package；影响 root POM/BOM/reactor |
| 其他 | root POM、`.pangrova/modules.yaml`、CI classifier、docs |

### 4.2 数据库

- 删除 92 个迁移、114 张表的旧 history。
- 重建 10+ 个 per-owner baseline（含 security storage、observability audit）；删除零引用表/列/索引。
- 本地/测试 DB 直接重建；无数据迁移、无兼容视图、无 dual-write。
- 影响所有 JDBC/Testcontainers 测试（86 个文件）和 E2E fixture。

### 4.3 测试

- 141 个 test 源码文件、852 个 `@Test`：迁出域的测试随代码走；旧包测试删除。
- 15 个 integration 测试必须在 full-regression 执行。
- 新增 API contract、minimal-classpath、schema owner、migration baseline、property tests。
- E2E 8 条主链路：catalog DDL、resolve、write commit recovery、artifact build/publish、index generation、materialization publish、CDC restart、evaluation release gate。

### 4.4 构建与 CI

- `.pangrova/modules.yaml` 从无到有；加入 metadata 及所有新模块。
- ArchUnit default-deny、package-info、API classpath、migration owner、cross-domain SQL、SPI annotation、dead code、size baseline gate。
- root POM 模块清单从 25 调整到目标 24 root/36 leaf；新 `query`/`semantic`/`ai` 纳入 reactor。
- 移除未使用 Iceberg 依赖；新增各 owner 的 JDBC 依赖按需 optional/runtime。

### 4.5 文档

- `docs/architecture/metadata.md`：删除旧 module structure（仍写旧包/V1），改为目标 `.api/.core/.store.jdbc`。
- `docs/architecture/long-term-modular-architecture.md`：AI 命名统一为 `pangrova-ai`；metadata 搬迁表补 `control/file/storage/source/materialization` 的最终 owner（file/storage → security，COPY → query.transaction，source identity → metadata）。
- `.pangrova/rules/code-standards.md`：AI owner 名称统一；metadata 边界引用新结构。
- `docs/internal/metadata-module-review-2026-10-31.md`：修正过时结论（见 §10）。

### 4.6 不影响

- 无外部 API/DB 兼容承诺；无生产数据迁移；无旧客户端/旧部署。
- 不改变 SQL 语义、执行计划、JNI/native 边界；这些仍由现有测试/E2E 保证。

---

## 5. Clean-Cut 重构计划

### 5.1 Phase 0：删除与冻结（1–2 周）

目标：先减少噪声，再建新骨架。

| 任务 | 内容 | 验收 |
|---|---|---|
| P0-1 | 删除 5 个零引用死类：`DdlPropagationWaiter`、`InvalidationMetrics`、`MetadataSyncMetrics`、`CachingFederatedCatalog`、`TableInvalidationListener`；接线或删除 `MetadataCleanupScheduler` | dead code gate 0 |
| P0-2 | 删除未使用 Iceberg 依赖、Arrow 格式名、过程引用（F-META/A-HA/Task/BUG） | C1/依赖/过程引用扫描 0 |
| P0-3 | 删除重复实现：metadata 与 `pangrova-streaming` 的 `JdbcCdcRegistrationStore`/`JdbcWatermarkStore` 只留一套；governance 未接线 `JdbcColumnTagStore` 删除；`JdbcIndexPublicationStore` 与 `JdbcIndexArtifactPublicationStore` 合并；generated/index/tabular 三套 publish 合并为一套 | 每个事实一套实现 |
| P0-4 | 删除 8 处构造函数 DDL | `grep CREATE TABLE IF NOT EXISTS` = 0 |
| P0-5 | 建 `.pangrova/modules.yaml` + 把 metadata 纳入依赖 gate；生成 POM/import/package/migration DAG | 未登记 import = 0 |
| P0-6 | 冻结 metadata 新增一级包/表；新包/表必须走 ADR | PR 阻断 |
| P0-7 | 114 张表逐表登记 owner/目标 baseline；零引用表标记删除 | 无 orphan 表 |
| P0-8 | 删除旧迁移，建立 per-owner baseline 草案（可先空） | 本地 DB 可从 baseline 重建 |
| P0-9 | 修 P0 正确性：默认 InMemory、事件丢弃、缓存无界、COPY claim、catalog revision、watermark、物理删除 | 对应测试通过 |

### 5.2 Phase 1：重建 Metadata API/Core/Store（3–5 周）

目标：给 Coordinator 稳定 API，删除旧 `MetadataService` 路径。

| 任务 | 内容 | 验收 |
|---|---|---|
| P1-1 | 建 `metadata.api.*`：catalog/object/schema/binding/control/statistics/event/error | API package 不依赖 core/store |
| P1-2 | 建 `MetadataGateway` + ≤7 方法 use-case ports；删除 `MetadataService` 巨型接口 | 每接口 ≤7 方法 |
| P1-3 | 重建 `metadata.core.*`：catalog/resolution/binding/mutation/statistics/control/dependency/cache/event/source | 内部无环（ArchUnit） |
| P1-4 | 重建 `metadata.store.jdbc.*`：repository/mapping/transaction/outbox；删除旧 `store/`/`service/`/`model/` | 0 构造 DDL；store 只依赖 api/core port |
| P1-5 | 拆 12 个 >500 行类中的 metadata 侧 6 个：DefaultMetadataService、TableControlService、JdbcWriteCommitJournal、JdbcIndexDescriptorStore、JdbcArtifactBuildStore、JdbcMaterializationRepository | 单类 ≤500；>800 = 0 |
| P1-6 | Coordinator 146 文件/1,114 import 切到 `metadata.api` 或迁出域 API | 对 `metadata.core/store` import = 0 |
| P1-7 | testing/connector fixture 切到 testkit/owner；不在 metadata 留测试 fixture | 0 跨域 test import |
| P1-8 | 统一错误/JDBC/JSON：public ErrorCode、一个事务模板、一个 JsonCodec；删除 4 套复制 | 重复模板 = 0 |

### 5.3 Phase 2：域迁出（4–8 周）

每个域独立一个可编译/测试绿的变更批次；**同一批次删除旧包/旧表/旧测试**，不留兼容。

| 顺序 | 域 | 目标 | 删除的旧内容 | 验收 |
|---:|---|---|---|---|
| 1 | Tenant | `security.core/store.jdbc` | metadata `tenant/`、`tenants`/`tenant_user_memberships` 旧表归 security baseline | tenant 测试；metadata 无 tenant import |
| 2 | Governance quality/tag | `governance.quality/catalog` | metadata `governance/`、`quality_*` 表；未接线 `JdbcColumnTagStore` | 质量规则测试；tag binding 仍 metadata |
| 3 | Index + Artifact | `indexing.control/store.jdbc` | metadata `index/`、`index/jdbc/`、`index/model/`、`artifact/` 全部；旧 index/artifact 表 | 三类型 build/publish；关闭 index 不影响基础 SQL |
| 4 | Streaming + CDC checkpoint | `streaming.control/store.jdbc` | metadata `streaming/`、CDC/offset/watermark/state 表；同名 JDBC 只留一套 | CDC 恢复/offset/watermark 测试 |
| 5 | Materialization | `materialization.control/store.jdbc` | metadata `materialization/`、`dynamic_table_*` 旧表 | refresh/publish/GC 测试 |
| 6 | Semantic | `semantic.api/core/store.jdbc` | metadata `semantic/`、semantic 表；metadata 只留 ObjectId/name binding | typed semantic 发布测试 |
| 7 | Feature/Evaluation/AI | `pangrova-ai.control/store.jdbc` | metadata `feature/`、`evaluation/`、`ai/`；evaluation/feature 表 | evaluation/release gate/grounding 测试 |
| 8 | Query/Coordinator coordination | `query.transaction` + `coordinator.coordination` | metadata `coordination/`、`checkpoint/` 按 record 拆分 | query recovery/write commit/lease/registry 测试 |
| 9 | File/Storage/Source | `security.storage.store`（按 security 既有 `<domain>.store` 约定，同 tenant.store；原 3.2 简写 `.core.storage/.store.jdbc.storage` 落实为模块实际约定）+ `query.transaction` + `metadata.store.jdbc.source` | metadata `file/`、`storage/`、`source/jdbc/` 按 3.2 决策拆分；worker 走 security API（现状 worker 无 metadata.file/storage 引用，无需改动） | COPY/managed source/file space 测试 |
| 10 | Query audit | `observability.audit` | metadata 中 `query_audit_records` 迁移；不删除表（代码迁移已自然完成：metadata 主代码无 query-audit Java，observability 已自有 `JdbcAuditStore`；仅剩 Flyway `V7` 重分布随 P3） | observability audit 测试 |

### 5.4 Phase 3：数据库 baseline 落地（与 Phase 2 并行）

| 任务 | 内容 | 验收 |
|---|---|---|
| P3-1 | 删除旧 `db/migration`，写入 per-owner baseline | 空库可启动 |
| P3-2 | 每个 baseline 只含 owner 表；零引用表/列删除 | 无 orphan 表 |
| P3-3 | 独立 `flyway_schema_history_<owner>`；per-owner location | 迁移互不影响 |
| P3-4 | 删除构造函数 DDL、兼容视图、dual-write 表 | 0 构造 DDL |
| P3-5 | Testcontainers/E2E 从 baseline 启动；删除旧 fixture | full-regression 绿 |
| P3-6 | schema owner gate：解析 SQL 表名，禁止跨 owner 读写 | 0 跨 owner SQL |

### 5.5 Phase 4：删除历史残留（1–2 周）

| 任务 | 内容 | 验收 |
|---|---|---|
| P4-1 | 删除旧 package/class/test/fixture/配置 | 旧名字扫描 0 |
| P4-2 | 删除旧 migration/table/column/index | DB schema 扫描 0 |
| P4-3 | 删除 `@Deprecated`/兼容 adapter/feature flag for old path/legacy wiring | 0 |
| P4-4 | 更新 metadata.md/long-term/code-standards/review | 文档无旧结构 |
| P4-5 | 运行 full-regression + E2E；审计 no-residue checklist | 全绿 |

#### 执行进展（2026-11-01）

- Phase 2 Wave 9（File/Storage/Source 域迁出）代码迁移已全部落地：metadata 主树只剩 `api/core/store`；`metadata.file`/`metadata.storage` 主包与空目录已删除，COPY 读准入迁入 `query.transaction`。
- 4 个存量域测试（`JdbcFileDirectoryIndexTest`、`JdbcFileSpaceAuthorityTest`、`JdbcFileSpaceRegistryTest`、`JdbcStorageProfileRegistryTest`）已从 metadata 测试树物理迁入 `pangrova-security` 的 `org.pangrova.security.storage.store` 测试包，并改为使用 security 自持的 clean-cut 测试 baseline（`src/test/resources/db/security-storage-domain*.sql` + `SharedSecurityPostgres.storageDomainDataSource`），不再引用 metadata 的 `classpath:db/migration` Flyway 链；34 个用例在真实 PostgreSQL 上全绿。metadata 测试树已无 `metadata.file`/`metadata.storage` 残留引用。
- `EvaluationRunTest`（纯单元测试）已迁入 `pangrova-ai` 的 `org.pangrova.ai.api` 测试包并验证通过；3 个 semantic/evaluation/feature JDBC 测试也已随 P3 baseline 物理迁入 `pangrova-semantic`/`pangrova-ai` 并各自全绿（见 §3.5.1 P3-5 清单）。
- P3-2/P3-3/P3-4 已落地（2026-11-01）：10 个 owner baseline（`db/<owner>/migration/V1__baseline.sql`）为最终形态 DDL（含 query 基线补 `execution_anchor_token_seq` 独立序列，START WITH 2/CACHE 1 对齐旧链终态）；`SchemaMigrationRunner.migrateAll(DataSource)` 按依赖顺序应用 security（bundled，幂等）→ metadata → semantic → ai → coordinator → governance → indexing → streaming → query → materialization → observability，classpath 缺失的 owner 跳过；`CoordinatorBootstrap`、各模块 `SharedPostgresContainer`、E2E `MetadataSchemaInitializer` 与全部 JDBC 测试调用点已重接；旧 `db/migration` V1–V101（91 文件）、15 个迁移历史测试与 4 个额外历史 replay 测试已同批删除。
- P3 就近验证第一批（2026-11-01 深夜）：`pangrova-ai` 20/20、`pangrova-semantic` 3/3 在全新 install 的 jar 上各自全绿（各模块自持 baseline，不依赖 metadata 旧 Flyway 链）。验证暴露 materialization baseline 的跨 owner 依赖：`validate_attempt()` 读 coordinator owner 的 `distributed_lease` 与 query owner 的 `execution_anchor`，`materialization_native_commit_capture` trigger 挂在 query 的 `write_commit_journal` 上——metadata 测试 classpath 此前缺 coordinator baseline，导致 100 个容器测试在 baseline 应用阶段报 `relation "distributed_lease" does not exist`。已修复：metadata pom 增加 test-scope `pangrova-coordinator`（coordinator jar 自带 `db/coordinator/coordination/migration` baseline），materialization baseline 头部注释补记 coordinator 依赖。
- P3 就近验证第二批（materialization baseline 幂等性 fold 回归）：materialization baseline 由 pg_dump 生成，丢掉了旧链 V68 的幂等语义（`IF NOT EXISTS`/`OR REPLACE`），而 `materialization` 是唯一跨测试类共享的全局 schema（其余 9 个 owner baseline 全为未限定 DDL，落在各自隔离 schema，无碰撞）——第二个容器测试类起 225 处报 `schema "materialization" already exists`。已修复并验证：`CREATE SCHEMA/TABLE/INDEX` 加 `IF NOT EXISTS`，17 个函数改 `CREATE OR REPLACE FUNCTION`，16 个 trigger 改 `CREATE OR REPLACE TRIGGER`（PG16 支持），34 个 `ADD CONSTRAINT` 块包 DO guard 捕获 `duplicate_object/duplicate_table/invalid_table_definition`；scratch 库连续两次全量应用 0 错误、16 trigger 就位。另恢复 metadata `src/test/resources/logback-test.xml`（root INFO、jetty INFO、testcontainers INFO、dockerjava WARN）——历史版本在 checkpoint 模块重构中被误删，缺失时回退 root DEBUG（本轮实测 1 分钟即 12 MB / 3.2 万行 DEBUG）。
- P3 就近验证第三批（authorization revision trigger-managed 适配）：security 新加的 `guard_pangrova_authorization_revision` trigger（用户 Wave 9 WIP）使 `SecuritySchema.initialize` 的重复调用从幂等 no-op 变为报错 `Authorization revision is trigger-managed`，metadata 测试树 6 个类（`MetadataTagOperationsTest`、`TableControlRowPolicyStoreTest`、4 个 artifact JDBC 测试）因 migrateAll 已初始化 security schema 又显式 `initSchema()` 二次初始化而失败。修复：删除 migrated source 上冗余的 7 处 `initSchema()` 调用；`ArtifactAuthorityFixtures.provision` 改为先探测 `pangrova_authorization_revision` 表存在性、仅在 schema 缺失时 init（兼容 isolatedDataSource 未 migrate 的调用方，如 `ArtifactCurrentAuthorityIntegrationTest`）。security 模块自身与 query 测试树同类问题的处理随用户 Wave 9 收敛。修复后 metadata 全量测试 **390/390 绿（BUILD SUCCESS，2026-11-01 02:59）**——含全部容器测试，per-owner baseline 链（security→metadata→semantic→ai→coordinator→governance→indexing→query→materialization，streaming/observability 按 classpath 缺失跳过）在共享容器 DB 上连续多 schema 应用无碰撞。
- 就近验证阻塞项（均为并行重构中的用户 WIP，按约定不触碰、不回退）：security 模块测试（`security-policy-schema.sql` 与测试 `security-storage-domain*.sql` 双建 `file_directory_index` → multiple primary keys）；query 模块测试（`JdbcWriteCommitJournal.purge` 直读 indexing 的 `index_commit_log`；`JdbcCopyFileClaimStoreTest` 对 security schema 二次 `initSchema`）；`pangrova-common` `PangrovaErrorCodeTest`（enum 新增项未同步期望尺寸）；shuffle 聚合器 pom（`pangrova-shuffle-core` 缺 `pangrova-shuffle-api` version，root reactor 暂不可读，按 leaf 逐个 `-f <module>/pom.xml` 隔离验证）。
- P3-6（`JdbcCheckpointStore` 物理迁 query、coordinator 对 `write_commit_quarantine` 直读改走 query API）与 P4（残留清单 + full regression）仍待执行。
- P4-4 文档命名对齐已完成（2026-11-01）：`.pangrova/rules/code-standards.md` 的 AI 归属改为 `pangrova-ai`；`metadata-center-redesign.md` 与 `long-term-modular-architecture.md` 中全部 `pangrova-ai-control`/`pangrova-ai-worker` 表述统一为单一 `pangrova-ai`（`.control`/`.worker` 为内部 package），目录树、职责表、package ownership、依赖骨架统一重编号（36→35 个叶子，27–36 行上移），并同步修正 count 句（35 个叶子、32 个生产/客户端/打包制品、aggregator 名单去掉 AI 补上 Streaming）。`.pangrova/setup.sh --check` 通过；`docs/` 与 `.pangrova/rules/` 中除 `docs/internal/` 计划文档自身（有意保留的历史决策表述）外已无 `pangrova-ai-control`/`pangrova-ai-worker` 引用。
- 390/390 口径修正（2026-11-01）：该数为 metadata 模块**默认 surefire 范围**（`**/*Test.java`、excludedGroups=`integration`）的绿值——`*IntegrationTest` 类与 `@Tag("integration")` 用例被默认排除，真实 PostgreSQL 容器类由 `-Pfull-regression` 覆盖。P4 批次迁移把容器测试随 owner 移走后，各 owner 模块再各自跑 full-regression；metadata 自身全量（含 integration）验证排在 P4-1 三批迁完之后一次性执行（就近验证约定）。
- P4-1 治理批已完成（2026-11-01）：4 个 governance JDBC 测试（`JdbcQualityRuleStoreTest`、`JdbcQualityRuleStoreIntegrationTest`、`JdbcQualityRuleStoreRecoveryIntegrationTest`、`JdbcQualityResultIdentityIntegrationTest`）从 metadata 测试树物理迁入 `pangrova-governance` 的 `org.pangrova.governance.quality.store` 测试包，改接 governance 自持的 `SharedPostgresContainer`（`db/governance/migration` + `flyway_schema_history_governance`），metadata 侧 4 个原文件同批 `git rm` 删除、`MetadataStoreTestSchema.qualitySchema` 方法一并删除；pom 增加 testcontainers-postgresql/flyway-core/pangrova-test-common/postgresql/pangrova-security/junit-jupiter/assertj/jqwik/logback-classic 与 surefire DOCKER env。迁后暴露并修复 2 个失败：(1) Flyway 默认 `createSchemas(true)` 把 `schema_that_does_not_exist` 自动建出来，`schemaCreationFailurePreservesSqlState` 断言 3F000 失败 → governance 容器 `migrate()` 加 `.createSchemas(false)` 让缺 schema 错误原样浮出；(2) recovery 测试类内共享 schema 使 drop-tables 用例与 Flyway no-op 重跑互相污染 → 改为 `@BeforeEach` 建 per-test 隔离 schema（admin 连接 `CREATE SCHEMA "quality_recovery_<uuid>"`），并修正 `sourceFor` 密码参数化 bug。全套 **140/140 绿（BUILD SUCCESS，EXIT=0）**。
- P4-1 索引批已完成（2026-11-01）：22 个 artifact/index 测试（含 6 个集成类：`JdbcArtifactBuildStoreTest`、`ArtifactPublicationFenceLockIntegrationTest`、`ArtifactExecutionFenceLockIntegrationTest`、`JdbcArtifactStageExchangeStoreTest`、`JdbcIndexArtifactPublicationStoreTest`、`JdbcTabularExportPublicationStoreTest`）从 metadata 测试树物理迁入 `pangrova-indexing` 对应 `org.pangrova.indexing.*` 测试包，metadata 侧原文件同批删除。配套改动：`JdbcIndexArtifactPublicationStore` 的 `file_column_statistics` DDL 从 query 基线迁入 indexing 基线（表 owner 归一，两基线均已验证无同名残留）；`MetastoreException` 接缝改抛 `IndexStoreException`；artifact 权威测试拆为 indexing 持 owner 半 + metadata 留 grant 语义半（`ArtifactAuthorityFixtures` 保留在 metadata）；`testinfra` 包从 metadata 迁入 indexing；`AnchorRowLockTest` 随类更名 `ArtifactExecutionFenceLockIntegrationTest`；`fixtureAnchorRow` helper 由生产代码 `ArtifactExecutionFenceLock` 静态内建替代。主代码硬编码两处安全校验（`ArtifactCurrentAuthority`→`JdbcTenantStore.requireDeferredTenant`、`CurrentDeferredExecutionContext`→`PersistentDeferredAuthorization.requireCurrentRoles`）绕过不了测试替身，新增 `ArtifactAuthorityTestDoubles.provisionTenant`（createRole+grantRoleToUser+createTenant+addUserToTenant）并在 5 个集成类接线。两轮运行时修复（缺 tenant 成员行 → 缺角色行）后全套 **254/254 绿（BUILD SUCCESS，EXIT=0，-Pfull-regression）**。
- P4-1 物化批已完成（2026-11-01）：`JdbcMaterializationRepositoryTest`（4 个用例，覆盖 reserve/commit/publish 幂等、rename 全表层、fence 过期与证据冲突、发布投影与依赖触发）从 metadata 测试树物理迁入 `pangrova-materialization` 的 `org.pangrova.materialization.jdbc` 测试包，metadata 侧原文件同批 `git rm`，metadata 测试树已无 `org.pangrova.metadata.materialization` 引用（test-compile 绿）。配套改动：新建 materialization 自持的 `testinfra.SharedPostgresContainer`（postgres:16-alpine 单例，`migratedDataSource` 按 security（bundled `SecuritySchema.initialize`）→ indexing → query → **内联 coordinator 的 `distributed_lease` DDL** → materialization 顺序应用 per-owner baseline；`distributed_lease` 因 coordinator MAIN 依赖 materialization、test 反向依赖会形成 reactor 环而内联，仅建表+PK，无历史残留语义）；pom 增加 test 依赖（pangrova-indexing/pangrova-query/pangrova-security/pangrova-test-common/testcontainers-postgresql/flyway-core/postgresql/logback-classic）与 surefire DOCKER env；新增 `logback-test.xml`（与 indexing 同款 INFO 规格）。三轮运行时修复：(1) 内联 `distributed_lease` 缺 PK → `ON CONFLICT (lease_key)` 失败，按 coordinator baseline 原样补 `distributed_lease_pkey`；(2) `purgeJournal` 触发的 query 基线 `validate_copy_write_journal_delete()` 运行时读 indexing 的 `index_commit_log` → 前置应用 indexing baseline；(3) 前置需 pangrova-indexing 新 jar install（携带现行 `db/indexing/migration`）。全套 **177/177 绿（BUILD SUCCESS，EXIT=0，-Pfull-regression）**。
- P4-2 DB schema 残留扫描已完成（2026-11-01）：10 个 owner 各仅一份 `V1__baseline.sql`（无 V2+ 旧迁移链），全仓无 `db/migration` 旧目录；跨 owner baseline 无同名表冲突；`deprecated/legacy/_backup/_old/zzz_/obsolete` 关键词扫描仅命中各 baseline 头部自述（"No legacy columns, compatibility views, dual-write tables or pre-split history"）与 query 基线一句序列重置注释（"final shape of the legacy chain's fencing-token migration"），无旧列/旧索引/兼容视图/dual-write 表残留。
- P4-3 @Deprecated/兼容 adapter/feature-flag 扫描已完成（2026-11-01）：metadata 主树 `@Deprecated` 0 处；`compat/legacy/backward` 命中均为真实产品语义——schema evolution 兼容性校验（`SchemaValidationResult`/`DefaultMetadataService`/`MetadataSyncService`）、table-control 的 legacy-write-allowlist（登记存量表的运行时特性，非重构残留）与普通注释；`useLegacy/useNew/featureFlag` 等旧路径开关 0 处，`enabled()` 命中均为 table-control 运行时开关本身。P4-3 验收 0 达成。
- metadata 测试 classpath 自制品冲突修复（2026-11-01）：P4-1 三批迁完后 metadata 首次 full-regression 报 127 处 Flyway `Found more than one migration with version 1`——已 install 的 `pangrova-metadata` jar（自带 `db/metadata/migration/V1__baseline.sql`）经 `pangrova-query` 与 `pangrova-coordinator` 两条 MAIN 依赖链回到 metadata 自身测试 classpath，与 `target/classes` 拷贝并存双 V1。修复：metadata pom 两个 test 依赖各加 `<exclusion>org.pangrova:pangrova-metadata</exclusion>`，`dependency:tree` 复核零自制品子节点（类从 `target/classes` 解析，无回归风险）。
- 授权事务门闩缺装修复（2026-11-01）：`ArtifactCurrentAuthorityIntegrationTest.publicationAndDirectSqlRevocationAreLinearlyOrderedByOneTransactionGate`（P4-1 索引批留下的 metadata 半）在 full-regression 下确定失败——`ArtifactAuthorityFixtures.provision` 见 `pangrova_authorization_revision` 已存在即跳过 `initSchema()`，导致运行时门闩 `AuthorizationTransactionGate.initialize`（`pangrova_authority_mutation_gate` + 各权威表 statement trigger）从未安装，直连 SQL 撤权无法与发布线性化。修复：provision 拆两路——schema 已存在（migrateAll 迁移过的源）时仅幂等安装门闩（`lockMutation` + `initialize`，全 `CREATE OR REPLACE`，不触碰 revision 表、不触发用户 WIP 的 trigger-managed guard）；schema 缺失时走完整 `initSchema()`。单测复核 1/1 绿。
- P4-1 旧名残留清理已完成（2026-11-01）：`JdbcTenantStoreTest`（11 用例）从 metadata 测试树物理迁入 `pangrova-security` 的 `org.pangrova.security.tenant.store` 测试包（改接 security 自持的 `SharedSecurityPostgres.storageDomainDataSource`，11/11 绿）；3 个留存测试类包名归位（`ArtifactCurrentAuthorityIntegrationTest`、`JdbcScalarIndexAuthorityTest` → `metadata.store.jdbc`，`MetadataCollectionOwnershipTest` → `metadata.api.schema`）；11 个空目录（artifact/index/tenant/adapter/evaluation/feature/materialization/model/property/schema/semantic/serde 层级）与死 helper `MetadataStoreTestSchema.tenantSchema` 同批删除。metadata 测试树归并为 `api/core/store/testinfra`，测试主树与 §P4 目标一致。
- metadata 最终 full-regression（2026-11-01）：**393/393 绿（BUILD SUCCESS，EXIT=0，-Pfull-regression）**——404 原口径减 11 个迁出 security 的 `JdbcTenantStoreTest` 用例；覆盖全部 Testcontainers 容器类与 per-owner baseline 链（security→metadata→semantic→ai→coordinator→governance→indexing→query→materialization 按 classpath 逐 owner 应用，streaming/observability 缺失跳过）。同批修复两项回归阻断：pom 自制品双排除（Flyway 双 V1：query/coordinator 两条 MAIN 依赖链把已 install 的 metadata jar 带回自身测试 classpath）与 `ArtifactAuthorityFixtures` 门闩分路安装（schema 已存在时仅幂等装 `AuthorizationTransactionGate`，直连 SQL 撤权与发布保持单门闩线性化）。
- E2E 编译级验证（2026-11-01）：`pangrova-test-e2e` test-compile 对现行 owner jar **EXIT=0**——P3-2 重接的 `MetadataSchemaInitializer` 与全部 E2E 调用点在 per-owner baseline 形态下编译通过。E2E 执行级验证不适用于本机：8 条主链路测试均经 `PangrovaE2ETestBase`/`SqlE2ETestBase` 加载进程内 `NativeRuntime`（打包 JNI），本机无 `pangrova-worker-native/build`、~/.m2 worker jar 内无 .so/.dylib；C++ 全链（build-pipeline Step 0–7）按 AGENTS.md 属 CI/release gate，且本次拆分不触碰 JNI/native/执行路径（§4.6 承诺不变）。
- P4-5 审计汇总（2026-11-01）：full-regression 各 owner 就近验证全绿——governance 140/140、indexing 254/254、materialization 177/177、ai 20/20、semantic 3/3、metadata 393/393；no-residue 终审 0——metadata 主树 `api/core/store`、测试树 `api/core/store/testinfra`、`@Deprecated` 0 处、源码无 `db/migration` 旧目录、无 `classpath:db/migration` 引用、10 个 owner 各仅一份 `V1__baseline.sql`。阻塞项全部为并行重构中的用户 WIP（security/query/common/shuffle，清单见上），按约定不触碰。
- P3-6 遗留（计划内显式后置项，随用户并行重构收敛后补）：`JdbcCheckpointStore` 仍在 `metadata.core.checkpoint`（计划 §3 注明"尚未物理迁入"）；`write_commit_quarantine` 表已归 query baseline，但 coordinator 的 `JdbcWriteCommitQuarantineStore`/`WriteCommitRecoveryService` 仍有直读 SQL（P3-6 门禁"0 跨 owner SQL"未达成）。两处均落在用户正在 staged 的 query 模块重构（`pangrova-query` 主树大量 `AM` 文件在途）与 write-commit recovery 活跃区内，按计划顺序待并行重构收敛后执行。

### 5.6 工作量与顺序

| Phase | 估算 | 主要风险 | 缓解 |
|---|---:|---|---|
| Phase 0 | 8–12 人日 | 删除误伤/漏删 | 引用扫描 + dead code gate；每删一类跑编译 |
| Phase 1 | 25–35 人日 | Coordinator 1,114 import 切换 | 先 API 后实现；按包分批；编译器驱动 |
| Phase 2 | 35–50 人日 | 域迁出/表重建 | 每域独立批次；先零依赖域；同一批次删旧 |
| Phase 3 | 12–18 人日 | baseline 漏表/权限 | 表级 owner 清单；Testcontainers 重建 |
| Phase 4 | 8–12 人日 | 残留 | checklist + grep/ArchUnit/SQL 扫描 |
| 合计 | 88–127 人日 |  | 5 个里程碑，每个可独立回滚 |

---

## 6. 前 90 天执行细节

### 第 1–2 周：删除与基线

- 删除死类、未使用依赖、重复实现、构造函数 DDL、过程引用。
- 建 `.pangrova/modules.yaml`，把 metadata 纳入依赖 gate。
- 114 张表 owner 清单；零引用表标记删除。
- 删除旧迁移，建立 per-owner baseline 草案。
- 修正 review 文档（§10）。

### 第 3–4 周：Metadata API 骨架

- 建 `metadata.api.*` 与 `MetadataGateway`；每接口 ≤7 方法。
- API contract test + SPI 注解 + package-info。
- 建 API minimal-classpath test。
- Coordinator 先切 `catalog/model/control` 调用到 API；记录 import 下降。

### 第 5–8 周：Core/Store 与 god class 拆分

- 建 `metadata.core.*`/`metadata.store.jdbc.*`；删除旧 `service/store/model`。
- 拆 DefaultMetadataService、TableControlService、JdbcWriteCommitJournal、JdbcIndexDescriptorStore、JdbcArtifactBuildStore、JdbcMaterializationRepository。
- 统一 JDBC/错误/JSON；删除复制。
- 第一域（tenant）迁出 + 旧表删除 + baseline 更新。

### 第 9–12 周：Index/Streaming/Materialization 迁出

- 按 Phase 2 顺序迁 index/artifact、streaming/checkpoint、materialization。
- 每域同批删除旧包/旧表/旧测试；更新 Coordinator import。
- 运行 full-regression；修复行为回退。
- 开始 Semantic/AI 的 package 设计与 typed model 迁移。

---

## 7. 无历史残留（No-Residue）清单

### 7.1 代码

- [ ] `org.pangrova.metadata` 下只有 `api/`、`core/`、`store/jdbc/`；无 `service/`、`store/`、`model/`、`artifact/`、`index/`、`streaming/`、`materialization/`、`semantic/`、`evaluation/`、`feature/`、`tenant/`、`governance/`、`file/`、`storage/`、`source/`、`ai/`、`coordination/`、`checkpoint/`、`adapter/` 顶层包。
- [ ] 无 `MetadataService`、`JdbcMetadataStore`、`JdbcIndexMetadataStore`、`JdbcArtifactBuildStore`、`JdbcMaterializationRepository`、`TableControlService` 旧类；如重命名，旧类已删除。
- [ ] 无 `@Deprecated`、兼容 adapter、dual-write、旧路径 feature flag、legacy wiring（如 `GovernanceWiring`）。
- [ ] 无 dead code/UOE 无 `@Experimental`/重复同名类/过程引用。
- [ ] 无 Iceberg/Arrow 禁用依赖或命名。
- [ ] 只有 `.api` export；其它 leaf 对 `metadata.core/store` import = 0。

### 7.2 数据库

- [ ] 无 V1–V101 旧迁移；只有 per-owner baseline。
- [ ] 无 `public` 大杂烩 schema；per-owner schema/history。
- [ ] 无 `orphaned_queries`、旧 `dynamic_table_*`、旧 `column_tags`、双写表、兼容视图；`query_audit_records` 已归 observability，`governance_audit_trail` 已归 governance。
- [ ] 无构造函数 DDL；Flyway 唯一 owner。
- [ ] 每张表有唯一 owner；无 orphan 表/列/索引。

### 7.3 测试与文档

- [ ] 无旧包测试/fixture；无 `@Disabled`/永久 false assumption。
- [ ] integration 在 full-regression 执行。
- [ ] `metadata.md` 不再描述旧 module structure；long-term/code-standards 的 AI 命名统一为 `pangrova-ai`。
- [ ] review 文档标记为历史 review，并修正过时结论。

### 7.4 构建

- [ ] root POM/BOM 无旧模块/GAV；新 `query/semantic/ai` 已登记。
- [ ] `modules.yaml` 是唯一事实源；manifest/ArchUnit gate 生效。
- [ ] 无未使用依赖/传递 JDBC/Hikari/Flyway/Jackson 到 API consumer。

---

## 8. 验收标准

### 8.1 边界

- metadata 只 export `.api`；`core/store.jdbc` 不被其它 leaf import。
- metadata POM 只依赖允许的 `common`/`spi`/`security.api`（如需要）；不依赖迁出域实现。
- 0 跨 owner 表访问；0 未登记 import/package root。
- 每张表/每个迁移有唯一 owner；0 orphan。
- 0 构造函数 DDL；0 缺失/重复迁移。

### 8.2 结构

- 新代码 file ≤500 行；>800 行 = 0。
- 接口 ≤7 方法；构造依赖 ≤6。
- 0 未注解 exported 类型；0 UOE 无 `@Experimental`。
- 0 dead code；0 跨模块同名类；0 过程引用。
- 只有 `.api` export；无横向 `service/model/store/util/impl` 包。

### 8.3 正确性/测试

- 默认 InMemory 控制面不存在（显式 metadata-less 除外）。
- 事件队列满不丢关键失效事件；cache 有界；失效后无旧值。
- watermark/offset 单调；COPY claim 明确；catalog revision 有测试。
- materialization 物理删除不在 DB 事务内。
- metadata integration 在 full-regression 必跑；关键状态机 property test。
- API minimal-classpath、migration baseline、E2E 8 条主链路通过。

### 8.4 终态

- metadata 只拥有 technical object identity/schema/version/binding/statistics/table control/managed-source identity。
- index/artifact 在 indexing；streaming/checkpoint 在 streaming/query；materialization 在 materialization；semantic 在 semantic；evaluation/feature/grounding 在 `pangrova-ai`；quality/lineage/audit 在 governance；tenant/policy/file access/storage profile 在 security；query audit 在 observability；query/write commit/anchor 在 query.transaction；registry/lease 在 coordinator.coordination。
- 旧 package/表/迁移/API/test/adapter 全部删除；`modules.yaml` 为 CI 强制事实。

---

## 9. 风险与回滚

| 风险 | 影响 | 缓解 |
|---|---|---|
| Clean cut 一次性改动过大 | 长时间不可编译 | 按 owner 分批；每批编译/测试绿；同批删除旧代码 |
| Coordinator 1,114 import 切换遗漏 | 编译失败/行为回退 | 编译器驱动；先 API 后删除；按包清单核对 |
| 域迁出后测试 fixture 未迁 | 测试失败/覆盖下降 | 每域同批迁移测试；testkit 统一 fixture |
| baseline 漏表/权限 | 启动失败 | 表级 owner 清单；Testcontainers 空库/启动测试 |
| 删除旧表误伤未发现功能 | 功能缺失 | 先 grep/引用扫描；运行 full-regression/E2E；每域保留行为测试 |
| 目标 query/semantic/ai artifact 未建 | 无法接收代码 | 先建 package/API/baseline；artifact 就绪后移动 |
| 大环依赖未打破 | 新包仍循环 | 先定义 API/port；ArchUnit 零环；禁止直接搬文件 |
| 规范/文档再次漂移 | 回到旧状态 | manifest + package-info + 月度评审 + ratchet |

回滚：

- 每个 Phase 独立分支/PR；回滚 = revert + 重建本地 DB；无兼容层需要清理。
- Phase 1/2 的域迁移按域回滚；不保留旧表/旧 API 的运行时开关。
- Phase 3 baseline 在合并前用空库和 Testcontainers 验证；失败直接回滚 baseline。

---

## 10. 一句话总结

**新项目没有兼容包袱，直接把 metadata 重构为单一技术事实 owner artifact：只 export `.api`，只拥有 identity/schema/version/binding/statistics/table control；query、coordinator、indexing、streaming、materialization、governance、security、semantic、pangrova-ai 各按 record owner 接收自己的状态、表、迁移和测试；旧包、旧表、旧迁移、旧 API、旧测试、兼容层全部删除，用 `modules.yaml + ArchUnit + migration baseline + no-residue checklist` 保证重构后没有历史残留。**
