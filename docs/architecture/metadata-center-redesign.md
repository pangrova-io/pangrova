# Pangrova 统一元数据中心重构蓝图

> **Status: Target architecture.** 本文中的新类型、模块与迁移步骤是目标设计，
> 不是当前代码已经提供的 API；当前行为以生产 wiring 与测试为准。

## 核心决策

Pangrova 应建立一个统一的元数据中心，但“统一”必须被定义为：

- 所有 catalog、namespace、table、view、function 的身份与名称解析有唯一入口；
- 所有查询在规划时获得不可变、版本化、授权后的表绑定；
- 所有元数据变更遵守统一的版本、幂等、事件和缓存协议；
- 所有 Connector 通过小型能力接口提供格式原生元数据；
- 所有实时系统视图和历史库存来自同一对象身份体系。

“统一”不意味着所有持久化状态都归 `pangrova-metadata` 所有。查询 journal、worker registry、execution anchor、分布式 lease、索引构建任务、流式 offset、动态表 refresh、租户配额和治理结果仍是各自业务域的状态。业务 Concept/Metric/Relationship/Ontology/Semantic View 由 Metadata 上层的 `pangrova-semantic` 拥有，AI grounding/evaluation 由 `pangrova-ai-control` 拥有；它们可以共享一个 PostgreSQL 集群，也可以引用统一的 `CatalogId/TableId/FieldId/VersionToken`，但不能继续共享一个没有边界的 Java 模块和 migration 所有权。

推荐的最终形态是：

```text
一个逻辑元数据中心
  = 一个 MetadataGateway
  + 一套稳定对象身份与版本模型
  + 一个 catalog 控制面事实源
  + 多个格式原生 metadata provider
  + 一个版本化缓存体系
  + 一个可靠变更事件源

不等于
  = 一个包含所有控制面状态的 pangrova-metadata 大包
```

这是最适合 Pangrova 的 Snowflake、Iceberg 和 Paimon 组合：借鉴 Snowflake 的集中事务元数据、查询入口和版本绑定；借鉴 Iceberg/Paimon 的格式原生快照、schema/manifest 和 OCC；保留 Pangrova Java 协调、C++ 执行、Connector 隔离的现有边界。

## 当前问题为何需要重构而不是整理

当前工作区中的 `pangrova-metadata` 有 222 个 Java 主代码文件、约 2.47 万行和 26 个一级包。只有一部分属于查询元数据：`catalog`、`model`、`schema`、`service`、部分 `cache/store/event`。其余大量代码分别属于 indexing、artifact build、coordination、streaming、dynamic table、tenant、semantic/evaluation 和 governance。

数据库 V1 migration 也承担了 catalog/table mirror、query/task journal、worker/coordinator registry、distributed lease、index、artifact、dynamic table、streaming、cache invalidation、quality 和 tenant 等多个领域的建表职责。与此同时，一部分 repository 又在构造函数中自行执行 `CREATE TABLE IF NOT EXISTS`。

这造成的不是目录观感问题，而是四个架构错误：

1. **权威源不清楚**：表格式 catalog、JDBC `table_metadata`、Connector handle、Calcite cache 都可能代表“当前表”。
2. **生命周期不一致**：Catalog/DDL 经 `MetadataService`，但统计、split、写入、系统表等主路径直接取得 `ConnectorMetadata`。
3. **版本没有统一语义**：当前存在 `long snapshotId`、`String snapshotToken`、table handle 内嵌 schema、query 级 pin、name-only cache 等多套机制。
4. **故障域被放大**：元数据 migration 失败可以阻断 query HA、索引构建、流式摄取和治理；任何领域扩展都会继续扩大 metadata 模块。

仅把包移动到新的子目录不会解决这些问题。必须先定义对象身份、事实源、查询绑定和变更协议，再按该协议迁移现有代码。

## Snowflake 元数据架构深度分析

### 集中的是事务性 hard state，而不是单个业务模块

Snowflake 的经典架构将系统分为数据存储、Virtual Warehouse 和 Cloud Services 三层。Cloud Services 负责身份认证、对象解析、权限、查询优化、事务与元数据管理；持久数据位于共享存储，计算集群可以独立创建、销毁和扩缩。[^1]

Snowflake 从 2014 年开始使用 FoundationDB 作为元数据底座。公开资料描述的需求是高频小读写、小范围扫描、ACID、高可用、跨版本 schema 演进和 change watch；Cloud Services 本身尽量无状态，所有 hard state 进入事务存储。[^2]

这里最容易产生错误类比：Snowflake 把 query state、transaction、lock queue、catalog object 等都称为 metadata-backed state，但并不意味着这些对象由一个 `MetadataService` 类负责。它表达的是**共同的持久化基础和事务规则**，不是放弃领域所有权。

对 Pangrova 的正确借鉴是：

- Catalog 与表身份由统一中心管理；
- Coordinator、Indexing、Streaming 等领域各自拥有状态模型；
- 各领域通过同一个稳定对象 ID 体系关联；
- store 必须支持小事务、CAS、watch/outbox 和 schema version；
- 服务可以无状态重启，不能依赖某一 Coordinator JVM 中的 map 作为事实源。

不应直接引入 FoundationDB。当前 Pangrova 已经依赖 PostgreSQL/Flyway，规模需求也没有证明 PostgreSQL 是瓶颈。应先用短事务、正确索引、transactional outbox 和 repository 隔离建立语义；只有量化证明写冲突、吞吐或多区域延迟无法满足 SLO 时，再替换存储实现。

### 所有查询先经过统一的解析、授权和优化入口

Snowflake 原始论文明确说明，查询在 Cloud Services 中完成 parsing、object resolution、access control 和 optimization，然后才把计划发送到计算节点。优化统计在装载和更新时自动维护。[^1]

这一点比“使用什么数据库存 metadata”更重要。Pangrova 当前有 53 处 Coordinator 主代码直接调用 `CatalogHandle.getMetadata(...)`，意味着没有一个结构性保证可以确保：

- 对象先授权再解析；
- table handle 与 schema/statistics 属于同一版本；
- 同一查询的 split 不重新解析 current table；
- cache key 包含 tenant、catalog generation 和 table version；
- DDL 和 query lifecycle 总能产生对应事件。

目标必须是所有**元数据语义操作**经过 `MetadataGateway`。执行数据的 page source/page sink 可以通过独立 `ConnectorRuntime` 路由，但它只能消费元数据中心产生的 `TableBinding`，不能重新按名称获取当前表。

### Snapshot Isolation 建立在不可变文件与版本元数据之上

Snowflake 使用不可变文件和 MVCC 实现 Snapshot Isolation。一个事务中的读取看到事务开始时的一致快照；insert/update/delete/merge 通过相对于旧版本增加或移除完整文件形成新表版本，文件集合变化记录在事务性元数据中。[^1]

这一模式与 Iceberg/Paimon 的共同点非常强：

- 数据文件先生成但不可见；
- 一个原子 metadata commit 发布新版本；
- reader 显式绑定已提交版本；
- 旧版本在 retention 窗口内继续可读；
- 写冲突由 OCC/requirements 验证，而不是覆盖 current 状态。

Pangrova 当前只保存一个 JVM 内 snapshot ID，不能达到这一语义：split session 没有 queryId；Iceberg/Paimon pin 是 query 级而不是 table 级；本地 pin 也无法阻止外部 snapshot expiration。

目标查询语义应定义为：

```text
默认保证：statement 内每张表 repeatable read
条件保证：同一 versioned catalog/ref 内可提供 catalog-consistent read
不保证：互不相关 catalog 之间的全局同一时刻快照
```

如果未来需要 Snowflake 式全局 statement snapshot，必须由 versioned catalog、多表事务协议或统一托管 catalog 提供，不能由 Coordinator 按顺序读取多个 current snapshot 假装实现。

### Universal Metadata 的关键是逻辑/物理解耦

Snowflake 2025 年公开的 universal metadata 设计强调逻辑类型、执行引擎表示、持久编码和 table format 之间解耦；同一元数据抽象可以覆盖 native table、Iceberg 与 Hybrid Table，同时又允许格式/工作负载特定的统计扩展。[^3]

这不是“取所有格式字段的并集”。它要求找到恰当的稳定公分母，并为格式特性保留 extension：

- 通用层：object identity、qualified name、schema field identity、logical type、version、ref、partition transform、sort order、capability；
- Iceberg extension：metadata location/version、snapshot/sequence、schema/spec/order ID、refs、delete semantics；
- Paimon extension：snapshot UUID/ID、schema ID、primary key、bucket mode、merge engine、changelog、commit identity；
- 执行层：opaque scan token 和 connector handle；
- 优化层：统一 `StatisticsEnvelope`，内部 payload 可由格式 provider 扩展。

当前 Pangrova `TableMetadata` 只有名称、列、字符串 partition/sort/properties，既不足以表达稳定逻辑模型，也不足以表达格式版本。继续向这一 record 填充所有 Iceberg/Paimon 字段会形成新的 God Object，应整体替换。

### 物理统计是版本化派生元数据

Snowflake 为每个 micro-partition 自动维护列范围、distinct count 和其它优化属性，在编译和执行时进行静态/动态 pruning。该信息远小于数据本身，并随装载和更新自动维护。[^1] [^4]

Pangrova 可借鉴其“自动、版本绑定、面向 pruning”的原则，但不能复制 micro-partition 实现：Iceberg/Paimon 已有 manifest/file statistics，Pangrova 又有独立 indexing 模块。

适合 Pangrova 的分层是：

- 表级 row/file/size/column statistics：元数据中心提供统一读取接口，按 `TableVersionToken` 缓存；
- manifest/file-level statistics：Connector 原生拥有，split planning 按需读取；
- Pangrova Bloom/vector/text 等增强索引：`pangrova-indexing` 拥有，与 table version 建立依赖；
- 动态 join filter：执行期状态，属于 planner/executor，不持久化到 catalog metadata；
- query history/cost statistics：observability 拥有，不属于表定义。

### 实时目录和历史库存必须分离

Snowflake 同时提供低延迟 Information Schema 和有延迟、长保留、包含 dropped object 的 Account Usage。官方文档明确区分两者的新鲜度和保留目标，并使用内部 object ID 区分同名对象的 drop/recreate。[^5]

这对 Pangrova 很重要：当前 `MetadataSyncService` 同时承担查询 mirror、搜索、drift 检测和失效触发，导致实时正确性与最终一致盘点混在一起。

目标应拆成两个产品面：

| 产品面 | 数据源 | 一致性 | 用途 |
|---|---|---|---|
| `information_schema` | MetadataGateway + connector current/version cache | 实时或显式版本 | planning、DDL、交互查询 |
| `metadata_history` / `account_usage` | outbox/connector event 派生库存 | 最终一致，长保留 | 搜索、审计、治理、成本分析 |

历史库存允许延迟，但必须公开 `observed_at/source_version/lag`；实时查询不可在 connector 不可用时无提示地回退到库存。

### 外部湖仓表需要“链接”和“深层映射”两种模式

Snowflake 对外部 Iceberg 提供 per-table link 与 catalog-linked database 两种模式。前者是显式单表映射，生命周期解耦；后者是 catalog/namespace/table 的深层语义映射，可进行自动发现、rename/drop 同步和双向 DDL。其工程说明特别强调 data lake 任意 namespace 层级与 warehouse 固定两级层次之间的语义映射问题。[^6]

Pangrova 当前固定 `catalog.schema.table`，但 Iceberg/Paimon REST catalog 可能拥有多层 namespace。目标模型必须使用：

```text
CatalogName + NamespacePath(List<String>) + ObjectName
```

SQL 前端仍可展示三段名称，但 connector profile 负责把 schema 映射到 namespace path。禁止通过字符串拼接和点号切分恢复结构。

Pangrova 也应显式支持两种注册模式：

- `TABLE_LINK`：只链接指定外部表；Pangrova DROP 默认只解除链接；
- `CATALOG_MOUNT`：挂载 catalog/namespace，外部 catalog 是权威源，Pangrova 代理 DDL 并维护本地 inventory projection。

这两种模式必须有不同的生命周期和权限语义，不能再用 `INTERNAL/CONNECTOR/FEDERATION` 三种 handle 类型隐式猜测。

### Snowflake 模式中不适合直接复制的部分

1. Snowflake 自己控制 native table 的文件和事务；Pangrova 对 Iceberg/Paimon 只是 engine，格式 catalog 才是表版本权威源。
2. Snowflake 的全局 metadata store 与 service fleet 是超大规模 SaaS 设计；Pangrova 当前不需要立即引入 FDB、多区域复制或独立 metadata server。
3. Snowflake 的 universal metadata 内部 schema 并未公开；只能借鉴公开的分层原则，不能假设其具体对象模型。
4. Snowflake 可以把 clone/share/time travel 实现为 native metadata-only 操作；Pangrova 只有在 connector capability 明确支持时才能暴露对应语义。
5. Snowflake Horizon Catalog 包含搜索、标签、质量、血缘和 AI 语义能力。Pangrova 可以提供统一发现入口，但业务语义 authority 应归 `pangrova-semantic`，治理/血缘归 `pangrova-governance`，检索归 Indexing/AI projection；它们都不是 query-critical catalog record。[^7]

## Iceberg、Paimon、Snowflake 与 Pangrova 的融合原则

| 原则 | Snowflake | Iceberg | Paimon | Pangrova 采用方式 |
|---|---|---|---|---|
| 中心控制面 | Cloud Services + transactional metadata | Catalog current metadata pointer | Catalog/snapshot publication | MetadataGateway + CatalogDirectory |
| 不可变版本 | table version/file set | metadata file + snapshot | schema/snapshot/manifest files | opaque version token + immutable projection |
| 并发控制 | SI/MVCC | optimistic requirements + atomic swap | OCC + atomic snapshot publish | provider commit + expected version + explicit outcome |
| 字段身份 | 内部稳定对象模型 | field ID | field ID | `FieldId` 必须保留 |
| 物理元数据 | micro-partition stats | manifest/file metrics | manifest/file/index metrics | connector-owned，统一读取 envelope |
| 查询绑定 | statement/transaction snapshot | scan snapshot/ref | scan snapshot/tag/consumer | per-table `TableBinding` |
| 历史 | Time Travel/Account Usage | snapshots/refs | snapshots/tags/consumer | connector history + derived metadata history |
| 多格式抽象 | universal metadata | Iceberg-specific | Paimon-specific | 小型通用核心 + typed extension |
| 安全 | Cloud Services/Horizon/RBAC | REST auth/credential vending | REST auth/capabilities | 授权先于解析，secret ref，短期凭据 |

## Pangrova 元数据中心的职责边界

### 必须属于元数据中心

1. Catalog 定义、稳定 ID、名称、provider/profile、generation、状态和 capability。
2. Namespace path、table/view/function 的稳定身份与名称绑定。
3. 逻辑 schema：field ID、名称、类型、nullability、comment 和 schema version。
4. Partition/spec、sort order、主键等可用于规划的 typed descriptor。
5. 表版本/ref/time-travel selector 的统一抽象。
6. Metadata resolution、授权后绑定、DDL 路由和 query table binding。
7. 版本化表级统计接口与不可变 cache。
8. Metadata domain event 与 transactional outbox。
9. 实时 `information_schema` 和 metadata capability/system tables。
10. 外部 catalog mount/link 的身份映射与 inventory 触发。

### 必须迁出元数据中心

| 当前内容 | 目标所有者 | 原因 |
|---|---|---|
| query/task state journal | `pangrova-coordinator` | 查询执行状态，不是对象 metadata |
| execution anchor、coordinator/worker registry、lease | `pangrova-coordinator` 或专用 coordination 包 | HA 与所有权状态 |
| write commit journal | coordinator/operations | 数据提交操作状态，只引用 TableId/VersionToken |
| index descriptor/file mapping/generation | `pangrova-indexing` | 派生物理加速结构 |
| artifact build job/page/output | `pangrova-indexing` | 后台构建工作流 |
| stream registration/offset/watermark | `pangrova-streaming` | 流处理恢复状态 |
| dynamic table refresh state/history | streaming/operations | 调度与物化状态 |
| materialized view refresh state | operations/indexing | 定义可在 catalog，refresh 状态不在 catalog |
| tenant membership/quota | security/resource | 身份与资源策略 |
| tag、quality rule/result、lineage | `pangrova-governance` | 治理 projection 与 policy |
| query audit/usage | observability/governance | 历史分析，不在实时 catalog |
| generic asset search index | governance/inventory | 跨域发现 read model |
| Semantic View、Concept、Metric、Relationship、Ontology、semantic binding | `pangrova-semantic` | 业务含义、关系和本体有独立版本/发布/校验生命周期；只引用 Metadata 稳定 ID/version |
| evaluation dataset/run/metric/release decision、GroundingSnapshot | `pangrova-ai-control` | 模型/语义检索与发布评测属于 AI 生命周期，不是 catalog 事实 |

### 可以共享，但不能共享所有权

- PostgreSQL instance/data source；
- `ObjectId`, `CatalogId`, `TableId`, `VersionToken` 等公共 value object；
- outbox relay 基础设施；
- migration runner 工具；
- tracing/metrics；
- secret provider SPI。

公共基础能力应位于 `common/spi` 或一个极薄的 state-store library，不得把业务 repository 重新集中到 metadata。

### 与 Semantic、Governance、AI 的硬边界

Metadata 只回答“对象是什么、位于哪里、当前/指定版本是什么”；Semantic 回答“对象在业务上是什么意思、如何组合与计算”；Governance 回答“谁负责、是否合规、如何流动、质量如何”；AI Control 回答“为当前用户意图选择哪些已发布且已授权的事实和工具”。依赖必须保持：

```text
AI / Governance / Materialization
  → Semantic API
    → Metadata API
```

Metadata Core 不得依赖 Semantic/Governance/AI。Semantic 通过 ObjectId/FieldId/VersionToken 绑定 Metadata，不能以 `catalog.schema.table.column` 字符串作为长期身份；Metadata schema 变化通过事件触发 Semantic compatibility analysis，不能同步反写或静默重绑。完整的 Concept/Metric/Graph/Ontology/AI 设计见 [long-term-modular-architecture.md](long-term-modular-architecture.md#metadata-之上的智能语义关系图谱与本体架构)。

## 目标模块结构

保留 `pangrova-metadata` 作为单一、纯粹的 catalog/metadata 中心；不建议第一阶段创建多个细碎 Maven module。内部包结构建议如下：

```text
pangrova-metadata
└── org.pangrova.metadata
    ├── api
    │   ├── MetadataGateway
    │   ├── CatalogDirectory
    │   ├── ObjectResolver
    │   ├── MetadataMutationService
    │   ├── QueryMetadataBinder
    │   ├── TableStatisticsService
    │   └── MetadataEventReader
    ├── domain
    │   ├── id
    │   ├── name
    │   ├── catalog
    │   ├── table
    │   ├── schema
    │   ├── version
    │   ├── capability
    │   └── statistics
    ├── catalog
    │   ├── CatalogDirectoryService
    │   ├── CatalogRuntimeRegistry
    │   └── CatalogAuthorization
    ├── resolution
    │   ├── DefaultObjectResolver
    │   ├── NamespaceMapper
    │   └── ProviderMetadataAccess
    ├── binding
    │   ├── DefaultQueryMetadataBinder
    │   ├── QueryMetadataSnapshot
    │   └── TableBinding
    ├── mutation
    │   ├── DefaultMetadataMutationService
    │   ├── MutationRequirement
    │   └── MutationResult
    ├── statistics
    │   ├── VersionedStatisticsService
    │   └── StatisticsEnvelope
    ├── cache
    │   ├── NameBindingCache
    │   ├── VersionedMetadataCache
    │   └── StatisticsCache
    ├── event
    │   ├── MetadataDomainEvent
    │   ├── MetadataOutbox
    │   └── MetadataEventRelay
    ├── store
    │   ├── CatalogStore
    │   ├── NameBindingStore
    │   ├── ProjectionStore
    │   └── jdbc
    └── system
        ├── InformationSchemaProvider
        └── MetadataCapabilitiesTable
```

模块依赖应收紧为：

```text
metadata -> spi, common
metadata -> security 仅通过注入的 SPI，最终移除实现依赖
metadata -X-> governance
metadata -X-> connector-iceberg / connector-paimon
metadata -X-> coordinator / streaming / indexing
```

格式 SDK 只能存在于各自 connector。当前 `pangrova-metadata/pom.xml` 中未使用的 Iceberg API/Core 依赖应在迁移早期删除。

## 统一 API 设计

不建立一个包含几十个方法的 `MetadataService`。`MetadataGateway` 只是依赖聚合入口：

```java
public record MetadataGateway(
        CatalogDirectory catalogs,
        ObjectResolver objects,
        QueryMetadataBinder binder,
        MetadataMutationService mutations,
        TableStatisticsService statistics) {}
```

核心接口建议控制在 7 个方法以内：

```java
interface CatalogDirectory {
    CatalogDefinition create(CreateCatalog command);
    CatalogDefinition update(UpdateCatalog command, CatalogVersion expected);
    void drop(CatalogId id, CatalogVersion expected);
    Optional<CatalogDefinition> get(CatalogId id);
    Optional<CatalogDefinition> findByName(TenantPrincipal principal, CatalogName name);
    Page<CatalogDefinition> list(TenantPrincipal principal, PageToken page);
}

interface ObjectResolver {
    ResolvedObject resolve(MetadataReadContext context, QualifiedObjectName name);
    Optional<ResolvedObject> get(MetadataReadContext context, ObjectId id);
    Page<ResolvedObject> list(MetadataReadContext context, NamespacePath path, PageToken page);
}

interface QueryMetadataBinder {
    QueryMetadataSnapshot bind(
            MetadataReadContext context,
            List<TableReference> references);
    void validate(QueryMetadataSnapshot snapshot);
}

interface MetadataMutationService {
    MutationResult createNamespace(MetadataWriteContext context, CreateNamespace command);
    MutationResult createTable(MetadataWriteContext context, CreateTable command);
    MutationResult alterTable(MetadataWriteContext context, AlterTable command);
    MutationResult renameTable(MetadataWriteContext context, RenameTable command);
    MutationResult dropTable(MetadataWriteContext context, DropTable command);
}
```

`CatalogRuntimeRegistry` 不再通过一个 `CatalogHandle` 同时暴露 metadata、split、page source 和 page sink。Connector SPI 分成：

```text
ConnectorRuntime
  ├─ ConnectorMetadataProvider
  ├─ ConnectorScanProvider
  ├─ ConnectorWriteProvider
  ├─ ConnectorMaintenanceProvider
  └─ ConnectorHealthProvider
```

元数据中心只能看到 `ConnectorMetadataProvider`。Coordinator 的 scan/write 路由只能看到对应执行 provider，并接收已经绑定版本的 opaque handle。这样既保持统一入口，又避免 metadata 模块成为执行层 service locator。

## 核心领域模型

### 稳定身份与名称分离

```text
CatalogId(UUID)
NamespaceId(UUID)
ObjectId(UUID)
TableId(ObjectId)
FieldId(int/string provider-stable id)

QualifiedObjectName(
  catalogName,
  namespacePath: List<String>,
  objectName
)
```

rename 只改变 name binding，不创建新对象 ID。drop 产生 tombstone；在 retention 窗口内重新创建同名对象必须获得新 ID。任何 index、lineage、policy、cache 和 query binding 都引用 ID，不以名称作为永久主键。

### Catalog 模型

```text
CatalogDefinition(
  catalogId,
  name,
  mountMode: TABLE_LINK | CATALOG_MOUNT,
  provider,
  protocolProfile,
  generation,
  state,
  namespaceMapping,
  nonSecretProperties,
  secretRef,
  tenantPolicyRef,
  capabilities,
  rowVersion,
  createdAt,
  updatedAt
)
```

`generation` 表示运行配置代际；URI、warehouse、认证、provider 或 namespace mapping 变化时递增。所有 runtime handle、cache 和 query binding 都携带 generation。旧 generation 可允许已开始查询完成，但不得接受新绑定。

### 表版本模型

```text
TableDescriptor(
  tableId,
  qualifiedName,
  objectKind,
  catalogId,
  catalogGeneration,
  versionToken,
  selectedRef,
  schema,
  partitioning,
  ordering,
  constraints,
  capabilities,
  providerExtension,
  observedAt
)
```

`MetadataVersionToken` 必须是 opaque value：允许相等比较、序列化和交还 provider，但通用层不假设可排序或连续。可以附带仅用于诊断的 snapshot ID、sequence、schema ID 等属性。

### Schema 模型

```text
SchemaDescriptor(
  schemaId,
  fields: List<FieldDescriptor>,
  highestFieldId,
  logicalConstraints
)

FieldDescriptor(
  fieldId,
  name,
  logicalType,
  nullable,
  documentation,
  defaultExpression,
  children
)
```

`logicalType` 必须支持 nested row/array/map、decimal precision/scale、timestamp precision/timezone、binary/vector/variant 等。禁止通过 `PangrovaType.name()` 序列化后再重建参数化类型。Iceberg/Paimon field ID 必须无损保留。

### Partition、排序与约束

```text
PartitioningDescriptor(specId, List<PartitionField>)
PartitionField(fieldId, sourceFieldId, transform, resultType)
PartitionTransform = identity | year | month | day | hour | bucket(n) | truncate(n) | providerOpaque

SortOrderDescriptor(orderId, List<SortField>)
TableConstraints(primaryKeyFieldIds, identifierFieldIds, checks)
```

Paimon 的 primary key、bucket mode、merge engine 和 changelog 不能被压缩为 properties 字符串；planner 需要的部分进入 typed capability，剩余部分保留 provider extension。

### 查询绑定模型

```text
MetadataReadContext(
  queryId,
  tenantPrincipal,
  sessionCatalog,
  requestedConsistency,
  statementStart,
  traceContext
)

TableBinding(
  tableId,
  qualifiedName,
  catalogId,
  catalogGeneration,
  versionToken,
  ref,
  schemaId,
  connectorId,
  opaqueTableHandle,
  opaqueScanToken,
  policyDecisionVersion,
  retentionLease
)

QueryMetadataSnapshot(
  queryId,
  boundAt,
  bindingsByReference,
  consistencyGuarantee
)
```

同一 SQL 可以两次引用同一表的不同 time-travel ref，因此 key 不能只有 `TableId`，而应是 `(TableId, requestedRef)`。所有 stats、split 和 worker plan 必须使用 binding 中的版本，不得再调用 current table resolution。

## 权威源矩阵

| 信息 | 权威源 | 元数据中心行为 |
|---|---|---|
| Pangrova catalog 定义 | CatalogStore | 强一致读写、generation/CAS |
| 外部 namespace/table 当前状态 | Iceberg/Paimon/REST catalog | 解析、版本缓存，不覆盖事实源 |
| 表 schema/snapshot/manifest | 格式原生 metadata | 转为无损 descriptor/opaque token |
| 查询 table binding | Coordinator query state | MetadataBinder 生成，Coordinator 持有/恢复 |
| DDL operation 状态 | operations/coordinator | metadata center 路由并返回结果，不持久化执行 journal |
| data commit WAL | coordinator/operations | 仅消费成功后的 version event |
| table-level optimizer stats | connector version | versioned cache/projection |
| file/manifest stats | connector | 按绑定版本规划，不做全量 mirror |
| Pangrova index | indexing | 通过 TableId + source VersionToken 关联 |
| tag/quality/lineage | governance | 通过 ObjectId 关联，授权接口注入 |
| query history/usage | observability | 历史 projection，不参与实时解析 |
| inventory/search | inventory/governance | 最终一致投影，公开 lag |

## 持久化设计

### Metadata core 数据表

建议 V2 schema 只包含：

| 表 | 主要字段 | 说明 |
|---|---|---|
| `metadata_catalog` | catalog_id、name、provider、mode、generation、state、row_version、secret_ref、properties、timestamps | Pangrova catalog 事实源 |
| `metadata_namespace_binding` | namespace_id、catalog_id、external_path、local_path、generation、state | 多层 namespace 映射 |
| `metadata_object_identity` | object_id、catalog_id、kind、created_generation、dropped_at | 稳定身份/tombstone |
| `metadata_name_binding` | catalog_id、namespace_path、name、object_id、valid_from/to、binding_version | rename/drop/recreate 安全 |
| `metadata_projection` | object_id、provider_version、projection_schema_version、payload、checksum、observed_at | 不可变加速投影，非权威 |
| `metadata_outbox` | event_id、aggregate_id、aggregate_version、event_type、payload_version、payload、created_at、published_at | 可靠领域事件 |
| `metadata_event_delivery` | consumer_id、event_id、state、attempts、lease_until、last_error | 无消息总线时的可靠 fan-out |

约束：

- 全部时间使用 UTC `TIMESTAMPTZ`；
- catalog name、有效 name binding 和 operation idempotency 分别设置唯一约束；
- `row_version/generation/aggregate_version` 使用 CAS；
- JSONB payload 带 schema version 和 checksum；
- `event_id` 仅用于身份和去重，不把 sequence 分配顺序当作事务 commit 顺序；
- 同一 aggregate 的事件按 `aggregate_version` 检查 gap；
- relay 使用 `published_at IS NULL` + lease/`SKIP LOCKED`，consumer 使用 inbox/delivery 去重；
- Flyway 是唯一 DDL owner，repository 构造函数不得建表。

### PostgreSQL 继续作为第一阶段存储

推荐短期继续使用 PostgreSQL：

- Pangrova 当前技术栈、运维和 migration 已围绕 JDBC/Flyway；
- catalog 定义和 name binding 属于典型 OLTP 小事务；
- outbox、CAS、JSONB 和行锁足以实现第一阶段语义；
- 存储抽象应围绕领域 repository，不围绕通用 KV，以免提前设计最小公分母。

只有满足以下任一条件时才评估 FDB/分布式 KV：

- 实测 catalog write contention 无法通过分片/索引解决；
- 需要跨区域低延迟强一致 active-active；
- metadata key/range read 吞吐超过 PostgreSQL 水平扩展能力；
- 在线多版本服务对 schema 演进提出 KV object mapping 的明确需求。

迁移存储实现不能改变 API 的 object/version/CAS 语义。

## 缓存设计

### 三种缓存，不再层层复制 current table

1. `CatalogDefinitionCache`
   - key：`CatalogId + generation`；
   - value：不可变配置和 capability；
   - generation 变化后旧 entry 只服务已绑定查询。
2. `NameBindingCache`
   - key：`principalScope + catalogId + generation + namespacePath + name`；
   - value：`ObjectId + pointer version`；
   - 短 TTL，negative entry 更短；授权必须在返回前验证。
3. `VersionedMetadataCache`
   - key：`catalogId + generation + objectId + ref + versionToken`；
   - value：不可变 `TableDescriptor`；
   - 可长 TTL、按重量限制、single-flight load。

统计单独以 `(TableId, VersionToken, StatisticsKind)` 缓存。Calcite schema 不再持有另一个 30 秒“当前表事实源”，只保存 generation-aware resolver/绑定对象。

### 正确性规则

- cache hit 必须精确命中版本，不能把 stale current pointer 当作 query binding；
- 失效通知只缩短陈旧时间，读 current pointer 时仍使用 version/ETag 复验；
- 上游故障时只允许复用已绑定的不可变版本；实时 name resolution 和授权失败必须 fail closed；
- snapshot 已被格式侧过期时返回明确错误，不得静默切换 current snapshot；
- credential 不进入长期 cache 或序列化 handle，其 TTL 不得超过临时凭据生命周期。

## Metadata mutation 与事件

### DDL

DDL 流程：

```text
SQL DDL
 -> MetadataMutationService
 -> Authorization + capability validation
 -> resolve stable object/base version
 -> ConnectorMetadataProvider.apply(requirements, changes, operationId)
 -> provider atomic commit
 -> load committed version
 -> update local name/projection transaction
 -> append MetadataDomainEvent in same transaction
 -> return committed VersionToken
```

外部 catalog 是权威源时，本地事务失败不能撤销已经成功的 provider commit。此时 mutation 返回“provider committed, local projection pending”，outbox/reconciler 按 operationId 修复；不能把请求重新提交到 provider。

### 数据写入

data commit WAL、execution fencing 和 UNKNOWN 状态迁出 metadata，归 coordinator/operations。成功提交后发布：

```text
TableVersionCommitted(
  operationId,
  tableId,
  previousVersion,
  committedVersion,
  changeSummary,
  committedAt
)
```

元数据中心消费该事件刷新 pointer/统计 projection；indexing、streaming、governance 也独立消费。一个消费者失败不能阻塞其它消费者，也不能使已提交数据回滚。

### Catalog mount 同步

用 `CatalogInventoryService` 替代当前 timestamp-based `MetadataSyncService`：

- 变更源优先使用 REST/version/opaque cursor；
- apply 变更和保存 cursor 同事务；
- 任一对象失败则本批 cursor 不前移；
- full reconcile 使用分页 enumeration epoch 和 mark-and-sweep；
- rename 按 stable external ID 识别，否则明确降级 delete+add；
- `TABLE_LINK` 与 `CATALOG_MOUNT` 使用不同生命周期策略；
- inventory 只服务发现和历史，不成为 query fallback。

## 安全模型

统一入口的调用顺序固定为：

```text
authenticate principal
 -> authorize catalog visibility
 -> resolve name to stable ObjectId
 -> authorize object/version/action
 -> bind provider metadata
 -> vend scoped data credential if needed
 -> cache authorized result or unfiltered immutable descriptor
```

要求：

- 当前 `allowedTenants` 替换为 `tenantPolicyRef/policyVersion`，由 security SPI 判定；
- `ConnectorSession` 接收可信 `QueryIdentity`，不能用任意 properties map 模拟安全上下文；
- catalog properties 只能保存 non-secret 配置，认证信息保存为 secret reference；
- 支持 REST Catalog credential vending 时使用 table/path scoped 短期凭据；
- storage location overlap 必须校验，避免一张表的临时凭据覆盖另一张表目录；Snowflake Open Catalog 官方也把目录重叠列为 credential vending 风险。[^8]

## 实时与历史系统视图

建议提供两组 SQL 表：

### `system.information_schema`

- catalogs、namespaces、tables、columns、views、functions；
- table_versions、table_capabilities、partitioning、sort_orders；
- metadata_cache_status、catalog_health；
- 数据直接来自 MetadataGateway/当前绑定，公开 provider version 和 observed time。

### `system.metadata_history`

- object_history、name_binding_history、schema_history；
- catalog_events、metadata_sync_history、dropped_objects；
- inventory_lag、consumer_delivery_failures；
- 来自历史 projection，明确数据延迟和保留期。

query/task history、worker health、index build、stream offset 不应伪装成 metadata history；分别由对应 system schema 暴露。

## 当前包迁移清单

| 当前包 | 处理 | 目标 |
|---|---|---|
| `adapter` | 删除平行 Federation abstraction；按 provider/mount mode 重写 | metadata `resolution` + connector SPI |
| `artifact` | 整包迁出 | `pangrova-indexing` |
| `cache` | 仅保留三个版本化 metadata cache；移除 JDBC 时间戳 poller | metadata `cache`；可靠事件在 `event` |
| `catalog` | 保留但重写身份、generation、runtime/provider 边界 | metadata `catalog/domain` |
| `checkpoint` | 迁出 | streaming/coordinator |
| `coordination` | 整包迁出 | coordinator coordination |
| `dependency` | 对象 ID 类型保留；依赖图服务迁出 | governance/lineage |
| `dynamictable` | 定义引用 catalog；refresh 状态迁出 | operations/streaming |
| `event` | 替换进程内 dispatcher 为 domain event + outbox | metadata `event` |
| `exception` | 只保留 catalog/object/version/mutation 异常 | metadata `api` |
| `governance` | 迁出 | `pangrova-governance` |
| `index` | 整包迁出 | `pangrova-indexing` |
| `model` | 删除扁平 TableMetadata/Asset 并建立 ID/version descriptor | metadata `domain` |
| `mv` | view 定义归 catalog；refresh/build 状态迁出 | metadata + operations/indexing |
| `schema` | 保留通用 schema model；格式演进验证由 provider capability 完成 | metadata `domain/mutation` |
| `serde` | 删除有损 TableMetadata serializer；只保留 versioned projection codec | metadata `store` |
| `service` | 拆成小接口和 gateway；删除生命周期/治理混合方法 | metadata `api/binding/mutation` |
| `store` | 只保留 catalog/name/projection/outbox store | metadata `store` |
| `streaming` | 整包迁出 | `pangrova-streaming` |
| `sync` | 改为 inventory/mount reconcile，不参与查询 fallback | governance/inventory + metadata mount trigger |
| `tag` | 整包迁出 | `pangrova-governance` |
| `tenant` | 整包迁出 | security/resource |

## 实施顺序

### 阶段 0：架构冻结与行为基线（1–2 周）

- 接受 ADR：权威源矩阵、per-table repeatable read、模块纯度边界；
- 禁止向 metadata 的 artifact/index/coordination/streaming 等旧包新增功能；
- 建立架构测试：metadata 不依赖格式 SDK；新代码不得直接暴露 `CatalogHandle.getMetadata`；
- 为现有 resolve、DDL、split、stats、commit、sync 建立 characterization tests；
- 给所有现有 metadata 表和 Java 类标注目标 owner。

### 阶段 1：新领域模型与 Gateway（2–5 周）

- 新建 ID、NamespacePath、VersionToken、TableDescriptor、TableBinding；
- 引入小型 metadata provider SPI 和 `MetadataGateway`；
- 用 adapter 包装现有 Iceberg/Paimon 实现，但新模型不得依赖旧 `TableMetadata`；
- 修复 createTable typed schema 和 Paimon RowType 有损序列化；
- 移除 metadata POM 中 Iceberg 依赖。

### 阶段 2：查询版本绑定（4–8 周）

- SQL analysis 后集中调用 QueryMetadataBinder；
- stats、split、worker plan 改为消费 TableBinding；
- queryId、principal、catalog generation 通过 typed context 传播；
- Iceberg/Paimon snapshot key 改为 `(query, table, ref)`；
- 增加 retention lease/明确 expiration error；
- 主路径不再使用 `StatementConnectorSession(system, Map.of())`。

### 阶段 3：Catalog Store V2 与事件（6–10 周）

- 新建 V2 表，不修改已发布 V1 checksum；
- backfill catalog/object identity/name binding；
- 使用一次性校验后切换权威读写路径，不建立长期双写或 fallback；
- transactional outbox + delivery/inbox；
- Catalog create/update/drop 使用 CAS、generation、tombstone；
- 停用运行时 `CREATE TABLE IF NOT EXISTS`。

### 阶段 4：领域迁出（8–16 周）

- 依次迁出 coordination、streaming、index/artifact、tenant、governance；
- 每迁出一个领域，即迁移其 Flyway location 和 system tables；
- Coordinator 作为 composition root 注入各领域服务；
- 删除旧 repository、旧 migration ownership 和跨域 imports；
- 将 `pangrova-metadata` 收敛到 catalog/binding/mutation/cache/store/system。

### 阶段 5：外部 Catalog 与历史库存（12–24 周）

- 实现 TABLE_LINK 与 CATALOG_MOUNT；
- 优先兼容 Iceberg/Paimon 官方 REST Catalog profile；
- 构建 inventory/history pipeline，公开 lag；
- 支持 nested namespace mapping、credential vending 和 capability negotiation；
- 只有在 provider 明确支持时暴露 multi-table transaction/ref/clone。

## 验收门槛

### 结构门槛

- `pangrova-metadata` 只包含约定的 10 个领域包；
- 无 Iceberg/Paimon SDK 依赖或 import；
- 无 query/task/worker/index/artifact/stream offset/tenant/quality repository；
- `MetadataService` God interface 被删除；所有 public interface 不超过仓库规定的方法数；
- Coordinator 元数据语义调用全部经过 MetadataGateway；执行 provider 不允许重新按名称解析 current table。

### 一致性门槛

- 同一 query/table/ref 的 schema、stats、split、scan 使用同一 VersionToken；
- 多表查询不共享 snapshot ID；同表不同 time-travel ref 可并存；
- snapshot 过期不回退 current；
- catalog generation 更新后新查询只使用新 generation，旧查询可受控完成；
- rename/drop/recreate 不复用 ObjectId；
- provider commit 成功、本地 projection 失败可按 operationId 收敛且不重复提交。

### 故障门槛

- outbox 事件在事务乱序、进程 crash、重复 relay 下不丢失；
- inventory batch 中一个对象失败不前移 cursor；
- PostgreSQL/connector 暂时不可用时不绕过授权或读取 mutable stale current；
- catalog register/update/drop 的 stale generation 被 CAS 拒绝；
- 两版本 Coordinator 并行滚动升级可以同时读取 metadata payload，写入只产生最新 schema version。

### 建议 SLO

以下是初始工程目标，需按真实规模校准：

| 指标 | 建议目标 |
|---|---|
| hot name resolution p99 | ≤ 10 ms |
| hot versioned descriptor p99 | ≤ 5 ms |
| 本地 DDL read-after-write | 同步可见 |
| 跨 Coordinator catalog 变更可见 | p99 ≤ 5 s |
| query binding version mismatch | 0 |
| metadata event 丢失 | 0 |
| inventory lag | 可观测并按 catalog 配置 SLO |
| UNKNOWN metadata mutation | 必须告警、可按 operationId 收敛 |

## 立即执行的十二项工作

1. 通过 ADR 确认“一个 Gateway、多个领域 owner”的中心定义。
2. 冻结 metadata 旧包扩张，建立 package allowlist。
3. 新建 `CatalogId/ObjectId/FieldId/MetadataVersionToken/NamespacePath`。
4. 新建 `TableDescriptor/TableBinding/QueryMetadataSnapshot`。
5. 拆出 `ConnectorMetadataProvider`，禁止 MetadataCenter 暴露 scan/page source/sink。
6. 把 queryId/principal/catalog generation 从 Coordinator 传到 split/statistics。
7. 修复 Iceberg/Paimon per-table snapshot binding 与 retention。
8. 修复 createTable typed columns 和 Paimon 完整 schema 表示。
9. 建立 Catalog Store V2：generation、CAS、tombstone、secret ref。
10. 建立 transactional outbox，替换 JDBC timestamp invalidation poller。
11. 将 coordination 与 index/artifact 作为第一批迁出样板。
12. 建立 `information_schema` 与历史 inventory 的明确一致性边界。

## 最终判断

最适合 Pangrova 的元数据中心不是 Snowflake 的缩小复刻，也不是 Iceberg/Paimon catalog 的二次实现。它应是一个**版本化 catalog control plane 和 query metadata binding service**：

- 对上，为 Planner/Coordinator 提供唯一、授权、不可变的对象视图；
- 对下，通过 Connector metadata provider 使用格式原生事实源；
- 对旁，通过稳定 ObjectId/VersionToken 和可靠事件连接 indexing、streaming、governance、security、observability；
- 对存储，只拥有 catalog、名称绑定、加速 projection 和事件，不拥有所有控制面状态；
- 对查询，保证每表 repeatable read，并且不承诺底层 catalog 无法提供的全局事务。

如果严格按上述边界实施，`pangrova-metadata` 会从当前的“持久化代码收容所”变成真正的元数据中心：更小、更强语义、更容易扩展新格式，也更接近 Snowflake 的控制面纪律与 Iceberg/Paimon 的开放湖仓事实模型。

## Sources

[^1]: Benoit Dageville et al., Snowflake, [“The Snowflake Elastic Data Warehouse,” SIGMOD 2016](https://www.snowflake.com/wp-content/uploads/2019/06/Snowflake_SIGMOD.pdf). 原始架构论文；用于三层架构、统一查询入口、事务元数据、Snapshot Isolation、MVCC、pruning 和在线升级事实。
[^2]: Ashish Motivala, Snowflake, [“How FoundationDB Powers Snowflake Metadata Forward,” 2018](https://www.snowflake.com/en/blog/how-foundationdb-powers-snowflake-metadata-forward/). 官方工程文章；用于 metadata OLTP 模式、ACID KV、watch、object mapping、schema version 与 hard-state 事实。
[^3]: Hossein Ahmadi et al., Snowflake, [“Snowflake’s Data Architecture: Enabling AI Apps, Next-Gen Lakehouse Analytics And More,” 2025](https://www.snowflake.com/en/blog/engineering/snowflake-s-data-architecture--enabling-ai-apps--next-gen-lakeho/). 官方工程文章；用于逻辑/物理类型解耦和 universal metadata 事实。
[^4]: Snowflake Documentation, [“Micro-partitions & Data Clustering”](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions). 官方文档；用于 micro-partition 统计、自动维护和 pruning 事实。
[^5]: Snowflake Documentation, [“Account Usage”](https://docs.snowflake.com/en/sql-reference/account-usage). 官方文档；用于实时 Information Schema 与延迟历史视图、对象 ID 和 dropped object 事实。
[^6]: Snowflake Engineering, [“Bringing the Snowflake Platform to Data Lakes,” 2025](https://www.snowflake.com/en/blog/engineering/snowflake-platform-data-lake/). 官方工程文章；用于 per-table link、catalog-linked database、namespace mapping 和 metadata overlay 事实。
[^7]: Snowflake Documentation, [“Snowflake Horizon Catalog”](https://docs.snowflake.com/en/user-guide/snowflake-horizon). 官方文档；用于跨引擎 catalog、治理、发现和策略层事实。
[^8]: Snowflake Documentation, [“Snowflake Open Catalog Overview”](https://docs.snowflake.com/en/user-guide/opencatalog/overview). 官方文档；用于 Iceberg REST catalog、current metadata pointer、原子更新、nested namespace 和 credential vending 风险事实。
[^9]: Apache Iceberg, [“Table Specification”](https://iceberg.apache.org/spec/). 官方规范；用于 metadata file、snapshot、schema/spec/order ID 和 OCC 事实。
[^10]: Apache Iceberg, [“REST Catalog Specification”](https://iceberg.apache.org/rest-catalog-spec/). 官方协议；用于 change-based commit、capability、multi-table commit 和 credential vending 事实。
[^11]: Apache Paimon, [“Snapshot Specification”](https://paimon.apache.org/docs/master/concepts/spec/snapshot/). 官方规范；用于 snapshot、manifest、schema ID 和 commit identity 事实。
[^12]: Apache Paimon, [“Concurrency Control”](https://paimon.apache.org/docs/master/concepts/concurrency-control/). 官方文档；用于 OCC、冲突验证和原子发布事实。
