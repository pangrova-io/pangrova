---
title: "模块重构处置台账"
description: "维护已核查的生产调用、类归属、事务与资源边界"
---

# 模块重构处置台账

本表记录已沿生产入口追踪的类。模块 manifest 记录整个 reactor 的依赖和包归属，无法证明某个接口已被装配或某条路径支持恢复。尚未逐类核查的模块继续列为待审。

按 2026-10-24 的 `b51114d0`（首次拆分 Coordination 权威之前）和当前工作树对照，可导出每个 Java 生产顶层及成员类型的原路径、目标路径、制品和 FQN。命令会纳入未提交的新源码。重名匹配仅作为审查线索，拆分、合并、运行时调用及测试证据由下表记录。JDK 17 解析实际声明，同文件中的附属类、成员枚举和记录各有独立条目。局部类与匿名实现随包含它们的方法审查。

```bash
python3 .pangrova/scripts/export-class-inventory.py --base b51114d0 --output target/architecture/class-dispositions.tsv
```

## 物化

`MaterializationReadPin` 使用 Execution API 的 `QueryId`，`MaterializationAttemptExecutor` 使用本领域的 `AttemptId`。持久化端口只接受带 `QUERY` 或 `REFRESH_ATTEMPT` 类型的 `ReaderId`。V68 空库初始化以 `(generation_id, reader_kind, reader_id)` 为主键，按 `(reader_kind, reader_id)` 索引续租与释放。到期清理和 GC 仍检查所有 reader 的有效期。PostgreSQL 对相同 UUID 的双行执行续租、定向释放、批量释放和到期清理，GC 用例验证存活的 refresh pin 可以独立保护已退役 generation。本批独立检出完成 143 项 reactor 编译，6 类 41 项测试通过；另以匹配发布基线 WriteDescriptor 的 macOS arm64 classifier 运行真实 Coordinator、Worker 和 Iceberg，DynamicTableMaterializationE2ETest 的 10 项全部通过。

表中的类名以 `org.pangrova.` 为共同前缀，制品名以 `pangrova-` 为共同前缀。

| 原类 | 处置及实际目标 | 生产调用、事实与资源 | 验证 |
|---|---|---|---|
| `materialization.port.MaterializationRepository` | 移至 `persistence-materialization-api` 的 `persistence.materialization.api.lifecycle.MaterializationRepository` | SQL handler 经 `MaterializationKernelFactory` 调用命令内核。Server 创建 `JdbcMaterializationRepository` 并借用 Metadata 数据源。保留登记、候选代际、发布、outbox、pin 的同事务操作 | `JdbcMaterializationRepositoryTest`、动态表和物化视图 SQL E2E |
| `materialization.port.MaterializationCatalog` | 移至 `persistence-materialization-api` 的 `persistence.materialization.api.publication.MaterializationCatalog` | `PlannerWiring` 与语句内核借用 `CoordinatorBootstrap` 创建的同一个读取端口；已发布头和 registry epoch 源自仓储 | `CoordinatorBootstrapTest`、`MaterializationReadPinIntegrationTest` |
| `materialization.port.EventOutbox` | 删除孤立写接口 | 无生产实现或调用者。发布和状态更新已在仓储方法中携带事件，事务提交由 PostgreSQL 实现持有 | 仓储测试、物化 SQL E2E |
| `materialization.extension.MaterializationExtensionRegistry` | 移至 `materialization-core` 的 `materialization.core.extension.MaterializationExtensionRegistry` | Server 创建注册表，命令内核借用同一实例选择策略。注册表持有不可变的扩展集合，无独立持久化或关闭动作 | `MaterializationExtensionRegistryTest`、`MaterializationArchitectureTest` |
| `coordinator.materialization.MaterializationExtensions`、`server.coordinator.assembly.materialization.MaterializationExtensions` | 删除硬编码装配。Server 的 `MaterializationDdlRegistration` 调用 `MaterializationExtensionRegistry.discover`，从 builtin 的服务描述符加载三个 `RefreshStrategyFactory` | 每个已声明 kind 只创建一个共享纯策略；DDL 创建首代需要 FULL，自动增量优先 PARTITION_REPLACE，随后 APPEND_ONLY。显式指定仍先匹配定义能力。`RefreshStrategyDiscoveryTest`、`MaterializationServiceIncrementalTest`、`PartitionReplaceRefreshStrategyTest` |
| `coordinator.materialization.MaterializationDdlRegistration` | 移至 `server-coordinator` 的 `server.coordinator.assembly.materialization.MaterializationDdlRegistration` | `QueryExecutionBootstrap` 向语句注册表安装 handler，并注入仓储、读投影及策略。命令内核只借用这些对象 | `DynamicTableMaterializationE2ETest`、`MaterializedViewMaterializationE2ETest` |
| `coordinator.materialization.MaterializationKernelFactory` | 保留在 `coordination-core` 并改为注入持久化端口与扩展注册表 | 每条命令绑定其会话、权限、查询 fence 和时钟。仍有基于数据源的原生提交验证依赖，待抽取对应 workflow | `CoordinatorBootstrapTest`、物化读 pin 和 SQL E2E |

## Direct Shuffle 与索引构建

| 原类 | 处置及实际目标 | 生产调用、事实与资源 | 验证 |
|---|---|---|---|
| `worker.shuffle.NativeShuffleTransport` → `NativeShuffleProducerRegistration` | 由平铺后的 `worker` 发布，包为 `worker.shuffle` | `WorkerTaskBootstrap` 注入受保护的 JNI 命令通道，`TaskLifecycleManager` 注册每个 executor 的 producer，`TaskResourceReleaser` 注销该注册。C++ FlightServerRegistry 持有进程共享服务。数据流保留在 C++；Native executor handle 的释放仍由 Worker 管理 | `NativeShuffleProducerRegistrationIsolationTest`、使用实际 Native 库的 `NativeShuffleProducerRegistrationTest` |
| `coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager`、`ShuffleProtocolAdapter` | 迁入 `shuffle-pangrova` 的 `shuffle.pangrova.client.coordinator.PangrovaShuffleCoordinator` 和包私有的 `PangrovaShuffleControlProtocol` | Server 以配置的 ID 发现 ShuffleCoordinatorFactory，向 DAG 注入对应会话。Provider 独占注册、读写 grant、finalize 校验、清理、gRPC 通道与关闭。DAG 仅使用领域契约 | `PangrovaShuffleCoordinatorTest`、`ShuffleLifecycleOperationsTest`、`NativeExchangeTransportExternalIntegrationTest` |
| `coordinator.query.execution.dispatch.shuffle.generation.ShuffleLifecycleEvidence` | 移入 `shuffle-api` 的 `shuffle.api.coordinator`，增加 `RecoverableShuffleControl` 与 `ShuffleTaskAccess` | Coordinator 提供 Generation 持久化 journal。Provider 确认后传入不可变回执和原始无凭据响应；数据库仍核对 owner、allocation、generation 和 producer 集合 | `ShuffleContractIsolationTest`、`GenerationExchangeEvidenceTest`、`PangrovaShuffleCoordinatorTest` |
| Worker 直接创建 RemoteShuffleClientPool 和 RemoteShuffleByteEgress | `shuffle-native` 的 NativeShuffleWorkerFactory、NativeShuffleReaders，`shuffle-pangrova` 的 PangrovaShuffleWorkerFactory | Server Worker 安装 Provider。WorkerTaskBootstrap 选择工厂，核验会话能力。读取和写出借用同一代的 channel 配额。WorkerTaskOwnership 在部分启动失败时关闭已获取的资源，成功后将关闭权移交给 NativeExchangeTransport、NativeTaskExecutor 和 TaskManager | WorkerTaskOwnershipTest、NativeShuffleEgressContractIntegrationTest、RemoteShuffleChannelAdmissionTest |
| 注册请求中的 shuffle_mode | 新增 shuffle_provider、Coordination API 的 WorkerShuffleDeployment 和 schema 初始化 V113 | Worker 在认证注册时公开实际安装的部署。WorkerManager 核对注册、重注册和故障接管。Memory 与 PostgreSQL 将该部署绑定到进程 incarnation，恢复后只有匹配的进程可参与调度 | WorkerResourceVersionCheckTest、WorkerManagerRegistryTest、JdbcWorkerRegistryPostgresTest |
| `shuffle.pangrova.client.RemoteShuffleClientException` 和原 Worker 后端异常分类 | Pangrova 的 `client.exception.RemoteShuffleClientException`、统一 API 的 `exception.ShuffleReadException`、`nativewire.exception.ShuffleChannelAdmissionException` | Worker 将数据损坏和通道配额拒绝映射为执行错误。它只需要 API 和 Native wire 类型。Server 根据角色安装后端；错误本身没有持久状态和关闭操作 | NativeExchangeTransportExternalIntegrationTest、RemoteShuffleChannelAdmissionTest、TaskCompletionMonitor |
| `indexing.build.IndexBuildCoordinator` | 保留在 `indexing-core` | `IndexDdlExecutor` 和增量刷新路径提交构建。每次提交创建单独 attempt 身份，它同时标识数据库 claim 和物理对象目录。借用的 executor 由创建者关闭 | `IndexBuildClaimLifecycleTest` 的并发接管与迟到写入测试 |
| `indexing.build.IndexFileBuilder` | 保留在 `indexing-core` | Coordinator 传入 attempt；builder 借用列读取器和存储访问器，计算校验和，写入返回映射所指向的确切路径 | `IndexBuildClaimLifecycleTest` |
| `indexing.storage.IndexStorageLayout` | 保留在 `indexing-core` | `IndexFileBuilder` 按代际、attempt UUID 和源文件标识生成路径。读取和已登记对象的清理使用保存的映射 | `IndexStorageLayoutTest`、`IndexBuildClaimLifecycleTest` |
| `indexing.lifecycle.IndexLifecycleListener` | 删除未装配的类 | 原实现从逻辑数据文件名推算要删除的对象。生产 `IndexGarbageCollector` 从仓储读取已登记的物理路径 | 删除前检查调用与装配；实际 attempt 隔离由并发测试覆盖 |

待核查的边界包括索引对象写入后、映射提交前崩溃时的回收，以及已经写入部分映射的代际如何恢复。Direct Shuffle 的启动和停止测试覆盖 Native 控制，不构成 Local、自研远端或 Celeborn 的发布回执、重试与恢复证据。

## Shuffle 持久化分配证据

| 旧类与职责 | 当前类与制品 | 入口、状态与验证 |
|---|---|---|
| `ShuffleLifecycleEvidence.Allocation` 的单一地址、`registered/finalized` 的带凭据 handle | `shuffle-api` 的 `ShuffleProviderDescriptor` 与 journal Allocation | Journal 只接收分配 ID、Provider ID、版本和最多 1 MiB 的私有字节，以及发布时的回执与无凭据响应。 |
| `GenerationExchangeRegistration.Address/Endpoint` | Generation API 保留逻辑 exchange、stage、epoch、cohort、原始 owner 和 descriptor。PangrovaAllocationCodec 独占远端路由编解码。 | GenerationExchangeEvidenceCodec 的注册 golden 与引用其 SHA-256 的发布 golden 同步更新。PostgresGenerationStageExchangeStore 在原权限与 owner 事务中拒绝冲突的 Provider、版本或字节。 |
| `DurableStageShuffleEvidence` 的地址转换、`StageExchangeAccessValidator` 的 Generation 路由比较 | Coordinator journal 恢复前核对完整 cohort。外部任务传输通过会话的 `ExternalShuffleDescriptorEncoder` 在分区 claim 事务中比较实际读写绑定与原分配。 | 动态权限由实际服务重新授予。已缓存的分配或发布回执也需核对本进程当前 owner。真实 PostgreSQL 接管测试验证旧 owner 被拒、错代被拒及 first owner 不变。 |

本批独立 JDK 17 编译及首轮 87 个测试通过。最终针对已调整的 gRPC 恢复、失主缓存、编解码与 golden 复验 45/45，贯穿真实 journal、producer 和 PostgreSQL 接管的套件复验 8/8。Direct、Local 与未来 Celeborn 的持久化回执需要各自生产实现及对应验证。

## Metadata Federation

| 原类 | 处置及实际目标 | 调用、状态与资源 | 验证 |
|---|---|---|---|
| `metadata.federation.FederatedCatalogFactory` | 移至 `metadata-api` 的 `metadata.api.catalog.FederatedCatalogFactory` | 外部工厂按此服务名注册。每次创建返回归对应 Catalog handle 关闭的实例 | `ServiceLoaderReachabilityTest`、`FederationProviderDiscoveryTest` |
| `coordinator.metadata.PluginFederationHandleFactory` | 移至 `metadata-core` 的 `metadata.core.catalog.federation.PluginFederationHandleFactory` | Server 构造后注入 Catalog 注册表；启动时发现工厂、拒绝重复或空 ID，每次建 Catalog 时解析配置里的密钥引用 | `PluginFederationHandleFactoryTest`、`FederationProviderDiscoveryTest` |
| `metadata.federation.FederationCatalogHandle` | 移至 Metadata Core 的 `metadata.core.catalog.federation.FederationCatalogHandle` | 注册表负责部署、退休和关闭；周期性同步通过 CatalogHandle.federationSource() 借用外部 Catalog；外部事实仍归 Provider | `MetadataBorrowedOwnershipIntegrationTest` |
| `metadata.federation.FederationConnectorMetadata` | 移至 Metadata Core 的 `metadata.core.catalog.federation.FederationConnectorMetadata` | 将命名空间、表、列和统计读取转交外部 Catalog。缓存装饰器的测试使用显式注入的实例，生产 handle 直接使用 Provider 返回的实例 | `MetadataAbstractionLayerAuditTest`、`CachingFederatedCatalogTest` |
| `metadata.federation.FederationTableHandle`、`metadata.federation.FederationColumnHandle` | 各自移至 Metadata Core 的 `metadata.core.catalog.federation` 包，类名保持 | 仅承载外部 Catalog 的表及列身份，无连接或文件所有权 | `CatalogHandleAndStatusProtocolConsistencyTest`、`CachingFederatedCatalogTest` |

仓库当前未随包提供 Federation 后端工厂。实际发行的 Iceberg、Paimon、Lance 使用 Connector 工厂；Federation 的测试工厂只验证发现、拒绝行为和适配器边界。

## Worker 进程与 Standalone

| 原类 | 处置及实际目标 | 调用、状态与资源 | 验证 |
|---|---|---|---|
| `worker.bootstrap.PangrovaWorkerMain` | 入口移至 `server-worker` 的 `server.worker.bootstrap.PangrovaWorkerMain`；配置转换抽至同制品 `server.worker.bootstrap.WorkerConfiguration` | Worker 启动脚本加载角色 JAR，入口校验配置、安装 Catalog runtime 和关闭钩子；WorkerBootstrap 持有执行资源 | `PangrovaWorkerMainTest`、`WorkerPortConfigurationTest`、脚本 classpath 测试 |
| `worker.embedding.EmbeddedWorker` | 移至 `server-worker` 的 `server.worker.embedding.EmbeddedWorker` | Standalone 传入实际 Coordinator HTTP 端口。配置使用同一转换器，安装生产 Catalog runtime；Standalone 先关闭 Worker，再关闭 Coordinator | `EmbeddedWorkerTest` |

Worker Core 仍包含执行组件的分阶段装配。macOS arm64 上的 `PhysicalTwoWorkerShuffleE2ETest` 通过两个 Worker 的 join 和全局 Top-K，第二个 Worker 运行在独立 JVM。Standalone 完整启动、独立进程故障恢复和 Linux 容器角色发行另行验证。

## Server Bootstrap 与索引映射事务

| 原类或方法 | 处置及实际目标 | 生产调用、事实与资源 | 验证 |
|---|---|---|---|
| 三个角色 Main 的 `loadPangrovaConfiguration` 与 `parseConfigPath` | 合并至 `server-bootstrap` 的 `server.bootstrap.ServerConfigurationLoader` | Coordinator、Worker、Standalone 在构造服务前传入各自的默认配置资源，日志脱敏保留原始文件路径语义 | `ServerConfigurationLoaderTest`、两个独立 Main 的配置与验证测试 |
| `server.coordinator.bootstrap.CoordinatorResourceRelease` | 合并到 `common.lifecycle.ResourceCloser.closeAll` | Coordinator、Standalone 与 Streaming 指定所拥有资源的释放顺序，执行全部关闭动作，重复抛出的同一异常不发生自抑制 | `ResourceCloserFailureTest` |
| `indexing.build.BuildFileMapping`、`indexing.core.build.BuildMappingPersister` | 原制品保留，传递同一 attempt 身份到持久化端口 | CREATE INDEX 与增量刷新两种入口共享转换器。批中掺入不同 attempt 在提交前被拒绝 | `IndexBuildClaimLifecycleTest`、`JdbcIndexMappingPublicationFenceTest` |
| `persistence.indexing.postgresql.metadata.JdbcIndexMappingWriteFence` | 在原事务中锁定精确 claim | 同一连接先锁 descriptor 和必要的 staging 行，再锁 index、generation 和 owner 一致的 claim，写整批映射后提交。旧心跳不授权后续写入 | `JdbcIndexMappingPublicationFenceTest` 覆盖已成功心跳后被接管、发布冲突和批写回滚 |

## 按实际调用方复核契约制品

本次按顶层领域目录统计 Java 主源码实际引用该制品的其他领域制品数。同一领域的依赖、测试代码、仅由 Maven 传递引入的依赖均未计入。反射发现和外部插件需要另外检查。表中计数用于判断契约是否仅在领域内部使用；计数不代表实现数，也不证明能力已通过端到端验证。

| 契约制品（省略前缀 `pangrova-`） | 领域外调用制品数 | 可定位的生产调用制品举例 |
|---|---:|---|
| `ai-api` | 4 | `coordination-core`, `persistence-ai-postgresql` |
| `connector-api` | 36 | `coordination-core`, `execution-core` |
| `coordination-api` | 4 | `persistence-coordination-api`, `persistence-coordination-memory` |
| `execution-api` | 14 | `coordination-core`, `generation-api` |
| `filesystem-api` | 7 | `ai-worker`, `coordination-core` |
| `function-api` | 4 | `ai-worker`, `planner` |
| `generation-api` | 9 | `ai-control`, `coordination-core` |
| `governance-api` | 5 | `coordination-core`, `metadata-core` |
| `indexing-api` | 7 | `connector-iceberg`, `coordination-core` |
| `materialization-api` | 4 | `coordination-core`, `persistence-materialization-api` |
| `metadata-api` | 8 | `coordination-core`, `persistence-catalog-api` |
| `observability-api` | 10 | `coordination-core`, `execution-core` |
| `persistence-catalog-api` | 4 | `coordination-core`, `metadata-core` |
| `persistence-coordination-api` | 5 | `coordination-core`, `coordination-zookeeper` |
| `persistence-file-api` | 3 | `coordination-core`, `server-coordinator` |
| `persistence-generation-api` | 5 | `coordination-core`, `execution-core` |
| `persistence-indexing-api` | 3 | `coordination-core`, `indexing-core` |
| `persistence-jdbc-api` | 4 | `coordination-core`, `metadata-core` |
| `persistence-materialization-api` | 3 | `coordination-core`, `materialization-core` |
| `persistence-query-api` | 6 | `connector-iceberg`, `connector-paimon` |
| `persistence-security-api` | 8 | `coordination-core`, `governance-core` |
| `persistence-table-api` | 3 | `coordination-core`, `metadata-core` |
| `persistence-workflow-api` | 3 | `coordination-core`, `metadata-core` |
| `scheduler-api` | 3 | `coordination-core`, `query` |
| `security-api` | 29 | `ai-control`, `ai-worker` |
| `shuffle-api` | 7 | `coordination-core`, `execution-core` |
| `streaming-api` | 3 | `coordination-core`, `persistence-streaming-postgresql` |

Client 的三个入口为 CLI、JDBC 和 HTTP。原 Client Core 的 19 个生产类型按调用职责归入 Common。跨进程传递的 ResultEndpoint 与 ResultProtocol 一起归入 `common.client.result`。HTTP SDK 创建连接，CLI 和 JDBC 借用 SDK，Common 声明查询操作与结果值。

Function 是此前唯一只有一个子模块的领域目录。源码已上移至 `pangrova-function/src`，根 reactor 直接构建该目录里的 `pangrova-function-api` JAR。AI 函数工厂及 Native 回调继续依赖同一个跨域契约。

| 原类或入口 | 当前制品和职责 | 运行边界与验证 |
|---|---|---|
| `ai.worker.provider.ConfiguredAiProviderRegistry` 的厂商分支 | `ai-worker` 保留模型、密钥解析、并发创建和关闭所有权；按 `ai-api` 的 `ai.api.provider.AiProviderFactory` 取得实例 | `ConfiguredAiProviderRegistryTest` 和 `ConfiguredAiMultimodalProviderRegistryTest` 覆盖版本、密钥与故障 |
| `metadata.service.TableControlWriteAuthorities` | 删除，Coordinator 通过 `CoordinatorTableWriteAuthority` 装配 PostgreSQL 工作流与 `MetadataTableControlPolicyProjections` | Generation 与 Query 在调用方事务中接受注入的 `TableControlWriteAuthority`，锁、租户、表、模型校验保持原顺序 |
| `MetadataTableControlPolicyEvaluator` / `TableControlPolicyEvaluator` | 删除中转接口与实现，`PostgresTableControlWriteAuthority` 使用 Security Core 的 `DeferredAuthorization` 与 `DeferredPolicyReader` | Metadata Core 生产依赖只保留 workflow API，前台与延迟授权共享稳定表身份的行策略和列掩码投影 |
| `ai.control.config.AiProviderDialectValidator` | OpenAI 校验移至 `ai-openai` 的 `ai.provider.openai.AiProviderDialectValidator`；`ai-control` 中的 `AiProviderFactories` 发现工厂并拒绝重复 dialect | 注册表用同一批工厂校验模型、创建模型客户端。通用 endpoint、egress 和能力限制仍由控制面校验 |
| `ai.provider.openai.inference.OpenAiCompatibleInferenceProvider` | 移至 `ai-http` 的 `ai.http.chat.OpenAiCompatibleInferenceProvider`；管理 OpenAI 兼容 chat 的调用、受控 HTTP、批处理及准入 | OpenAI 和 Qwen 两个独立 Provider 复用同一传输实现，两个 Provider 无相互依赖 |
| `ai.qwen.QwenProviderFactory` | `ai-qwen` 的生产服务描述符安装工厂，支持 Model Studio 兼容接口上的 TEXT inference | `QwenProviderRegistryTest` 通过真实本地 HTTP 往返核对部署名、请求和结果，同时验证拒绝错误路径和能力；阿里云账号线上调用待验 |
| `parser.api.ast.statement.privilege.GrantPrivilegeStatement`、`RevokePrivilegeStatement` | 保留；语法经 parser 构建 AST，Server 的 `PrivilegeStatementTasks` 接入当前授权和持久化，授权事实由 Security 持有 | `SqlPrivilegeAuthorityE2ETest` 覆盖认证 HTTP 提交、回执、授予/撤销后的 Native 读取权限以及越权拒绝 |

## Common 无状态工具与消费边界

| 原类或入口 | 目标与调用位置 | 契约和验证 |
|---|---|---|
| `org.pangrova.materialization.api.domain.Require`、`org.pangrova.materialization.api.refresh.RequireExtension`，以及 Connector、Generation、Persistence 等制品内的同义私有校验 | 合并到 `org.pangrova.common.util.Preconditions`。调用方传字段名，保留原字符串、原异常类型和消息。Semantic 和 AI 的可选空值规则与必填字段分开 | `PreconditionsTest` 验证 Unicode 空白、保留首尾空格、null 和字段名。Generation 的发布、刷新 attempt 与 AI Blob 超时代码共享严格为正的 Duration 校验 |
| `CdcRow` 的数值访问器 | 复用 `org.pangrova.common.util.NumberConversions`，调用者指定缺失或非数值的返回值 | `NumberConversionsTest`、`CdcEnvelopeFieldEvolutionTest`。Paimon 类型写入仍执行严格转换，非法值继续失败 |
| `CdcRow`、`TypedRow`、`ColumnValue.MapValue`、`SplitData`、`DataSplitProtoCodec`、`ShuffleStageStatsReport`、`NativeMetricsPayload` 中的顺序 Map 快照 | 统一到 `org.pangrova.common.util.OrderedMaps.immutableCopy`，每次生成独立、浅层、不可修改的快照 | `OrderedMapsTest`、`ColumnValueNestedNullTest` 核查输入顺序、null 与外部修改。SQL 行序与线格式由调用域定义 |
| HTTP Client 的 `metadataPathComponent` 和 Worker 的 `CatalogDeploymentClient.encode` | 统一到 `org.pangrova.common.util.UriComponents.encode`。Worker 路径中的空格使用 %20，字面加号使用 %2B | `JdbcMetadataProtocolTest`、`CatalogDeploymentClientUriTest` 使用 HTTP 对端检查路径、查询参数与 UTF-8 编码 |
| `org.pangrova.coordinator.http.QueryPathUtils` | 删除唯一自测及类。实际 state、cancel、profile、profile JSON 路径统一由 `QueryResource` 直接绑定 JAX-RS 的 queryId 路径参数 | 检查了生产引用、反射类名与资源注册。`QueryCancelProtocolTest` 核验实际取消请求和返回值 |
| `org.pangrova.metadata.codec.json.MetadataJsonMapper` | 七个静态转发方法删除。PostgreSQL 的 Catalog、Workflow、Indexing 及 Coordinator 直接使用 `org.pangrova.common.json.CanonicalJsonMapper`。Coordination Core 和 Indexing PostgreSQL 移除用于转发的 Metadata Codec 直接依赖 | `CanonicalJsonMapperTest` 验证相同时间表示、泛型、字节、树所有权和错误。Metadata Codec 的 `TableMetadataSerializer` 继续服务缓存和 PostgreSQL 的表模型 |
| Indexing、Generation、Security 的各领域 Codec，以及 JDBC Support | 保留原制品。分别拥有索引字节格式、Generation 持久化证据与重放限制、权限范围信封，以及连接池和 JDBC 资源生命周期 | 无状态辅助抽取未移动其提交权、数据所有权和资源关闭职责。通用工具均沿既有指向 Common 的依赖边使用 |

本批在 JDK 17 临时输出目录编译变更源码并独立运行 144 项 JUnit 测试，全部通过，包含 PostgreSQL 16 的真实 Generation 发布测试。初次扩大验证时，临时类路径混入旧的 Semantic/Generation 测试夹具和重复服务描述符。重新编译当前源码并移除旧 Provider 类路径后得到以上结果。另用 Common 与 Jackson 注解加载 21 个 Client 契约类型，并运行带完整 Jackson 依赖的固定编码回归。

## Semantic View

| 原类 | 处置及实际目标 | 调用、事务与资源 | 验证 |
|---|---|---|---|
| `coordinator.semantic.SemanticViewStatementService` | 创建、幂等判断、来源授权、校验和核对及发布移至平铺制品 `pangrova-semantic` 的 `semantic.core.version.SemanticViewService` | Server 将 Store、AccessControl 注入一个领域服务，三个 SQL task 通过同制品的 `semantic.api.version.SemanticViewOperations` 直接调用。PostgreSQL Store 在一个事务内更新 active version 与 publication epoch；服务借用仓储，无连接或发布缓存 | `SemanticViewSqlIntegrationTest`、`JdbcSemanticViewStoreTest` |
| 同一旧类的会话和名称解析 | 移至 SQL task 同包的 `coordinator.query.execution.dispatch.task.semantic.SemanticViewSqlScope` | 入口从认证会话取 tenant、默认 catalog 与 schema，向领域服务传入完整身份。SQL AST 和 QuerySession 留在调用侧 | `SemanticViewSqlIntegrationTest` 的无租户拒绝、跨租户隔离与 SQL 分发用例 |

## Materialization 与 Streaming 子模块复核

| 制品 | 入口、状态与保留理由 |
|---|---|
| `materialization-api` | 定义尝试、代际、刷新上下文、持久化刷新类型、刷新策略与工厂契约，供 Planner、Persistence 和 Coordinator 使用 |
| `materialization-core` | 一套定义、attempt、发布、恢复与保留流程共享同一批端口和身份；注册、创建首代、增量能力判断与计划验证共用一个注册表 |
| `materialization-strategies-builtin` | 三个无外部资源的算法独立于 Core 发布。Server 仅保留 runtime 依赖。改变服务文件顺序不会改变显式策略和自动策略优先级 |
| `streaming-api` | CDC 信封、源、转换器及其工厂、checkpoint、提交和 watermark 端口被生产控制面及 PostgreSQL 使用 |
| `streaming-core` | Pipeline 的读取、解码、写入、checkpoint 和 watermark 由同一调度生命周期协调；格式实现通过服务发现 |
| `streaming-source-kafka`、`streaming-formats-builtin`、`streaming-writers-builtin` | Kafka SDK、JSON 格式和 Connector writer 分别打包；具体生产与测试入口见本文 Streaming source、format 与 writer 小节 |

本批 64 项策略发现、注册表、刷新服务、证明与 attempt 测试通过。另在 JDK 17 编译本家族全部生产源码并运行 6 项包依赖架构测试，核对领域值、调用端口、扩展契约的依赖方向。

## Indexing Core 与 JVM 策略

| 原位置 | 当前归属 | 调用及所有权 | 验证 |
|---|---|---|---|
| `indexing.build`、`indexing.core.build`、`indexing.metrics.IndexBuildMetrics` | 同一制品 `indexing.core.build` | DDL 和维护复用同一个 build coordinator 与 attempt，映射提交继续使用当前 PostgreSQL claim；Core 借用执行器及存储端口 | `IndexBuildClaimLifecycleTest`、`JdbcIndexMappingPublicationFenceTest` |
| `indexing.eval`、`indexing.core.evaluation` | `indexing.core.evaluation` | JVM 数据准备、断路器与评估计数同属一个能力包，Java 评估服务仍未接入实际 Worker split 裁剪 | `IndexEvaluationPreparationTest`、`IndexEvalCircuitBreakerTest` |
| `indexing.storage`、`indexing.fs`、`indexing.metrics.IndexCacheMetrics` | `indexing.core.storage` | 数据访问器、缓存、缓存指标及 attempt 专属对象路径集中；文件访问器借给单次构建或读取 | `IndexStorageLayoutTest`、`IndexCacheDataFileIndexTest` |
| `indexing.store`、`indexing.planner.vector`、`indexing.config` | 分别归 `indexing.core.evolution`、`indexing.core.planner.vector`、`indexing.core.config` | Schema 变动通过持久化端口更新 descriptor；规划只识别向量能力并绑定计划；配置由 Server 构造 | `VectorSearchRewriterCastTest`、全量 Indexing 主源码编译 |
| `indexing.registry.IndexEngineRegistry`、`indexing.builtin.codec.BuiltinIndexSerializers` | 注册表移至 `indexing.core.strategy`，删除手工 serializer 清单 | 每个已发现的 `IndexStrategyProvider` 提供自身 payload codec，由 Server 一次装配同时用于实际构建和读写。Builtin 在 Server 的运行时 classpath 中提供九条注册 | `IndexStrategyDiscoveryTest`、`IndexCodecGoldenTest`、`IndexTypeStableIdTest` |

## Governance 血缘后端

原 `governance.lineage.LineageBackendConfiguration` 的进程装配迁至 Server Coordinator。
`governance.api.lineage.LineageBackendFactory` 分别由 Log、OpenLineage HTTP 实现，服务描述符随
Governance Providers 发布。工厂发现不打开 HTTP 客户端，选中的工厂在出站方式和 outbox
要求检查通过后才创建 transport。Query 拥有持久化事实；Governance 的 emitter 完成实际发送、
确认与 drain 后关闭 transport。启动拒绝和真实 HTTP 测试套件分别覆盖工厂选择、
PostgreSQL 发送确认及进程接管。SQL 质量检查所需的授权查询执行器仍未在此路径装配。

## Scheduler 的决策输入、重试预算与 Generation 发布路由

| 原类或包 | 当前制品与包 | 生产入口、状态与验证 |
|---|---|---|
| `scheduler.api.placement.{ClusterSnapshot,SchedulingWorkerSnapshot,ControlLoopAction}` | `scheduler-api` 的 `.api.control` | 这些类型表达整个集群的一次观察及拟议操作。实验接口 `SchedulerControlLoop` 和空动作实现有独立测试，目前没有 Coordinator 定时执行这些操作，也没有生效的 control-loop 配置项 |
| `scheduler.api.placement.{RetryBudget,FaultTolerancePolicy,NativeFaultPolicy,TaskDuration}` | 前两个归 `.api.retry`，Native 故障策略归 `.api.health`，任务耗时归 `.api.speculation` | `NativeCrashHandler` 处理故障，慢任务识别使用 `TaskDuration`。`RetryBudget` 有独立策略测试，线上失败任务的重试次数由 Query 的 Stage 维护 |
| `scheduler.core.speculation.{SpeculativeWorkerInventory,SpeculativeQueryInventory,SpeculativeAttemptCanceller}` | `scheduler-api` 的 `.api.speculation` | Coordinator 提供 Worker/Query 的观察和取消入口。带有 Protobuf 请求或执行报告的 `TaskRegistry`、`SpeculativeAttemptDispatcher` 留在 Core 执行适配层。Scheduler API 独立加载不需要 Protocol 或 Calcite |
| `SpeculativeTaskLauncher` 的 lineage 和次数缓存 | 删除重复缓存，新增 `scheduler.api.retry.TaskRetryAdmission` | `QueryBootstrap → DAGLifecycleController → Stage` 核对可替换性、持有 lineage 预算与槽位，并在派发前登记终态路由。错误的 query、无可用替代节点、预算耗尽均不会消耗额度。实际派发失败时回退槽位、次数与路由。取消推测落败 attempt 与失败重试的接纳分开 |
| `scheduler.api.placement.WorkerSelectionStrategyFactory` 和内建算法 | 工厂与 placement 值保留 Scheduler API，候选资格和调度留 Core，算法与服务描述符留 `scheduler-strategies-builtin` | Coordinator 配置 `coordinator.query.placement-provider` 经 `QuerySchedulingWiring` 查找版本兼容且 ID 唯一的工厂。角色装配和租户选择用例覆盖该路径 |
| `generation.core.submission.GenerationPublicationFactory`、`IndexingBootstrap.artifactPublicationStores` | 删除转发及重复路由，使用 `generation.core.publication.GenerationPublicationRouter` | Server 将 vector index、text index、generated column values、tabular export 四类输出一次性映射到三个 PostgreSQL 参与者，`DurableGenerationPublisher` 在既有 claim 和发布确认范围内调用所选存储端口。单元测试检查四条分派、拒绝未知类型、失败延后重试、已占用 claim 与过期 claim 接管；数据库测试检查真实空表发布 |
| `generation-api`、`generation-codec`、`generation-core` | 保留三个现有制品 | API 供 Planner、Persistence 与调度端引用；Codec 实现跨读取者统一的值及图编码；Core 驱动提交、执行与发布。加入 PostgreSQL 路由发生在 Server，契约保持独立，Core 无须反向依赖 Provider |

独立 JDK 17 编译包括 Coordinator 装配入口与 SQL 端到端 fixture。19 个定向测试类运行 158 项，含 PostgreSQL 16 容器发布、Stage/DAG 的重试竞争与回退、推测取消、策略发现及 query 归属检查。此处计数覆盖被执行的定向套件。

macOS arm64 的远端分区读取比较观察了持久化 Arrow 字节、mTLS 响应帧及读取时间。三轮交替测量包含 192 个有效样本。环境、分位数、吞吐、复现方式和原始 CSV 见[远端 Shuffle 分区读取测量](../testing/shuffle-read-comparison.md)。

原 Worker ShuffleRequestRouter 及其自测已移除。旧 Java 注册表与明文 ticket 编解码只供自身测试调用。实际 Direct Flight 路由由 C++ FlightShuffleServer 注册带租约的 handle，使用签名票据，并在运行时关闭时释放共享端口。

## Security Secret 与 AI 函数资源

Coordinator、计算 Worker 和 AI 共享 `pangrova-security-secrets` 的前缀注册与发现。Coordinator 的发行物包含 ENV 和 FILE，标准 Worker 包含 ENV。由角色 classpath 决定可用后端。
`SecretResolverRegistry` 维护前缀到实现的唯一映射，未知前缀与重复注册均拒绝。
`CoordinatorSecretDiscoveryTest` 经角色装配验证解析。Worker Catalog 传递租户作用域，AI 按模型配置的引用查找，ENV 拒绝空值。AI 函数的注册、生产资源初始化和关闭由同一 `AiFunctionFactory` 串行管理。
关闭后的调用在创建文件、线程或模型客户端前拒绝，重复关闭只释放一次。

## AI、Security、Governance 与 Connector 的顶层类型

这四个家族的 61 个顶层类型按所属制品的 `.enums`、`.exception` 和 `.constants` 归类。包括 `connector.api.enums.WriteMode`、`security.api.exception.AuthorizationDeniedException`、`ai.control.enums.EvaluationRunState` 和 `connector.api.constants.ConnectorFactoryContract`。嵌套异常原因、状态与只在本类使用的常量仍由原类定义。Filesystem 以及现存缓存能力包没有需要迁出的同类顶层类型。

Connector 的类加载隔离测试同时打包宿主契约与插件代码，核对公开签名及嵌套类型的 Class 身份，也验证 Common 工具和 Protocol HTTP DTO 由插件自行加载。

## Quality、Lineage 与文件后端

| 原路径 | 当前处置及验证入口 |
|---|---|
| `QualityCheckService`、`GovernanceStatementHandler` 与写前检查各执行规则 | 程序化入口和定时任务使用 `GovernanceStatementHandler`。与写前检查共享 `QualityRuleEvaluation` 校验结果的规则、表、严重性与行数。实际审计与写入拒绝由 `WriteQualityGateTest`、`CronSchedulerTest` 等覆盖。 |
| `GovernanceQueryRunner` | 结构化读取携带稳定表 UUID 与租户主体。PG SQL 测试覆盖表达式；生产尚无已授权的定时读取通道与候选写入 post-image。 |
| `LineageBackend.isAvailable()` | 两个既有实现曾无条件返回 true，现以发送确认表达交付成功。所有实现明确承担 close 契约。LOG 无私有外部资源，HTTP 客户端由发送端在 drain 后收回。 |
| Worker 中的环境变量解析和硬编码文件后端 | 共享 Secret 注册表接入实际 Catalog 与 AI，`BlobStorageFactory` 提供 LOCAL、S3、OSS 的 FILE 发布路径；`ObjectStoreAccessorProvider` 提供 LOCAL、S3 文件空间。COPY 的精确版本来源继续使用 S3。 |
| `EmbeddingProviderLookup` 与静态目录转发 | 移除前者无人调用的空值路径，模型广告由 `WorkerAiModelCatalog` 直接读取已安装工厂的模型定义。执行期间由配置注册表负责创建、缓存与关闭。 |

## Metadata、Table 与连接器的公共表示

| 原职责或类 | 现有入口及 owner | 验证 |
|---|---|---|
| Metadata 的 RegistryTablePolicyAuthority 转发五个方法 | CatalogRegistry 在构造时绑定唯一 TableControlPolicyBinder。它直接实现 TablePolicyAuthority。行策略、列掩码、质量规则借用同一实例。TableControlService 决定 stable ID、当前部署和 ACTIVE 准入，PostgreSQL 工作流继续拥有事务 | TableControlRowPolicyStoreTest、TableControlQualityRuleStoreTest、MetadataTagOperationsTest、TableControlPolicyBinderTest 包含改名、重建同名对象、旧授权拒绝 |
| metadata.service 的行策略存储、事务策略投影、DefaultConnectorSession | 行策略及列掩码在 metadata.security 按 stable ID 投影。Connector 会话在 metadata.connector 构造，测试验证目标 catalog/schema、租户传递及令牌隔离。缓存键按授权会话取摘要的测试与包内缓存键同位置 | MetadataResolutionCacheKeyTest、DefaultConnectorSessionTest；与上一行合计 32 项通过 |
| 无装配引用的 MetadataCleanupConfig | 原类列出的三个 YAML 字段没有加载入口。失效日志由 maintenance leader 调用 PostgreSQL cleanup，L1 由缓存自己的 TTL 管理。删除失效配置声明 | ModuleInvalidationLifecycleTest 检查领导权轮换及关闭 |
| `DefaultMetadataService` 中的已解析表缓存、`MetadataResolutionCacheKey` | `metadata.core.resolution.CatalogTableResolution` 持有按认证会话隔离的 cache、受控表版本核验、观察表 TTL 与 DDL 后的失效。DDL 使用当前认证会话从 Connector 读取元数据。SQL 与 REST 调用仍经 MetadataService | DefaultMetadataServiceResolveTest、DefaultMetadataServiceCacheTest、DefaultMetadataServiceAlterTableTest、MetadataResolutionCacheKeyTest 共 12 项通过 |
| `metadata.generation` 的 PostgreSQL 事务装配、发布候选项和事务权限 | `persistence.workflow.postgresql.generation` 的 PostgresGenerationStores、PostgresGenerationPublicationCandidates、PostgresGenerationTransactionAuthority 组合当前权限、fence 与 Generation 参与者。Metadata 的生产代码移除了对 Query、Generation 和 Security PostgreSQL 实现的依赖 | 实际 PostgreSQL 的 Generation 存储与 fencing 30 项、锁 4 项通过。跨域提交的整组集成测试随 full-regression 验证 |
| Coordination 中的 HTTP 端点、Filter 和 StageFileUrlService | `server.coordinator.http` 持有 Jersey 注册与入口。Query、Catalog、Copy、File 的资源和过滤器从 `http.endpoint` 向下划分。`http.request` 独立持有请求限额与进程内幂等键，使端点无需导入装配包 | 迁移后的 FileUrlServiceTest、RequestLimitControlIsolationTest、CatalogRestResourceTest、QueryStatusResourceTest、InternalAuthFilterPropertyTest 共 41 项通过。首次复查检出五个包组成的循环，调整请求包后扫描 826 个生产包，循环分量为零 |
| Iceberg、Paimon 各自的同格式快照标签命名 | Common 的 connector.SnapshotPinTags 拥有持久标签名称的 UTF-8 编码与快照后缀。两个 Connector 分别向自身 Catalog 创建与删除标签、验证快照与标签的一致性 | 提交 d10e8c49 的 SnapshotPinTagsTest、IcebergSnapshotPinProviderTest、两个 SnapshotPinLifecycleTest 和两组架构门禁共 39 项通过 |
| Java FuzzerCoordinator.NativeFuzzerRunner | 当前发行物没有实现、服务注册或 JNI 入口。可执行的 run-operator-fuzzer.sh 调用 C++ reference runner 与 DuckDB 做 SQL 生成自测。Java 调用 Pangrova Native 的差分执行为未实现 | Java 接口、CMake 入口、runner 和脚本按各自可达范围审查 |

## Execution、Query 终态与 Coordinator 元数据进度

| 原位置 | 当前归属 | 调用与验证 |
|---|---|---|
| `coordinator.query.execution.dispatch.dag.StageCompletionCallback` | `pangrova-query` 的 `query.api.stage.StageCompletionCallback` | Query 管理逻辑任务、Stage 和 DAG 最终结果。生产 `DAGDispatchEngine` 明确查询是否完成并交付终态。删除默认返回 true 的实现，JVM 编译要求所有实现显式给出就绪判定。端口仅使用 JDK 和 `QueryFailure` |
| `coordinator.query.task.TaskReportCompletion` | 保留报告接收侧，合并普通、后台和推测路径中重复的终态投影 | 已验证的报告提供 query、state 和四个错误字段，胜出 attempt 由接收者选择原始逻辑任务槽位。接收层继续负责 fence、journal、写片段与 drain；Query API 不接收 `TaskStatusReport` 或 REST 错误。`TaskStatusReceiverWriteCommitTest`、`SpeculativeExecutionWiringTest` 观察实际顺序和结果 |
| `execution.core.stage.QueryExecutionScope` 和 `execution.core.stage.binding` | 保留 Execution Core，修正规则文档曾指向 Query Core 的描述 | 绑定 QuerySession 以及会话或 durable 任务 authority、写入配置和检索/重排意图。若迁入 Query 将经由 Execution Core 的 StageExecutionContext 反向依赖 Query 本身。Execution API 单独发布身份、期限和 stage/purpose，不持有流程状态 |
| `coordinator.coordination.InvalidationProgressRegistryAdapter` | 改为 Server 的 `bootstrap.CoordinatorInvalidationProgress` | Server 一次选择 JDBC 或 quorum 成员注册表；HA 和失效日志读取相同的注册表。查询角色注册后读取自己的已提交水位，再开始轮询。独立维护角色仅观察 peer floor，按本地已消费位置与在线节点的最低水位两者较小值清理。维护租约重获时重新读取下界 |
| `PostgresInvalidationPoller.setInvalidationProgressRegistry` | 删除晚绑定入口，由构造器一次性绑定查询成员或维护观察者 | 重启时注册表读取失败向启动层传播，操作者可在恢复后重试；读取错误不会把数据库中的进度覆盖为 0。轮询时暂时上报失败仍可继续应用本地失效，后续轮次重报；PostgreSQL 上报端将 SQL 失败传给调用者 |

真实 PostgreSQL 的 `CoordinatorInvalidationProgressTest` 将两个已提交失效条目写入日志，
恢复保留第一个水位的 query 成员只驱逐第二个目录。维护测试让第二个进程夺取、释放实际数据库租约；
本进程重获更高 epoch 后仅删除落后成员确认的条目。`JdbcInvalidationPollerTest` 同时覆盖首次注册、
游标读取失败后恢复、分批消费和未知 peer 水位。

同 ID 重新注册只更新成员信息。ZooKeeper 用 znode 版本 CAS 保留并发上报的水位；Memory 在同一 map 更新内保留水位，PostgreSQL 的注册 upsert 保留已持久化值。成员记录已被删除时，以新记录的水位重新开始。

独立 JDK 17 验证共 196 项。任务回报、变更后的目录写入 fixture 和 Execution Core 的 121 项通过；
失效日志、后端故障与维护租约的 51 项通过；真实嵌入 ZooKeeper 及 Memory 成员测试 23 项通过；
Execution API 单独 classpath 反射验证 1 项通过。首轮写入 fixture 在删除目录时显式抛错，现改为按精确部署身份删除并保留查询所需历史，三组写入测试重跑通过。

### Transport / Client / Protocol 与运行角色发行边界

Client 和 Transport 的六个子制品内没有以 Bridge 或 Adapter 命名的生产类型、包或服务注册。实际调用中，原 `org.pangrova.client.http.ResultStreamFetcher` 仅向 `FlightResultFetcher` 转发调用、转换异常并转交关闭请求，唯一调用者是 `PangrovaRestClient`。该包装类已删除。SDK 现在直接拥有 Flight reader，在批量和流式两个公共入口转换连接或服务端错误，保留错误码、服务端状态及原始异常。客户端关闭时仍先封住 HTTP 入口，再关闭 Flight 和 HTTP 资源。

`BatchStreamProducer` 在 Flight Server 侧实现 Arrow 的 `FlightProducer`，负责票据校验和批流交付，保留在 Transport Flight。安全和非安全监听器都使用同一生产者。Client HTTP 和 Transport Flight 两模块现有 142 项测试在独立 JDK 17 reactor 中通过。新增覆盖以要求 TLS 的客户端读取明文端点，两种读取入口均保留 `TRANSPORT_TLS_REQUIRED` 和 Transport 原始异常，测试类四项通过。

| 原责任 / 调用 | 归属与处置 | 证据 |
|---|---|---|
| JDBC 元数据直接读取 Substrait 的向量元素与可空性 | Transport Client 的 `ColumnDescriptor` 解读 wire 描述，JDBC 将 Java 数组类型和可空标志转换为 JDBC 返回值，Protocol 直接依赖降为该模块的测试 scope | 独立打包后的 JDBC JAR 逐 class 检查，无 Protocol 或 Substrait 类型引用 |
| 从 Maven runtime 图生成角色发行清单 | `deploy/docker/stage-role-classpaths.py` 检查 JAR 内主类、角色禁用类及 Native 载荷，包括多版本条目；启动器按该角色的完整清单启动 | macOS ARM64 私有 reactor install 和 staging 产出 Coordinator 358、Worker 306、Standalone 416 个 JAR；三个独立 JDK 17 进程成功验证角色主类、Planner 与 Native Bridge 可见性 |
| 物理 Shuffle 测试的子 Worker 使用 Surefire 测试 classpath | 发行验证可传 `-Dpangrova.e2e.worker.classpath`，取自 `bin/pangrova-classpath.sh` 的 `pangrova_runtime_classpath worker`；未打包的 Maven reactor 保留测试 classpath | 该快照的 Worker classifier 与同次 CMake 的 osx/aarch64 Native 库 SHA-256 相同；Linux 镜像与跨 Worker 执行属于独立验证 |
| Query Submit 按裸 key 预存 queryId | Server HTTP 以经过认证的 principal 和 key 所有权绑定 SQL 与完整会话属性。成功保存原始完整回执，尚未登记的拒绝允许重试，已登记或无法确认的失败保存带 queryId 的错误 | `IdempotencyKeyRegistryTest` 覆盖并发与到期，`QuerySubmissionIdempotencyE2ETest` 覆盖真实身份、HTTP 409、drain 重试与已登记失败；待统一回归 |

## 目录读取、表 DDL、列标签与流状态的归属

| 旧位置 | 生产调用方和现行归属 | 验证状态 |
|---|---|---|
| Coordination 的 `metadata.read` 八个生产类型 | 全部进入 Planner 的 `core.metadata.catalog`。MetadataSchemaBuilder 和 LazyMetadataSchema 构造 Calcite 结构，MetadataResolvedTable 保留选定的列声明。PlanningReadBindings 按当前表身份复查 snapshot/range 与缓存，CatalogStatisticsProvider 与行类型使用同一绑定。认证会话、快照选择与增量选择随绑定同属 Planner。具体注册表实现 Metadata 的 CatalogReadAccess，TableControlService 实现 TableReadAuthority，TableControlStatisticsService 实现 CurrentTableStatistics | 单模块类型推导与缓存用例已迁入 Planner。真实 Connector、目录不可用与 SQL 链用例继续使用下游集成夹具。待统一回归 |
| DefaultMetadataService 的 CREATE、ALTER、DROP 和列声明转换 | Metadata 的 core.ddl.CatalogTableDdl 持有 ALTER 互斥、连接器 DDL、表身份核对、缓存失效和事件派发。core.resolution.CatalogTableResolution 拥有读缓存。每个 MetadataService 实例拥有一组读缓存与 DDL 执行器 | DefaultMetadataServiceControlTest、DefaultMetadataServiceAlterTableTest 与 MetadataColumnDeclarationsTest 待统一回归 |
| MetadataTagOperations | Governance 的 core.classification.ColumnTagBindings 通过 TablePolicyBindings 绑定已捕获的稳定表身份，并重放标签。Metadata 负责在当前用户会话内从 Connector 解析实际列；业务执行不需要反向加载 Metadata | PostgreSQL 标签、改名与旧授权用例待统一回归 |
| governance-providers 同时发布日志、HTTP SDK 与质量执行器 | 两个血缘后端各在自己的 JAR 注册 LineageBackendFactory。日志后端依赖治理契约和 SLF4J，OpenLineage 后端拥有 SDK 和 HTTP 客户端。SQL 质量执行器独立发布；缺失授权执行器时的拒绝行为归治理流程 | 启动时重复标识拒绝、Marquez 和 HTTP 用例待统一回归 |
| Streaming Core 的两种进程内存储 | persistence-streaming-memory 持有 CDC 注册和 watermark。Server 在明确选择进程内状态的调用路径实例化。Streaming Core 的生产源码只使用读写端口 | WatermarkTrackerTest、StreamingGovernanceChainTest 和 PostgreSQL 恢复用例待统一回归 |
| ModuleIntegrationBootstrap 的两组计时任务 | Server 的 CatalogMaintenance 独占目录同步与失效日志清理的定时器。协调器在获得领导权时安装任务，失去领导权时取消定时器，关闭时等待执行器退出并关闭 poller | ModuleInvalidationLifecycleTest 用真实 PostgreSQL 观察轮询线程和租约轮换，拆分后待统一回归 |

### Streaming source、format 与 writer 的实际边界

`CREATE STREAM ... FROM <provider>` 的 source id 经 AST、SQL 打印、StreamState、PostgreSQL、恢复调度传递。Server 发现一次 `CdcSourceFactory`，DDL 和 Scheduler 共用注册表。DDL 先检查 source 属性和 format，Scheduler 按持久化的 source 和不可变 checkpoint id 创建独占 reader。Kafka provider 负责 SDK、topic、group、rebalance、seek、commit 与部分初始化异常的关闭。Core 和 API 没有 Kafka SDK 编译依赖。退役的 `KafkaStreamSourceReader` 仅实现无人调用的 `StreamSourceReader`，截断 poll 时会跨过未消费记录；生产 `KafkaCdcSourceReader` 保留按 partition 回退与恢复 seek 的实现。

三个 JSON format 共用 Jackson 和注册方式，进入 `streaming-formats-builtin`。Lake、merge-tree 和 incremental writer 使用 Connector 写入契约、Types 与 Maintenance，进入 `streaming-writers-builtin`。Coordinator CDC 实际写入仍使用 `ConnectorPageSinkCdcWriter`；独立 writer 制品由现有 writer 使用方和集成测试引用，不将这些 writer 自动广播成支持的 SQL sink。

最终统一验证需覆盖 `StreamDdlRoundTripPropertyTest`、`StreamSchedulerLifecycleTest`、`CdcSourceRegistryTest`、`KafkaCdcSourceReaderIntegrationTest`、`StreamingKafkaIdentityIntegrationTest` 与 PostgreSQL StreamState 存取。Kafka 实际交付为现有 source，新增 provider 能力由其自身的真实实现与验证决定。

## Table 所有权变更与 Metadata 读取

| 原类和职责 | 实际调用方及归属 | 状态、释放和验证 |
|---|---|---|
| `TableControlService.rename/completeRename` | DDL 处理器直接调用 `org.pangrova.table.rename.TableRenames`。begin 将原注册写为 RENAMING，complete 验证目标物理身份，再激活注册。 | 共享同一 TableControlStore，保留 revision 检查和 rename intent。带版本的最终状态与观察记录仍在一次写入中提交。现有 Memory/PG 与 DDL 测试参与统一回归。 |
| `TableControlService.observeVersion/markUnavailable` | 周期性 observer、ANALYZE 和受控表维护直接调用 `org.pangrova.table.observation.TableVersionObservations`。 | 观察器只更新捕获的注册版本。目录不可达与已证实表缺失有不同结果。TableRenames 与 observations 均借用相同的 TableCatalogAuthority 及后端。 |
| `CatalogTableResolution` | Metadata 读取在访问 Connector 前取得目录部署身份及表 ownership view，读取完成后复核两者。 | 缓存键包含目录 incarnation 和 revision。外部 OBSERVED 表在目录重建后重新解析。覆盖读取期间发生 ADOPT 和同名重建的用例已加入 DefaultMetadataServiceControlTest，待统一运行。 |
| `ModuleIntegrationBootstrap` 中元数据定时任务 | Server Coordinator 持有 `CatalogMaintenance`，按 leadership 期限安排同步及失效清理。 | stop 取消调度，close 等真正退出再释放借用的 poller。某项释放失败仍尝试其它独立资源，保留失败 owner 供重试。 |

### Scheduler、Generation、Materialization、Execution、Indexing 顶层值与异常

每一行对应单一制品的包。下列 26 个顶层枚举、6 个异常和 1 个纯常量类迁移 FQN，保留枚举常量名称、声明顺序、错误码和编码值。实现与测试根据新 FQN 编译。Scheduler Core 的队列权重继续由 Scheduler Core 拥有，Indexing Core 的 Schema 变更分类继续由 Indexing Core 拥有。

| 现有制品内的目标包 | 类 |
|---|---|
| `org.pangrova.execution.api.enums` | `TaskPurpose` |
| `org.pangrova.generation.api.constants` | `GenerationOutputTypeIds` |
| `org.pangrova.generation.api.enums` | `GenerationEmptyPartitionPolicy`, `GenerationExecutionKind`, `GenerationJobState`, `GenerationOutputScope`, `GenerationPageKind` |
| `org.pangrova.generation.api.exception` | `GenerationExecutionOwnerLostException` |
| `org.pangrova.indexing.api.enums` | `DistanceMetric`, `IndexState`, `IndexType` |
| `org.pangrova.indexing.api.exception` | `IndexAlreadyExistsException` |
| `org.pangrova.indexing.enums` | `SchemaChangeType` |
| `org.pangrova.materialization.api.enums` | `DependencyKind`, `MaterializationAction`, `MaterializationEventType`, `MaterializationKind`, `PinState`, `RefreshMode`, `ScheduleState`, `SecurityMode`, `TriggerKind` |
| `org.pangrova.materialization.api.exception` | `MaterializationException` |
| `org.pangrova.scheduler.api.enums` | `AdmissionVerdict`, `ControlLoopAction`, `ErrorCategory`, `FaultTolerancePolicy`, `HealthState`, `NativeFaultPolicy` |
| `org.pangrova.scheduler.api.exception` | `CircuitBreakerOpenException`, `NoAvailableWorkerException`, `TaskDispatchException` |
| `org.pangrova.scheduler.enums` | `QueueType` |

原 `RefreshMode.accepts(RefreshKind)` 将枚举指回业务定义包。改由 `RefreshKind.satisfies(RefreshMode)` 执行相同的全量/增量判断。原 `MaterializationException.notFound(MaterializationId)` 接收业务对象却只使用其 canonicalName，现由调用方取出 canonicalName，异常包依赖基础错误类型。TaskDispatchException 使用同制品的 ErrorCategory 枚举。检查这五个领域中上述包的源码，只有这条 exception → enums 的同领域生产导入。

StrategyId 的具名值、IndexSerializer 的 wire 版本、RetryBudget 和 SpeculationConfig 的字段默认值由拥有其校验或编码语义的类型保留。RefreshKind 的嵌套单例枚举实现 sealed interface，保持原类型层级；包内及方法级辅助错误保留限定可见性。独立 JDK 17 编译涵盖这五个领域的 337 个生产源文件及相关存储 API；JNI、Proto、库中均未发现迁移类型的硬编码 native 类名。角色回归和最终发行旧类检查待 clean 构建。

### Coordination、Server、Transport、Client、Operations、Standalone 与 Testing 公共类型

| 制品内目标包 | 类型 |
|---|---|
| `org.pangrova.coordination.api.enums` | `WorkerHeartbeatStatus`, `CoordinatorRole` |
| `org.pangrova.coordinator.exception` | `NativeDatasetControlRpcException`, `UnregisteredWorkerException` |
| `org.pangrova.operations.enums` | `JfrState` |
| `org.pangrova.operations.exception` | `DiagnosticException` |
| `org.pangrova.server.coordinator.enums` | `AuditWriterConfig`, `CoordinatorRole` |
| `org.pangrova.testing.integration.enums` | `FaultType` |
| `org.pangrova.testing.integration.exception` | `FaultInjectionException`, `SqlExecutionException` |
| `org.pangrova.transport.client.exception` | `TransportConnectionException`, `TransportException`, `TransportServerException` |
| `org.pangrova.transport.flight.exception` | `TicketValidationException` |

两个 CoordinatorRole 分别拥有选主状态和进程配置，保留各自语义和制品。所有迁移的顶层值和异常不反向依赖本制品业务包。SqlExecutionException 保存 runner 提供的错误文本，保留原有 SQL 与消息；audit 枚举的外部日志/JDBC 类型此前仅作 Javadoc 引用，现由装配包导入具体实现。CLI 的包私有 QueryTimeoutException 和 ResultDecodeException 仅被 CLI 内部调用，保留包私有可见性。Standalone 无公共候选。TestObservability 的四个静态字段持有测试 tracing、logging、metrics 实例与过滤器，按测试 fixture 处置。配置类的默认值仍由同一校验类型拥有。

三进程 `IndependentLocalProcesses` 分别使用冻结的 Coordinator/Worker classpath，两个 Worker 各自拥有 PID、HTTP/Flight 端点与 Native 运行时。SHA-256 清单包含角色清单本身、相应角色 JAR 以及原有 flat/CLI inventory。SQL、CLI 与故障注入共享这一装配。此套件包含并行的真实子 JVM；与持有同一 Native 运行时、测试库或存储容器的其他套件串行执行。既有 `PhysicalTwoWorkerShuffleE2ETest` 则将一个 Worker 和测试 Coordinator 放在一个 JVM 内，第二个 Worker 在独立 JVM 中。

验证边界：独立 JDK 17 编译迁移的公共类、冻结制品核对、三进程装配以及 SQL 异常消费端，最终统一回归核验真实子进程和资源释放。

## Security、AI、Governance 的授权入口与事务

下表的旧类名以 `b51114d0` 为基线。所有包名均省略 `org.pangrova.` 前缀。测试列列出可单独复现该行的断言，交付验证记录区分实际运行与源码核查。

| 旧类及处置 | 现有制品与生产类 | 调用、权威事实与资源 | 对应测试 |
|---|---|---|---|
| `coordinator.http.endpoint.AuthenticationFilter` 移动，`security.authn.AuthenticationManager` 移动并保留认证链 | `server-coordinator` 的 `server.coordinator.http.endpoint.AuthenticationFilter`、`security-core` 的 `security.core.authentication.AuthenticationManager` | Jersey 过滤器区分公开健康检查、内部路由和用户请求。外部请求经认证后设置当前请求的身份属性。内部路由要求显式内部令牌。Server 的 `SecurityBootstrap` 核对工厂与实例的 bypass 声明，创建失败回收实例，结束时关闭 manager 和认证器。 | `AuthenticationFilterInternalTokenIsolationTest`、`AuthenticatorFactoryDiscoveryTest`、`SignedOidcSecurityBootstrapIntegrationTest`、`LdapAuthenticatorTest` |
| `security.authz.DynamicRbacAccessControl` 移至策略实现。授权变更与重放拆出专门的持久化端口 | `security-policy-builtin` 的 `security.policy.builtin.authorization.DynamicRbacAccessControl`、`persistence-security-postgresql` 的 `persistence.security.postgresql.grant.JdbcPrivilegeMutationRepository` | Server 启动时读取角色和授权检查后端可用性。查询检查读取当前角色、权限、行策略和列掩码。管理 SQL 的 GRANT/REVOKE 进入仓储拥有的事务，锁后检查当前执行者权限，授权行与请求回执同一次提交，调用方可用请求身份确认重放。 | `PrivilegeSqlExecutionTest`、`JdbcPrivilegeMutationRepositoryTest` |
| `worker.ai.governance.ConfigurableAiInvocationAuthorizer`、`worker.ai.context.GovernedAiBindings`、`worker.ai.invocation.AiInvocationService` 移到 Worker 侧 AI 运行时 | `ai-worker` 的 `ai.worker.governance.ConfigurableAiInvocationAuthorizer`、`ai.worker.context.GovernedAiBindings`、`ai.worker.invocation.AiInvocationService`、`AiTaskBudget` | `AiFunctionFactory` 安装调用授权器。未配置时默认拒绝，启用内置 authenticated 模式需要模型、能力与权限白名单。实际回调按已批准的 Task/Attempt 与 OperatorBinding 核对调用，预算登记和取消属于任务生命周期。配置授权器和任务绑定承担不同检查，二者均在出站调用之前执行。 | `ConfigurableAiInvocationAuthorizerTest`、`GovernedProviderTest`、`AiPhysicalInvocationAdmissionTest` |
| 新增发布时模型当前权限协作接口与 PostgreSQL 实现 | `persistence-ai-postgresql` 的 `persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority`、`persistence-workflow-postgresql` 的 `persistence.workflow.postgresql.generation.PostgresGenerationTransactionAuthority` | Coordinator 注入同连接直接角色读取器。Generation 发布工作流先取得授权锁，在同一请求事务中重读租户、表和模型权限，并比较模型 incarnation、revision 与部署签名。模型读取器借用连接，事务的关闭与提交属于上层工作流。 | `ModelRevocationAuthorityIntegrationTest`、`QueryOnlyWriteAuthorityIntegrationTest` |
| `metadata.service.MetadataTagOperations` 拆分；标签操作进入 Governance，列解析保留在 Metadata | `governance-core` 的 `governance.core.classification.ColumnTagBindings`、`persistence-table-postgresql` 的 `persistence.table.postgresql.governancebinding.JdbcTableControlGovernanceBindingStore` | Metadata 解析实际列与当前表身份。Governance 验证 PII 类型，通过 Table 的策略端口把绑定持久化到稳定表 UUID，再通知标签投影。重放检查当前 catalog，表改名保留原身份。 | `MetadataTagOperationsTest` |
| `governance.quality.QualityCheckService` 删除，`governance.quality.rule.GovernanceStatementHandler` 移动，抽取本域共享的结果校验 | `governance-core` 的 `governance.core.quality.GovernanceStatementHandler`、`QualityRuleEvaluation` 与 `RuleBasedWriteQualityGate`，`governance-quality-sql` 的 `governance.quality.executor.sql.SqlQualityRuleExecutor` | 定时与程序化入口从规则仓储取得规则；执行端把结果绑定到规则 ID、表 UUID、严重性及行数，写前入口按规则严重性处理执行失败。SQL 读取请求携带表身份，执行身份显式携带主体与租户。当前生产定时 SQL 读取缺少可重验的持久主体，写前缺少候选提交数据的可见视图，这两项维持不可执行状态。 | `GovernanceStatementHandlerTest`、`WriteQualityGateTest`、`QualityPredicateSqlIntegrationTest`、`QualityAuthorityMigrationIntegrationTest` |

2026-11-14 对表列 16 个测试类进行 JDK 17 源码编译，独立启动 JUnit 与 PostgreSQL，109 项通过、0 项跳过。权限变更故障包括执行中丢失提交确认、审计写失败、第三方越权撤销、按授权选项级联撤销。模型测试实际删除和重建角色后复验延期授权；Worker 回测覆盖缺失 Native 上下文、冒用另一个 operator 的身份、取消时唤醒等拒绝路径。OIDC 使用测试签发者签名并核对受众和服务中断。LDAP 的绑定与错误通过测试目录上下文验证，尚未执行真实 LDAP 服务器与生产证书链的互操作测试。上述 Worker 授权测试没有向商业 AI 厂商发起请求。定时 SQL 与写入后视图的生产支持状态见上表。

## 索引持久化端口与垃圾回收保护

同一 PostgreSQL 仓储提供九组操作。描述符缓存和卡住的构建恢复使用 `IndexDescriptors`，构建输出校验使用 `IndexBuildGenerations`，映射写入使用 `IndexFileMappings`，代际发布使用 `IndexPublications`。提交日志轮询器使用 `IndexCommitLog`；共享熔断状态使用 `IndexBreakerStates`。`IndexCleanup`、`IndexCacheEvents` 和 `IndexFileStatistics` 分别定义清理、跨进程失效和统计操作。需要跨多个能力的消费者和 Server 通过 `IndexMetadataStore` 注入同一仓储实例。数据库连接、发布事务、映射写入的 claim 锁仍由原 PostgreSQL 实现持有。

未实现的持久化操作显式拒绝调用。带创建时间保护的清理候选只包含早于 cutoff 的映射。按状态分页按更新时间、索引 ID 排序。`IndexPersistenceGuaranteesTest` 覆盖缺失实现和清理时间边界，独立契约加载由 `IndexPersistenceApiIsolationTest` 检查。


## 2026-11-14 macOS 双 Worker SQL 采样

在同一台 macOS arm64 主机上，用 JDK 17 和各自从源码构建的 Release JNI 比较两份冻结的发行清单。历史 Java 源为提交 `ec51db67b183fd8af2495c4ab099b4837c6e7ee9`，本轮 Java 源为工作树摘要 `69b7aea5dd0fc63b6cd48d1fd9d5ee7ed288038cc8839517c4385ed997ecae3e`。后续的索引文件指纹变更在此次采样之后。历史版采用该提交 Dockerfile 中实际存在的制品及其运行依赖。其清单还引用了该提交中已删除的 `pangrova-file-worker-adapter`，测试依照当时的 File reactor 装配。

按历史版、本轮版、本轮版、历史版的顺序分别启动全新的 Coordinator、两个独立 Worker、PostgreSQL 与 LDAP。每轮在 Iceberg JDBC Catalog 中写入三个数据文件，每个文件的三行依次为 4、5、5。交替执行常量 SQL 和全表求和，各预热三次、采样十二次。96 个正式查询均有独立 query ID，并通过公开 CLI 验证单行结果为 42、查询终态为 FINISHED、结果已消费且采用 gRPC TLS。计时从启动 CLI 子进程前开始，覆盖 CLI 完成和父进程的查询状态校验。

| 工作负载 | 每版样本 | 历史版中位 / P95（毫秒） | 本轮版中位 / P95（毫秒） |
|---|---:|---:|---:|
| `SELECT 21 + 21` | 24 | 3994.75 / 7686.60 | 3527.89 / 5314.47 |
| `SELECT SUM(id)` | 24 | 5065.55 / 12416.22 | 4651.81 / 6293.05 |

从首个正式样本完成后按秒采集各子进程 RSS。历史版与本轮版最大采样值分别为 Coordinator 389.3 / 393.0 MiB、单 Worker 187.5 / 196.0 MiB、单 CLI 135.8 / 141.5 MiB。RSS 包含堆、Native 及共享映射。本次未独立测量 JNI 拷贝量、网络字节或等待队列时长。后台同时运行 Linux Native 编译和 PostgreSQL 测试，时间分布仅描述该共享主机上的两份冻结清单。

历史版与本轮的运行清单 SHA-256 分别为 `ccec52b0a3cea4c7b38559f65d0c5b14d969c4c3658753e3c0081159667ef46d`、`b3788bd0c2df8c08f23b3389ace39b3b11058595553a5418e435cc2306a8ee11`，Release JNI 摘要分别为 `6cfa0b80749bcb0d322948d394bbd9aebe36f843f5c7071c903a19bc00f0682f`、`75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929`。

[复现命令与各轮分布](../testing/speculative-process-e2e.md#measuring-public-sql-latency-across-two-builds) · [原始查询记录、RSS 与摘要](../../pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-process)

### 双 Worker 资源、回收与 Shuffle 背压补测

资源对照沿用上述 macOS 机器、两个源码版本、发行清单、JNI 和三文件 Iceberg 数据。四轮按历史、本轮、本轮、历史的顺序启动。每轮各条 SQL 先预热一次，再测三次，共 24 个正式查询。客户端已取回 42 后，采样程序等待同一查询观测到的所有 Worker 任务都返回 Native owner-released 回执，并向 PostgreSQL 核对租户并发槽为零，再接受样本。四轮均正常关闭进程、连接及容器。下表的延迟是从客户端校验结束到观察到上述条件的中位数，含读取数据库的开销。

| SQL / 版本 | 任务数/查询 | 最大 Velox 算子内存预留（字节） | 释放观察延迟 ms | Worker 状态 HTTP 请求体 / 响应体总字节（每查询） |
|---|---:|---:|---:|---:|
| 常量 / 历史 | 1 | 9,280 | 73.7 | 4,102–4,105 / 213 |
| 常量 / 本轮 | 1 | 9,280 | 138.0 | 4,104–4,110 / 213 |
| Iceberg SUM / 历史 | 4 | 75,648 | 148.6 | 16,316–16,363 / 639 |
| Iceberg SUM / 本轮 | 4 | 75,648 | 126.0 | 16,332–16,350 / 639 |

60 个实际任务的最终统计均可读取，Velox 报告的 consumer wait 均为 0 毫秒，spill 均为 0 字节。两种 SQL 产生的单行结果没有形成可观测的毫秒级阻塞。请求体和响应体由 HTTP relay 对收发字节计数，口径限于任务状态端点。

通过 macOS libproc 采集同一角色 JVM 的 RSS、线程、打开的描述符和 socket 描述符，通过 JDK 17 的 `jstat -gc` 独立采集堆使用量。共保存 309 条 OS 观测。比较窗口为各轮第一个正式查询结束至最后一个正式查询完成前，排除启动和进程退出，保留其中 90 条带堆数据的采样。下表值为每类单个进程各指标分别观测到的最大值，线程和 socket 数含服务进程自身的后台工作。

| 版本 / 角色 | 采样数 | 堆 MiB | RSS MiB | 线程数 | 文件描述符数 | Socket 描述符数 |
|---|---:|---:|---:|---:|---:|---:|
| 历史 / Coordinator | 14 | 201.2 | 348.6 | 110 | 546 | 19 |
| 本轮 / Coordinator | 16 | 213.8 | 257.1 | 109 | 442 | 17 |
| 历史 / 单 Worker | 28 | 96.3 | 179.3 | 115 | 533 | 16 |
| 本轮 / 单 Worker | 32 | 95.2 | 188.8 | 123 | 377 | 15 |

另开两轮，通过 JVMTI NativeMethodBind 在实际任务状态、统计和释放 JNI 方法返回时读取所复制的数组长度。两轮均建相同表并执行两个常量查询及两个 Iceberg 求和，两个 Worker 的 PID 均与角色日志核对。历史版的释放方法 14 次 / 6,224 字节、状态方法 186 次 / 85,680 字节；本轮释放方法 14 次 / 6,232 字节、状态方法 318 次 / 127,521 字节。两轮均未调用独立统计方法。轮询次数受进程停留时间影响。该观测覆盖 Worker 从启动到退出期间三个 Native 方法的返回拷贝，其他 JNI 接口、进程总 Native 内存和整机网络线路流量未计量。

另对同版本生产 `ShuffleBufferStore` 建立可重复的慢读者负载。容量为一个记录，驻留预算 0 字节，第一个批次占用 65,536 字节和一个实际 spill 文件。第二个生产者调用 store，消费方至少 40 毫秒后释放第一个记录。历史版 12/12 次返回容量错误，调用时长中位 0.012 毫秒；本轮版 12/12 次等待到消费者归还额度，完整重放第二份 65,536 字节，中位 44.12 毫秒，范围 40.96–45.75 毫秒。24 次运行结束后记录数、spill 字节和文件数均归零。这组测量复用相同 C++ 工具链与 SDK，并分别编译两版本的真实存储源和头文件。它单独覆盖存储层背压，SQL 测量覆盖包含客户端的完整路径。

四轮 SQL、两轮 JNI、存储层对照各自使用独立进程。同期运行的 Linux Native 编译以及部分重叠的其他隔离 SQL 进程都会影响时序。这批资源样本用于界定每条路径的成本和释放行为。历史查询延迟、远端 Shuffle 吞吐与本批数据分别保留原采样口径。[原始 CSV](../../pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-resources) · [复现命令](../testing/speculative-process-e2e.md#measuring-public-sql-latency-across-two-builds) · [远端 Shuffle 吞吐](../testing/shuffle-read-comparison.md)

### 双 Worker 的 JDBC 与 CLI 取消

2026-11-14 在 macOS arm64、JDK 17 上使用提交 `32b3d02b36b205bf958ccda9771589c27eb595d2` 的同批发行物运行 `IndependentClientCancellationE2ETest`。一个 Coordinator、两个独立 Native Worker、PostgreSQL 和 LDAP 使用真实网络端口。发行清单 SHA-256 为 `66cc08c640c9d5d0619b52f7cf2705a391f99e4be61feaf26937ccb311ce2a41`，Native 为 `1197e854373d45f6ee137cd6c495fba67bdc26252517b6014c3c497f74c22392`。JDBC 运行 classpath 的 22 个 class 和服务描述符条目逐个与冻结 JAR 核对一致。首次运行揭示前一个查询重发的任务报告也会命中测试屏障。改为排除已观测的查询，并在公开 HTTP 确认相同查询已取消后放行回报，复测一项通过、零跳过，耗时 100.8 秒。两个场景分别由 JDBC `Statement.cancel()` 和打包 CLI 的 15 秒期限触发；随后同一 JDBC 连接和新 CLI 进程都用 gRPC TLS 取回结果 42。JDBC 会将收到的 Arrow 数据缓冲后创建 ResultSet。运行方法见[独立进程测试](../contributing/testing.md)。

后续 JDBC 生命周期修复覆盖了服务器已创建查询、客户端尚未收到提交响应时的取消。每个 Statement 发布本次请求的查询 ID，Connection 关闭时先取消并关闭它拥有的 Statement，再释放 HTTP 客户端。独立 Maven 运行的 JDBC 套件 241 项全部通过。Coordinator 的查询、DAG 和推测尝试共用一个精确取消重试队列。断开实际 HTTP 连接、验证相同尝试的释放回执并回收队列的 `TaskCancelRetrierTest` 套件通过 11 项。`TaskIdentityRegistryDrainContractTest` 通过 11 项，覆盖退休读取尝试的迟到结果通知、真正的 Native 释放回执以及错误权限或携带写入产物时的拒绝。以上运行均为零跳过。

在上述 `32b3d02b` 的 macOS 发行物副本中，使用同一源码树加本批取消和回执修订对 11 个源码单元重编，更新 Protocol、Coordination、Server Coordinator 和 Worker 四个 JAR。新角色清单为 `780f9d7734b48d25dd3c331bad915024c44513a4919924551804cbbd500622e3`，Native 沿用上述哈希。独立进程 E2E 延迟首次 FINISHED 直至同一 Worker 的实际重发抵达，检查 PostgreSQL 的租户并发槽由占用变为空闲，随后交付迟到的报文。Coordinator 对该未释放通知返回 `RETIRED_RESULT_NOTICE`，对真实 Native 释放回执返回 `CONTINUE`。JDBC 和打包 CLI 恢复查询及 TLS 读取均通过，套件一项、零跳过，耗时 123.9 秒。此前同时拦截所有重试的测试使仍持有 dispatch 操作的查询无法在屏障打开前退休，已改为仅延迟原始报文。这是对四个变更 JAR 的针对性验证，最终整仓发行仍需重新构建并核对清单。

取消审计的复测在上一次角色清单上更新了 Query 制品的 `QueryAuditEmitter`，移除 Coordination 中的旧类，并重编 Dispatcher 及调用其工厂的 QueryManager。新清单 SHA-256 为 `9cb36ae9e40843933bfc3faa0db1f9b994b315c5602b28c5c0c2c749489b0362`，Native 哈希沿用上述值。同一独立进程测试通过一项，零跳过，耗时 108.2 秒。JDBC 与 CLI 各自按本次实际 query ID 查询 PostgreSQL，校验历史记录的原 SQL、LDAP 用户、解析后的租户、完成时间及失败标志，并核对安全日志中提交与取消各一次、顺序一致。首次断言要求提交审计也带租户，实测提交早于准入阶段的租户解析。复测分别验证提交时已认证的用户与 SQL，以及终态时已经解析的租户与取消结果。 随后使用真实 Arrow IPC 的 UUID ResultSet 在土耳其语默认 Locale 下验证按列名读取，列名 `ID` 和 `iD` 均读出原值。独立 JDBC 套件 242 项通过，零跳过。

## 2026-11-14 macOS 发行角色实物核验

Corretto 17 在 Apple M2 上完成 149 个 reactor 模块的安装后，按 Maven 运行依赖顺序冻结 Coordinator 的 363 个 JAR、Worker 的 309 个 JAR 和 Standalone 的 421 个 JAR。三个独立 JVM 使用生产启动器选择各自的 classpath。Coordinator 加载了 `SqlCompiler`，加载 WorkerMain 和 `NativeRuntime` 时返回 ClassNotFoundException。Worker 加载了 WorkerMain 和 `NativeRuntime`，加载 CoordinatorMain 和 `SqlCompiler` 时返回 ClassNotFoundException。Standalone 加载了这四个类型及自己的 main 方法。每个角色中的 Pangrova 主制品均与本批 target 中对应 JAR 逐字节匹配。

Worker 与 Standalone 中的两份 Native 资源分别来自 Worker JAR 和平台 classifier，两者都与本批 Release dylib 的 SHA-256 `75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929` 相同。包含 1,096 条记录的 runtime.sha256 的摘要为 `4d052d60150412c320eae6b97f8d3f1e9f0716ad4360cc805dc55b2c3e69aed3`。本次验证覆盖 macOS 角色制品、类加载与 Native 字节一致性。[角色冻结与复查命令](../testing/speculative-process-e2e.md)

## CDC 与 Metadata 失效链的数据库验证

`CdcCheckpointDatabase` 已删除。`CdcPipelineExactlyOnceTest` 与 `TwoPhaseCommitCoordinatorTest` 每类持有独立 PostgreSQL schema，使用正式迁移初始化，每个用例清空该类的 checkpoint 表。旧行故障用例先把隔离 schema 初始化到 V72，再写入代表行和应用正式 V73，以 PostgreSQL 错误码 23514 验证新坏行遭到拒绝。`CdcEosChaosIntegrationTest` 由实际 Coordinator 与 JDBC Store 持有 PREPARED、提交和恢复状态，Toxiproxy 中断数据库连接，测试 sink 独立记录未发布的 attempt。JDK 17、PostgreSQL 16 和 Toxiproxy 2.12.0 实跑这三类共 44 项，全部通过。

`MetadataSyncPropagationE2ETest`、`MultiCoordinatorArchE2ETest` 和 `MultiCoordinatorRoleRoutingE2ETest` 借用 SQL 测试集群的 PostgreSQL，每类初始化独立 schema，正式 V42 提供提交顺序触发器。跨连接用例将同一表放入读取侧缓存，提交失效消息，等待独立 poller 真正驱逐其缓存项。三类在 macOS arm64 上使用 JNI、HTTP SQL、Iceberg candidate 和真实模型 fixture 实跑 35 项，全部通过。随后将其中一个直接写失效日志的用例替换为生产投影事务的重放测试并单独通过，核对持久化表、回执和一条失效记录。两个 Coordinator 的缓存验证使用独立连接和 poller，共享测试进程。

## 查询注册、任务身份和物化命令

| 原入口 | 处置与调用路径 | 状态、资源及验证 |
|---|---|---|
| `coordinator.query.QueryManager` 提交、恢复、清理与 drain | 提交和恢复均先向 `query.core.lifecycle.QueryRegistry` 预留查询 ID，authority 初始化成功后发布该实例。保留策略通过同一注册表移除过期查询。 | 注册失败释放预留；已发布的查询在物理清理完成前占用 drain 计数；同一状态机只释放一次。`QueryRegistryTest` 覆盖初始化中遇到重复提交、读取半初始化实例、初始化失败后重试和重复完成。 |
| `coordinator.scheduler.RemoteTaskDispatcher.bindControlIdentity` | 任务及 Shuffle 身份绑定移至 `execution.core.binding.TaskControlBinding`。Dispatcher 继续负责发送前检查当前 authority、Worker 路由、HTTP 超时和重试。 | 已绑定的任务和 Shuffle 必须匹配同一 plan epoch 与 execution owner。封装、冲突拒绝、epoch 追赶、COPY TLS 和 HTTP 测试共 24/24 通过。 |
| `materialization.usecase.MaterializationService` | 定义计划和重放验证归 `MaterializationDefinitionPlanning`，CAS 调度更新归 `MaterializationSchedules`，级联删除归 `MaterializationRemoval`。命令仍使用同一 repository，先校验全部待删除对象的管理权，再逐对象更新。 | CAS 重试、权限、source pin、定义重放、refresh、发布后回收及恢复相关测试共 66/66 通过。 |
| `server.coordinator.bootstrap.ModuleIntegrationBootstrap` 维护主节点回调 | 选举启动中任一步失败，撤销该任期并逆序停止已安装服务；失主时继续尝试每个停止动作。 | `ModuleInvalidationLifecycleTest` 注入调度器拒绝，检查下一任期能重新启动，重复通知不积累任务。 |

查询注册、终态、读 pin、恢复、索引持久化保证及清理边界的联合验证为 77/77，测试使用 JDK 17 和 PostgreSQL。该数字不包含角色发行物的独立进程验收。

## Worker 任务注册、取消和 Catalog 操作作用域

| 基线职责 | 生产入口和当前类 | 事实、资源及故障语义 |
|---|---|---|
| `worker.task.TaskManager` 中的任务表及本地 ID 计数 | Worker HTTP → `TaskManager` → `TaskSubmissionHandler` → `WorkerTaskRegistry` | 注册表独占本地资源 ID 和对应的 `TaskInfo`，并发发布在同一临界区检查容量。已取得资源但回滚失败的记录可超过普通准入限额，保留至实际清理完成。精确尝试的准入/重放回执另由 `TaskAdmissionRegistry` 持有，可在资源释放后继续查询。 |
| `TaskSubmissionHandler` 的取消与回收 | 精确 seal、任务退役与 Worker 内部取消经 `WorkerTaskCancellation` 进入现有 `TaskLifecycleManager` | 继续由同一个 `TaskInfo` 进行终态 CAS。Native 拒绝取消则传播失败；同一查询的其他任务仍能收到取消，失败尝试保留在查询索引中供重试。收到取消仅代表请求被接受，完整退出还需要 Native 回执。 |
| drain 的全局倒数锁存器 | `WorkerShutdownSequence` → `TaskManager` → `TaskDrainExecutor` | 拒绝新记录后按精确本地 ID 等待终态。重复通知、同时 drain、已移除的终态任务均按实际任务状态处理。达到超时或等待线程被中断时对仍活跃的尝试强制取消，保留中断标记。 |
| `TaskRetryExecutor`、`TaskRetryConfig`、`TransientFailureClassifier` | 退役 Worker 启动时创建并记录为已启用的无调用方对象及其孤立单测。真实的故障重新调度由 Coordinator 的 `QueryBootstrap`、Scheduler 的 `SpeculativeTaskLauncher.retryFailedTask` 和 `TaskRetryAdmission` 协作 | 新 attempt 的分配和预算保留在控制面。Worker 报告实际失败，不复用原 attempt 重新发送写入。 |
| `worker.catalog.WorkerCatalogRuntime` 的版本缓存与任务输出验证 | `WorkerCatalogRegistry` 持有 tenant、catalog、incarnation、revision、fingerprint 对应的 connector，`WorkerCatalogRuntime` 绑定任务输出，`CatalogControlLease` 限定控制操作的使用期 | 注册表负责单次初始化、LRU、租约计数与关闭失败后的重试。热缓存入口校验 SHA-256 算法与摘要长度；控制操作还要求 output 内嵌的 deployment 与请求完全相同。每个任务持有自己的 session 与经共享 codec 验证的输出句柄。Native 写调用由 `NativeWriteAdmission` 防止 release 与当前调用并发。 |

2026-11-14 使用 JDK 17 在独立输出目录编译 Worker 家族和集成调用方 484 个源码单元。任务提交、JNI、内存/结果释放、取消和准入组 82 项通过。随后分别运行包含 drain 竞态的 33 项、包含真实 Native 取消拒绝后重试的 33 项，均通过，两个重跑组与前组存在重叠。真实认证 Coordinator、Paimon/Lance 与 JNI Catalog 生命周期组另有 9 项通过，覆盖 1024 个活跃租约满载、LRU、并发创建、关闭与部署拉取竞争、错误指纹算法和跨部署输出拒绝。以上结果来自 macOS arm64。

### 标量索引剪枝和 CDC 水位

| 原职责 | 当前归属和调用路径 | 边界与验证 |
|---|---|---|
| Coordinator 的 `FilterPredicateExtractor` | Planner 追溯 Filter 输入的列序号并解析 Calcite 谓词。直接投影可改名和重新排序。无法证明向下推安全的计算、聚合、窗口和 LIMIT 会终止该条件的下推。 | 通过真实 Calcite 计划验证同名遮蔽、表达式列和 LIMIT 前后谓词。动态 IN 成员不能丢失。 |
| Coordinator 的 `IndexSplitFilter` 和 `filter` 包 | `indexing-core` 的 `core.pruning` 拥有分片剪枝、已发布代际的选择、L1 并发和 L2 提示。Coordinator 借用绑定到当前会话的 Catalog 查询能力。Server 选择过滤器和持久化实现。 | PostgreSQL 在同一 REPEATABLE READ 事务中读取活动 manifest 和映射。跨两批读取之间发布新代际的测试检查原批次一致、后续请求可见新发布。确切查询 snapshot 与 manifest 比较，可复用来源较早的不可变文件。 |
| `IndexHint` 和 `IndexPredicateAnalyzer` | 每个提示携带分析时读取的代际。去重使用完整的索引、代际、类型和谓词等值关系。 | 查询入口通过发布端口读取确切代际。新建、未发布、已取代和 snapshot 冲突分别核对；缺少合格提示时保留原分片。该次读取不授予清理租约。 |
| `WatermarkTracker`、`CdcPipelineLoop` | Pipeline 在恢复时加载已提交水位。Tracker 的每个本地观察实例拥有水位、最近事件和状态；持久化端口拥有单调位置。 | 晚到的旧写入完成后只能更新已分离的观察实例。恢复不计为新事件。水位、管道、调度和 PostgreSQL 等相关测试 30 项通过。 |

## 表创建、标签投影和运行时命名

| 入口 | 责任和验证 |
|---|---|
| SQL 和 Metadata 的 CREATE TABLE | 两个入口向 TableRegistrations 传递实际执行 CREATE 的 Catalog handle 所属部署。登记失败只能在该部署记录孤立对象。目录被重新创建、表名被另一个身份占用或发生待完成改名时，都保留当前身份。PostgreSQL 在同一事务退休旧记录、插入新 UUID、更新名称并写失效事件。用持久化后的 revision 作为并发令牌，保留数据库时间精度。六组测试共 127 项通过，包括强制 INSERT 冲突后的回滚与外部 CREATE 期间 Catalog 替换。 |
| Metadata 列标签与 Governance | ColumnTagBindings 在构造时绑定表策略仓储与事件源。Coordinator 使用同一事件源安装监听器并重放已有标签。治理关闭时移除自身注册的监听器。Paimon 联测使用同一个 PostgreSQL 权威构造控制记录、标签、遮蔽和元数据读取。 |
| OpenTelemetry、Worker HTTP、任务状态与维护血缘 | 两份重复的 trace/span ID 读取合并到 CurrentSpanIds；Worker 访问日志读取当前 span，Native 执行使用提交时捕获的执行身份。TaskReportCompletion 将已接受的报告路由到逻辑任务。MaintenanceLineageListener 从元数据事件产生血缘通知。PhysicalTypeProjectionDeclaration 校验 Substrait 物理类型的投影规则。 |

## File 授权与 Worker 操作审计

| 原入口 | 真实职责与处置 |
|---|---|
| `file.api.audit.FileOperationAudit.fileOperation` 和 `coordinator.query.audit.FileSpaceAuditEmitter.fileOperation` | 原调用发生在签票之后。FileGrantAudit 通过安全域 AuditEventWriter 发出 FILE_ACCESS_GRANTED，FileOperation 明确区分 HEAD、LIST、READ、WRITE、DELETE。未识别的输入在签票或 Worker 解码时拒绝。 |
| `FileSpaceAuditEmitter.Mutation` | 此枚举仅选择审计事件，现已删除。SQL 入口在真实成功的 namespace 操作后按 Volume 或 Stage 明确选择事件。文件属性变更继续使用 file.api.space.FileSpaceMutation 的既有 sealed 操作及 PostgreSQL 原子更新。 |
| `worker.file.FileSpaceFileResource` | POST head 和 HTTP HEAD 产生 FILE_METADATA_READ；LIST 按页确认，PUT 和 multipart complete 于 provider 返回后确认，GET 在请求字节数匹配、写出、flush 和关闭源流后确认。DELETE 要求 provider 成功返回。认证或版本失败、取消和部分流写出不产生成功事件。对象存储不与审计 sink 共用事务。 |
| `server.worker` 的独立及嵌入入口 | 将现有 LoggingAuditEventWriter 传给 Worker bootstrap。Worker 只持有 Security API 的 writer 并借用它，保持 Worker 与 Coordinator、JDBC 解耦。生产结构化日志为 Worker 侧完成证据；Coordinator 使用其既有日志或组合后端。 |

以发布提交 7a374091f 为父提交，在独立检出中使用 JDK 17、Maven 3.9.16 和 Python 3.12 生成协议并编译 143 项 reactor，14 个 File、Worker、SQL 和 HTTP 测试类的 103 项测试全部通过，无跳过。PostgreSQL 测例核对 6 次签票和 7 次实际操作，覆盖同一 READ 票的 HEAD、Range GET、客户端写失败及条件删除失败。MinIO 测例核对 multipart 完成与跨 Worker 恢复。

## Catalog DDL 与查询资源

| 能力 | 实际入口、权威与验证 |
|---|---|
| Catalog 配置与授权 | SQL 的 CREATE、DROP、ALTER、DESCRIBE 由同一组任务注册。访问检查要求当前身份，ALTER 使用专用 CATALOG grant。ALTER 在发布前创建并验证候选 Connector，然后以 PostgreSQL 的 incarnation 和 predecessor revision 提交 current、history 和 invalidation。未确认提交保留持久化事实，节点可重新对账。DESCRIBE 只返回名称、类别、Provider、状态、revision。CREATE/ALTER 的 SQL 报告文本在生成查询事件、审计、lineage 与 journal 前去除属性。 |
| Catalog 资源 | QueryConnectorAuthority 单次取得实际 metadata 及其 CatalogHandle。提前规划、增量扫描和 metadata query scope 共用这一版本，持续到该查询 drain 和 cleanup 完成。旧 revision 在所有读者释放后关闭，释放失败可以重试。对同一 incarnation 的提交监听器在替换前登记到新 handle。 |

本批以 f522eceab 为父提交独立编译 143 项 reactor，7 类 66 项测试全部通过。CatalogLifecycleTest 另在真实 Coordinator、Worker、PostgreSQL 和 Iceberg 上通过 6 项，包括 CREATE/DROP、ALTER/DESCRIBE、无授权拒绝、输出脱敏和历史 revision；两组均无跳过。

## 连接分支选择与会话赋值

`USE BRANCH` 验证当前 SELECT 授权、tenant、表和可读分支后返回 `branch_selection`。JDBC/CLI 共享的 HTTP Client 按连接维护选择。下一次查询根据完全限定的表名再次授权、解析真实分支头并固定 snapshot。MAIN 清除当前表的选择，显式 VERSION AS OF 优先。删除了 Planner 中独立的 AST 改写器及其进程内 branch map。

从连接发起的通用 SQL SET 请求使用 PARSER_UNSUPPORTED_FEATURE 明确拒绝，SQL 报告和审计不包含赋值内容。

以 a407b3938 为父提交独立构建，143 项 reactor 编译通过，协议、客户端、授权、解析、路由和 HTTP 10 类 96 项测试通过。macOS arm64 真实 SQL 验证为 Iceberg BranchTag 14 项、PaimonTimeTravel 5 项、SessionManagement 5 项，均无跳过。Iceberg 测例在读到冻结分支后撤销真实 PostgreSQL 角色，重新选分支及使用已有回执读取均因缺少 SELECT 被拒绝。Paimon 已创建的具名分支被显式拒绝，MAIN 清除语义由 Iceberg 实际验证。
