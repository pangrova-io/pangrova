# Persistence迁移候选清单

中央Persistence目标包含所有持久化端口与后端实现，按领域和能力组织。下面从当前主源文件名
（Store/Repository/Dao/Registry/Jdbc/Postgres）及store/jdbc包发现候选。注册表、缓存、驱动、
装配工厂和领域适配器也可能命中；命中不等于确认它是持久化实现，未命中也不等于完成审查。
逐文件验收必须继续追踪调用者、实现、装配、事务、SQL/编解码、异常与最近测试。

已归位的File、Coordination、Security、Query、Generation JDBC及Query/Coordination内存后端在Persistence中；
其他领域存储仍待实施。Security保留行过滤/掩码渲染和权限判断，数据库事实读取归JDBC。
内存/文件/仲裁等非JDBC后端须单独确定能力与模块落位，不能混入API或伪称JDBC。

Coordination JDBC 实现必须保留：原 `pangrova-coordination-jdbc` 的
`JdbcDistributedLease`、`JdbcCoordinatorRegistry`、`JdbcWorkerRegistry` 和
`CoordinationStorageException` 已迁入 `pangrova-persistence-jdbc` 的
`org.pangrova.persistence.jdbc.coordination` 包，原有八个测试类一并保留。
Coordinator 的租约、HA 注册和 Worker 注册装配仍直接使用这些实现，默认租约后端仍为
`jdbc`。清理旧模块目录仅移除迁移后留下的空壳，不代表移除 JDBC 后端能力。

后续迁移顺序：Generation领域适配 → Materialization/Table →
Metadata/Catalog/source authority → Indexing/AI/Semantic/Governance/Streaming/Operations →
Worker与其余运行时持久化候选。每批先消除实现反向依赖，再原样迁移SQL与事务语义；不能
通过增加API→实现依赖、取消真实数据库测试或恢复旧包转发来完成搬迁。

## pangrova-ai-control

16 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [AiInfrastructureStores.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/assembly/AiInfrastructureStores.java) | `org.pangrova.ai.control.assembly` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ConcurrentMapEmbeddingModelRegistry.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding/ConcurrentMapEmbeddingModelRegistry.java) | `org.pangrova.ai.control.embedding` | 候选；待逐文件确认职责并迁移/留调用方 |
| [EmbeddingModelRegistry.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding/EmbeddingModelRegistry.java) | `org.pangrova.ai.control.embedding` | 候选；待逐文件确认职责并迁移/留调用方 |
| [EmbeddingRegistryAccessDeniedException.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding/EmbeddingRegistryAccessDeniedException.java) | `org.pangrova.ai.control.embedding` | 候选；待逐文件确认职责并迁移/留调用方 |
| [EvaluationStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/evaluation/EvaluationStore.java) | `org.pangrova.ai.control.evaluation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CatalogExportBuildStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/feature/CatalogExportBuildStore.java) | `org.pangrova.ai.control.feature` | 候选；待逐文件确认职责并迁移/留调用方 |
| [FeatureCatalogStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/feature/FeatureCatalogStore.java) | `org.pangrova.ai.control.feature` | 候选；待逐文件确认职责并迁移/留调用方 |
| [EmbeddingModelRoleAuthority.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/embedding/EmbeddingModelRoleAuthority.java) | `org.pangrova.ai.control.store.jdbc.embedding` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcCurrentModelAuthority.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/embedding/JdbcCurrentModelAuthority.java) | `org.pangrova.ai.control.store.jdbc.embedding` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcEmbeddingModelRegistry.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/embedding/JdbcEmbeddingModelRegistry.java) | `org.pangrova.ai.control.store.jdbc.embedding` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcEvaluationReleaseGate.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/evaluation/JdbcEvaluationReleaseGate.java) | `org.pangrova.ai.control.store.jdbc.evaluation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcEvaluationStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/evaluation/JdbcEvaluationStore.java) | `org.pangrova.ai.control.store.jdbc.evaluation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcFeatureViewEvaluationEvidence.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/evaluation/JdbcFeatureViewEvaluationEvidence.java) | `org.pangrova.ai.control.store.jdbc.evaluation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcCatalogExportBuildStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/feature/JdbcCatalogExportBuildStore.java) | `org.pangrova.ai.control.store.jdbc.feature` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcCatalogExportJobInsertion.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/feature/JdbcCatalogExportJobInsertion.java) | `org.pangrova.ai.control.store.jdbc.feature` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcFeatureCatalogStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/feature/JdbcFeatureCatalogStore.java) | `org.pangrova.ai.control.store.jdbc.feature` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-ai-worker

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [GovernedBlobStore.java](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/blob/GovernedBlobStore.java) | `org.pangrova.ai.worker.blob` | 候选；待逐文件确认职责并迁移/留调用方 |
| [AiModelBudgetRegistry.java](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/invocation/AiModelBudgetRegistry.java) | `org.pangrova.ai.worker.invocation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ConfiguredAiProviderRegistry.java](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/provider/ConfiguredAiProviderRegistry.java) | `org.pangrova.ai.worker.provider` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-client-jdbc

16 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [JdbcConnectionParams.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/JdbcConnectionParams.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [JdbcSharedHttpOwner.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/JdbcSharedHttpOwner.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [JdbcUrlParser.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/JdbcUrlParser.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [MetadataResultSets.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/MetadataResultSets.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaConnection.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaConnection.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaDatabaseMetaData.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaDatabaseMetaData.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaDriver.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaDriver.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaParameterMetaData.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaParameterMetaData.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaPreparedStatement.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaPreparedStatement.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaResultSet.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaResultSet.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaResultSetMetaData.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaResultSetMetaData.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [PangrovaStatement.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/PangrovaStatement.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [QueryCompletionWaiter.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/QueryCompletionWaiter.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [SqlParameterSubstitutor.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/SqlParameterSubstitutor.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [StaticResultSet.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/StaticResultSet.java) | `org.pangrova.client.jdbc` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |
| [VectorSqlArray.java](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/value/VectorSqlArray.java) | `org.pangrova.client.jdbc.value` | 已识别为客户端JDBC驱动；保持Client边界，不迁入Persistence |

## pangrova-common

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [JdbcSupport.java](../../../pangrova-persistence/pangrova-persistence-jdbc-support/src/main/java/org/pangrova/persistence/jdbc/support/JdbcSupport.java) | `org.pangrova.persistence.jdbc.support` | 已迁入 JDBC support；保留现有 SQL/连接语义 |
| [ProcessIncarnationStore.java](../../../pangrova-common/src/main/java/org/pangrova/common/lifecycle/ProcessIncarnationStore.java) | `org.pangrova.common.lifecycle` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CheckpointStore.java](../../../pangrova-common/src/main/java/org/pangrova/common/resilience/api/CheckpointStore.java) | `org.pangrova.common.resilience.api` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-config

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [StateStoreFactory.java](../../../pangrova-config/src/main/java/org/pangrova/config/StateStoreFactory.java) | `org.pangrova.config` | 候选；待逐文件确认职责并迁移/留调用方 |
| [StateStoreType.java](../../../pangrova-config/src/main/java/org/pangrova/config/StateStoreType.java) | `org.pangrova.config` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-connector-iceberg

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [IcebergResidualRegistry.java](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/IcebergResidualRegistry.java) | `org.pangrova.connector.iceberg` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IcebergViewStore.java](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/IcebergViewStore.java) | `org.pangrova.connector.iceberg` | 候选；待逐文件确认职责并迁移/留调用方 |
| [QuerySnapshotRegistry.java](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/QuerySnapshotRegistry.java) | `org.pangrova.connector.iceberg` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-connector-lance

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [LanceCatalogStore.java](../../../pangrova-connector/pangrova-connector-lance/src/main/java/org/pangrova/connector/lance/LanceCatalogStore.java) | `org.pangrova.connector.lance` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-connector-paimon

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [PaimonSplitRegistry.java](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/PaimonSplitRegistry.java) | `org.pangrova.connector.paimon` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-connector-runtime

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ConnectorFactoryRegistry.java](../../../pangrova-connector/pangrova-connector-runtime/src/main/java/org/pangrova/connector/runtime/discovery/ConnectorFactoryRegistry.java) | `org.pangrova.connector.runtime.discovery` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-coordination-core

23 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ClusteringMetadataStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/clustering/ClusteringMetadataStore.java) | `org.pangrova.coordinator.clustering` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CoordinatorRegistryClient.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination/CoordinatorRegistryClient.java) | `org.pangrova.coordinator.coordination` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InvalidationProgressRegistryAdapter.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination/InvalidationProgressRegistryAdapter.java) | `org.pangrova.coordinator.coordination` | 候选；待逐文件确认职责并迁移/留调用方 |
| [QueryOwnershipRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination/QueryOwnershipRegistry.java) | `org.pangrova.coordinator.coordination` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IdempotencyKeyRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/IdempotencyKeyRegistry.java) | `org.pangrova.coordinator.http` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CatalogJdbcMetadataProvider.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/CatalogJdbcMetadataProvider.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcExecutionException.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcExecutionException.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMetadataProvider.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcMetadataProvider.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcQueryExecutor.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcQueryExecutor.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcQueryResult.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcQueryResult.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcWireProtocolServlet.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcWireProtocolServlet.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PangrovaJdbcMeta.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/PangrovaJdbcMeta.java) | `org.pangrova.coordinator.http.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [MetadataBreakerStateStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/indexing/MetadataBreakerStateStore.java) | `org.pangrova.coordinator.indexing` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CoordinatorPhysicalGenerationStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/materialization/CoordinatorPhysicalGenerationStore.java) | `org.pangrova.coordinator.materialization` | 候选；待逐文件确认职责并迁移/留调用方 |
| [RedisL2CacheStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/cache/RedisL2CacheStore.java) | `org.pangrova.coordinator.query.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TaskTerminalRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/handler/TaskTerminalRegistry.java) | `org.pangrova.coordinator.query.execution.dispatch.dag.handler` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcShuffleCleanupIntentStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle/JdbcShuffleCleanupIntentStore.java) | `org.pangrova.coordinator.query.execution.dispatch.shuffle` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CoordinatorShuffleEvidenceRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle/generation/CoordinatorShuffleEvidenceRegistry.java) | `org.pangrova.coordinator.query.execution.dispatch.shuffle.generation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryCheckpointStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/checkpoint/InMemoryCheckpointStore.java) | `org.pangrova.coordinator.resilience.checkpoint` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ResilienceRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/resilience4j/ResilienceRegistry.java) | `org.pangrova.coordinator.resilience.resilience4j` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CoordinatorSpeculativeTaskRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/scheduler/CoordinatorSpeculativeTaskRegistry.java) | `org.pangrova.coordinator.scheduler` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryStorageProfileRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/storage/InMemoryStorageProfileRegistry.java) | `org.pangrova.coordinator.storage` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TaskIdentityRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/task/TaskIdentityRegistry.java) | `org.pangrova.coordinator.task` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-coordination-zookeeper

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ZooKeeperCoordinatorRegistry.java](../../../pangrova-coordination/pangrova-coordination-zookeeper/src/main/java/org/pangrova/coordination/zookeeper/ZooKeeperCoordinatorRegistry.java) | `org.pangrova.coordination.zookeeper` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-filesystem-api

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ObjectStoreAccessor.java](../../../pangrova-filesystem/pangrova-filesystem-api/src/main/java/org/pangrova/filesystem/api/object/ObjectStoreAccessor.java) | `org.pangrova.filesystem.api.object` | 物理文件能力，保持Filesystem边界 |
| [ObjectStoreAccessorProvider.java](../../../pangrova-filesystem/pangrova-filesystem-api/src/main/java/org/pangrova/filesystem/api/object/ObjectStoreAccessorProvider.java) | `org.pangrova.filesystem.api.object` | 物理文件能力，保持Filesystem边界 |

## pangrova-filesystem-local

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [LocalObjectStoreAccessor.java](../../../pangrova-filesystem/pangrova-filesystem-local/src/main/java/org/pangrova/filesystem/local/object/LocalObjectStoreAccessor.java) | `org.pangrova.filesystem.local.object` | 物理文件能力，保持Filesystem边界 |
| [LocalObjectStoreAccessorProvider.java](../../../pangrova-filesystem/pangrova-filesystem-local/src/main/java/org/pangrova/filesystem/local/object/LocalObjectStoreAccessorProvider.java) | `org.pangrova.filesystem.local.object` | 物理文件能力，保持Filesystem边界 |

## pangrova-filesystem-s3

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [S3ObjectStoreAccessor.java](../../../pangrova-filesystem/pangrova-filesystem-s3/src/main/java/org/pangrova/filesystem/s3/object/S3ObjectStoreAccessor.java) | `org.pangrova.filesystem.s3.object` | 物理文件能力，保持Filesystem边界 |
| [S3ObjectStoreAccessorProvider.java](../../../pangrova-filesystem/pangrova-filesystem-s3/src/main/java/org/pangrova/filesystem/s3/object/S3ObjectStoreAccessorProvider.java) | `org.pangrova.filesystem.s3.object` | 物理文件能力，保持Filesystem边界 |

## pangrova-governance

15 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [CatalogTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/catalog/CatalogTagStore.java) | `org.pangrova.governance.api.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
| [AuditConfigStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/classification/AuditConfigStore.java) | `org.pangrova.governance.api.classification` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ColumnTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/classification/ColumnTagStore.java) | `org.pangrova.governance.api.classification` | 候选；待逐文件确认职责并迁移/留调用方 |
| [QualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/quality/QualityRuleStore.java) | `org.pangrova.governance.api.quality` | 候选；待逐文件确认职责并迁移/留调用方 |
| [QualityRuleStoreException.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/quality/QualityRuleStoreException.java) | `org.pangrova.governance.api.quality` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryAuditConfigStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification/InMemoryAuditConfigStore.java) | `org.pangrova.governance.core.classification` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryCatalogTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification/InMemoryCatalogTagStore.java) | `org.pangrova.governance.core.classification` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryColumnTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification/InMemoryColumnTagStore.java) | `org.pangrova.governance.core.classification` | 候选；待逐文件确认职责并迁移/留调用方 |
| [AlertChannelRegistry.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality/AlertChannelRegistry.java) | `org.pangrova.governance.core.quality` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IdentityQualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality/IdentityQualityRuleStore.java) | `org.pangrova.governance.core.quality` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryQualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality/InMemoryQualityRuleStore.java) | `org.pangrova.governance.core.quality` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcColumnTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/store/jdbc/JdbcColumnTagStore.java) | `org.pangrova.governance.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcGovernanceAuditTrail.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/store/jdbc/JdbcGovernanceAuditTrail.java) | `org.pangrova.governance.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcQualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/store/jdbc/JdbcQualityRuleStore.java) | `org.pangrova.governance.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [QualityRuleStores.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/store/jdbc/QualityRuleStores.java) | `org.pangrova.governance.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-indexing-core

30 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [BreakerStateStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/eval/BreakerStateStore.java) | `org.pangrova.indexing.eval` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexEngineRegistry.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/registry/IndexEngineRegistry.java) | `org.pangrova.indexing.registry` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TypeSerializerRegistry.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/serde/TypeSerializerRegistry.java) | `org.pangrova.indexing.serde` | 候选；待逐文件确认职责并迁移/留调用方 |
| [BreakerState.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/BreakerState.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CacheEvent.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/CacheEvent.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CommitEventRequest.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/CommitEventRequest.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CommitLogEntry.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/CommitLogEntry.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexFileMapping.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexFileMapping.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexFileSelection.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexFileSelection.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexGenerationFormat.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexGenerationFormat.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexGenerationManifest.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexGenerationManifest.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexGenerationSelectionSupport.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexGenerationSelectionSupport.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexMetadataStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexMetadataStore.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexPublicationRequest.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexPublicationRequest.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexSchemaEvolutionHandler.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexSchemaEvolutionHandler.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SchemaChangeEvent.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/SchemaChangeEvent.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SchemaChangeType.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/SchemaChangeType.java) | `org.pangrova.indexing.store` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GenerationReservation.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/GenerationReservation.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [IndexMetadataMapper.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/IndexMetadataMapper.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexCommitQueue.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexCommitQueue.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexDescriptorReadStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexDescriptorReadStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexDescriptorStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexDescriptorStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexFileMappingStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexFileMappingStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexGcStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexGcStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexMetadataStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexMetadataStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexPublicationStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexPublicationStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexSql.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexSql.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexStateStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexStateStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexStatisticsStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexStatisticsStore.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcStoreSupport.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcStoreSupport.java) | `org.pangrova.indexing.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-materialization-core

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [MaterializationExtensionRegistry.java](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/extension/MaterializationExtensionRegistry.java) | `org.pangrova.materialization.extension` | 候选；待逐文件确认职责并迁移/留调用方 |
| [MaterializationRepository.java](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/port/MaterializationRepository.java) | `org.pangrova.materialization.port` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PhysicalGenerationStore.java](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/port/PhysicalGenerationStore.java) | `org.pangrova.materialization.port` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-materialization-store-jdbc

9 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [JdbcMaterializationCatalog.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationCatalog.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationPublications.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationPublications.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationQueries.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationQueries.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationReaderPins.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationReaderPins.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationRepository.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationRepository.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationRows.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationRows.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationSql.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationSql.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMaterializationWrites.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/JdbcMaterializationWrites.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [MaterializationJson.java](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc/MaterializationJson.java) | `org.pangrova.materialization.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-metadata

39 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [QualityRuleStoreWiring.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/governance/QualityRuleStoreWiring.java) | `org.pangrova.metadata.adapter.governance` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TableControlQualityRuleStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/governance/TableControlQualityRuleStore.java) | `org.pangrova.metadata.adapter.governance` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TableControlColumnMaskStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/security/TableControlColumnMaskStore.java) | `org.pangrova.metadata.adapter.security` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InvalidationProgressRegistry.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/cache/InvalidationProgressRegistry.java) | `org.pangrova.metadata.api.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [L2CacheStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/cache/L2CacheStore.java) | `org.pangrova.metadata.api.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [MetadataStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/catalog/MetadataStore.java) | `org.pangrova.metadata.api.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ObjectDependencyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/relation/ObjectDependencyStore.java) | `org.pangrova.metadata.api.relation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GenerationStores.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/assembly/generation/GenerationStores.java) | `org.pangrova.metadata.assembly.generation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ColumnMaskStoreWiring.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/assembly/security/ColumnMaskStoreWiring.java) | `org.pangrova.metadata.assembly.security` | 候选；待逐文件确认职责并迁移/留调用方 |
| [DisabledL2CacheStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/cache/DisabledL2CacheStore.java) | `org.pangrova.metadata.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryL2CacheStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/cache/InMemoryL2CacheStore.java) | `org.pangrova.metadata.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CatalogRegistry.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/catalog/registry/CatalogRegistry.java) | `org.pangrova.metadata.catalog.registry` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcCheckpointStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/checkpoint/JdbcCheckpointStore.java) | `org.pangrova.metadata.checkpoint` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GeneratedValueBuildMetadataStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/GeneratedValueBuildMetadataStore.java) | `org.pangrova.metadata.generation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GeneratedValueBuildSubmissionStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/GeneratedValueBuildSubmissionStore.java) | `org.pangrova.metadata.generation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GenerationPublicationJob.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/GenerationPublicationJob.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GenerationPublicationManifestDigest.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/GenerationPublicationManifestDigest.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [GenerationPublicationOutputValidator.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/GenerationPublicationOutputValidator.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcGeneratedValueDescriptorStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcGeneratedValueDescriptorStore.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcGeneratedValuePublicationStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcGeneratedValuePublicationStore.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcIndexGenerationPublicationStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcIndexGenerationPublicationStore.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcTabularExportPublicationStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcTabularExportPublicationStore.java) | `org.pangrova.metadata.generation.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcGeneratedValueBuildSubmissionStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/submission/JdbcGeneratedValueBuildSubmissionStore.java) | `org.pangrova.metadata.generation.jdbc.submission` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryObjectDependencyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/memory/relation/InMemoryObjectDependencyStore.java) | `org.pangrova.metadata.memory.relation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [RegistryTablePolicyAuthority.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/service/RegistryTablePolicyAuthority.java) | `org.pangrova.metadata.service` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TableControlRowPolicyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/service/TableControlRowPolicyStore.java) | `org.pangrova.metadata.service` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcManagedSourceCatalog.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcManagedSourceCatalog.java) | `org.pangrova.metadata.source.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcSourceNamespace.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourceNamespace.java) | `org.pangrova.metadata.source.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcSourceObjectCleanup.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourceObjectCleanup.java) | `org.pangrova.metadata.source.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcSourceObjectRegistration.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourceObjectRegistration.java) | `org.pangrova.metadata.source.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcSourcePreparationInvocations.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourcePreparationInvocations.java) | `org.pangrova.metadata.source.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcInvalidationPoller.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/cache/JdbcInvalidationPoller.java) | `org.pangrova.metadata.store.jdbc.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [DriverManagerDataSource.java](../../../pangrova-persistence/pangrova-persistence-jdbc-support/src/main/java/org/pangrova/persistence/jdbc/support/DriverManagerDataSource.java) | `org.pangrova.persistence.jdbc.support` | 已迁入 JDBC support；保留现有 SQL/连接语义 |
| [JdbcMetadataStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/JdbcMetadataStore.java) | `org.pangrova.metadata.store.jdbc.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
| [MetadataJsonMapper.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/MetadataJsonMapper.java) | `org.pangrova.metadata.store.jdbc.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PooledDataSourceFactory.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/PooledDataSourceFactory.java) | `org.pangrova.metadata.store.jdbc.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SchemaMigrationRunner.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/SchemaMigrationRunner.java) | `org.pangrova.metadata.store.jdbc.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcObjectDependencyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/object/JdbcObjectDependencyStore.java) | `org.pangrova.metadata.store.jdbc.object` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcTenantStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/tenant/JdbcTenantStore.java) | `org.pangrova.metadata.tenant` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-operations

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [DdlJobStore.java](../../../pangrova-operations/src/main/java/org/pangrova/operations/ddl/DdlJobStore.java) | `org.pangrova.operations.ddl` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-parser

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [StatementParserRegistry.java](../../../pangrova-parser/src/main/java/org/pangrova/parser/StatementParserRegistry.java) | `org.pangrova.parser` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SourceLocationRegistry.java](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/SourceLocationRegistry.java) | `org.pangrova.parser.ast` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-persistence-api

28 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [CoordinatorRegistry.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/coordination/CoordinatorRegistry.java) | `org.pangrova.persistence.api.coordination` | 已在目标owner；继续语义/装配验收 |
| [WorkerRegistry.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/coordination/WorkerRegistry.java) | `org.pangrova.persistence.api.coordination` | 已在目标owner；继续语义/装配验收 |
| [CopyFileClaimStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/claim/CopyFileClaimStore.java) | `org.pangrova.persistence.api.file.claim` | 已在目标owner；继续语义/装配验收 |
| [StorageProfileRegistry.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/profile/StorageProfileRegistry.java) | `org.pangrova.persistence.api.file.profile` | 已在目标owner；继续语义/装配验收 |
| [FileSpaceRegistry.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/space/FileSpaceRegistry.java) | `org.pangrova.persistence.api.file.space` | 已在目标owner；继续语义/装配验收 |
| [GenerationStageExchangeStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/exchange/GenerationStageExchangeStore.java) | `org.pangrova.persistence.api.generation.exchange` | 已在目标owner；继续语义/装配验收 |
| [GenerationStageExecutionStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/execution/GenerationStageExecutionStore.java) | `org.pangrova.persistence.api.generation.execution` | 已在目标owner；继续语义/装配验收 |
| [GenerationJobStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/job/GenerationJobStore.java) | `org.pangrova.persistence.api.generation.job` | 已在目标owner；继续语义/装配验收 |
| [GenerationPublicationStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/publication/GenerationPublicationStore.java) | `org.pangrova.persistence.api.generation.publication` | 已在目标owner；继续语义/装配验收 |
| [GenerationExecutionTransactionFence.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/transaction/jdbc/GenerationExecutionTransactionFence.java) | `org.pangrova.persistence.api.generation.transaction.jdbc` | 已在目标owner；继续语义/装配验收 |
| [GenerationJobSubmissionTransactions.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/transaction/jdbc/GenerationJobSubmissionTransactions.java) | `org.pangrova.persistence.api.generation.transaction.jdbc` | 已在目标owner；继续语义/装配验收 |
| [GenerationPublicationCandidates.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/transaction/jdbc/GenerationPublicationCandidates.java) | `org.pangrova.persistence.api.generation.transaction.jdbc` | 已在目标owner；继续语义/装配验收 |
| [GenerationTransactionAuthority.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/transaction/jdbc/GenerationTransactionAuthority.java) | `org.pangrova.persistence.api.generation.transaction.jdbc` | 已在目标owner；继续语义/装配验收 |
| [GenerationTransactionInputs.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/transaction/jdbc/GenerationTransactionInputs.java) | `org.pangrova.persistence.api.generation.transaction.jdbc` | 已在目标owner；继续语义/装配验收 |
| [QueryAuditStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/audit/QueryAuditStore.java) | `org.pangrova.persistence.api.query.audit` | 已在目标owner；继续语义/装配验收 |
| [ExecutionAnchorStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/coordination/ExecutionAnchorStore.java) | `org.pangrova.persistence.api.query.coordination` | 已在目标owner；继续语义/装配验收 |
| [WriteCommitQuarantineStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/transaction/WriteCommitQuarantineStore.java) | `org.pangrova.persistence.api.query.transaction` | 已在目标owner；继续语义/装配验收 |
| [ColumnMaskStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/mask/ColumnMaskStore.java) | `org.pangrova.persistence.api.security.mask` | 已在目标owner；继续语义/装配验收 |
| [EnrichedColumnMaskStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/mask/EnrichedColumnMaskStore.java) | `org.pangrova.persistence.api.security.mask` | 已在目标owner；继续语义/装配验收 |
| [GrantStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/policy/GrantStore.java) | `org.pangrova.persistence.api.security.policy` | 已在目标owner；继续语义/装配验收 |
| [RoleStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/policy/RoleStore.java) | `org.pangrova.persistence.api.security.policy` | 已在目标owner；继续语义/装配验收 |
| [IdentityRowPolicyStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/rowfilter/IdentityRowPolicyStore.java) | `org.pangrova.persistence.api.security.rowfilter` | 已在目标owner；继续语义/装配验收 |
| [RowPolicyStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/rowfilter/RowPolicyStore.java) | `org.pangrova.persistence.api.security.rowfilter` | 已在目标owner；继续语义/装配验收 |
| [TenantStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/tenant/TenantStore.java) | `org.pangrova.persistence.api.security.tenant` | 已在目标owner；继续语义/装配验收 |
| [TableControlStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/table/control/TableControlStore.java) | `org.pangrova.persistence.api.table.control` | 已在目标owner；继续语义/装配验收 |
| [TableControlGovernanceBindingStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/table/governancebinding/TableControlGovernanceBindingStore.java) | `org.pangrova.persistence.api.table.governancebinding` | 已在目标owner；继续语义/装配验收 |
| [TableControlMaintenanceStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/table/maintenance/TableControlMaintenanceStore.java) | `org.pangrova.persistence.api.table.maintenance` | 已在目标owner；继续语义/装配验收 |
| [TableControlStatisticsStore.java](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/table/statistics/TableControlStatisticsStore.java) | `org.pangrova.persistence.api.table.statistics` | 已在目标owner；继续语义/装配验收 |

## pangrova-persistence-jdbc

57 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [CoordinationStorageException.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/coordination/CoordinationStorageException.java) | `org.pangrova.persistence.jdbc.coordination` | 已在目标owner；继续语义/装配验收 |
| [JdbcCoordinatorRegistry.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/coordination/JdbcCoordinatorRegistry.java) | `org.pangrova.persistence.jdbc.coordination` | 已在目标owner；继续语义/装配验收 |
| [JdbcDistributedLease.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/coordination/JdbcDistributedLease.java) | `org.pangrova.persistence.jdbc.coordination` | 已在目标owner；继续语义/装配验收 |
| [JdbcWorkerRegistry.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/coordination/JdbcWorkerRegistry.java) | `org.pangrova.persistence.jdbc.coordination` | 已在目标owner；继续语义/装配验收 |
| [JdbcCopyFileClaimStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/claim/JdbcCopyFileClaimStore.java) | `org.pangrova.persistence.jdbc.file.claim` | 已在目标owner；继续语义/装配验收 |
| [JdbcCopySourceReadAdmissions.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/copy/JdbcCopySourceReadAdmissions.java) | `org.pangrova.persistence.jdbc.file.copy` | 已在目标owner；继续语义/装配验收 |
| [JdbcFileDirectoryIndex.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/directory/JdbcFileDirectoryIndex.java) | `org.pangrova.persistence.jdbc.file.directory` | 已在目标owner；继续语义/装配验收 |
| [JdbcStorageProfileRegistry.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/profile/JdbcStorageProfileRegistry.java) | `org.pangrova.persistence.jdbc.file.profile` | 已在目标owner；继续语义/装配验收 |
| [JdbcFileSpaceAuthority.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/space/JdbcFileSpaceAuthority.java) | `org.pangrova.persistence.jdbc.file.space` | 已在目标owner；继续语义/装配验收 |
| [JdbcFileSpaceRegistry.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/space/JdbcFileSpaceRegistry.java) | `org.pangrova.persistence.jdbc.file.space` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationStageExchangeStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/exchange/PostgresGenerationStageExchangeStore.java) | `org.pangrova.persistence.jdbc.generation.exchange` | 已在目标owner；继续语义/装配验收 |
| [PostgresCurrentGenerationAuthority.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/execution/PostgresCurrentGenerationAuthority.java) | `org.pangrova.persistence.jdbc.generation.execution` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationExecutionFence.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/execution/PostgresGenerationExecutionFence.java) | `org.pangrova.persistence.jdbc.generation.execution` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationExecutionRoutes.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/execution/PostgresGenerationExecutionRoutes.java) | `org.pangrova.persistence.jdbc.generation.execution` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationAdmission.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationAdmission.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationCompletion.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationCompletion.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationJobInsertion.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationJobInsertion.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationJobReader.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationJobReader.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationJobStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationJobStore.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationJobStoreMappings.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationJobStoreMappings.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationJobStoreSupport.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationJobStoreSupport.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationOutputQueries.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationOutputQueries.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationPageClaim.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationPageClaim.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationPageSelection.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationPageSelection.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationPublishedInputs.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationPublishedInputs.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationRevocation.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationRevocation.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationStageExecutionStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job/PostgresGenerationStageExecutionStore.java) | `org.pangrova.persistence.jdbc.generation.job` | 已在目标owner；继续语义/装配验收 |
| [PostgresGenerationQueries.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/query/PostgresGenerationQueries.java) | `org.pangrova.persistence.jdbc.generation.query` | 已在目标owner；继续语义/装配验收 |
| [JdbcExecutionAnchorStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/coordination/JdbcExecutionAnchorStore.java) | `org.pangrova.persistence.jdbc.query.coordination` | 已在目标owner；继续语义/装配验收 |
| [JdbcExecutionTransactionFence.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/coordination/JdbcExecutionTransactionFence.java) | `org.pangrova.persistence.jdbc.query.coordination` | 已在目标owner；继续语义/装配验收 |
| [JdbcQueryEventListener.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/event/JdbcQueryEventListener.java) | `org.pangrova.persistence.jdbc.query.event` | 已在目标owner；继续语义/装配验收 |
| [JdbcQueryLineageOutbox.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/lineage/JdbcQueryLineageOutbox.java) | `org.pangrova.persistence.jdbc.query.lineage` | 已在目标owner；继续语义/装配验收 |
| [QueryLineageFactSql.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/lineage/QueryLineageFactSql.java) | `org.pangrova.persistence.jdbc.query.lineage` | 已在目标owner；继续语义/装配验收 |
| [JdbcQueryStateJournal.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/state/JdbcQueryStateJournal.java) | `org.pangrova.persistence.jdbc.query.state` | 已在目标owner；继续语义/装配验收 |
| [JournalEventSql.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/state/JournalEventSql.java) | `org.pangrova.persistence.jdbc.query.state` | 已在目标owner；继续语义/装配验收 |
| [JournalFlushOwner.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/state/JournalFlushOwner.java) | `org.pangrova.persistence.jdbc.query.state` | 已在目标owner；继续语义/装配验收 |
| [JournalPendingWrites.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/state/JournalPendingWrites.java) | `org.pangrova.persistence.jdbc.query.state` | 已在目标owner；继续语义/装配验收 |
| [JournalSqlOperation.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/state/JournalSqlOperation.java) | `org.pangrova.persistence.jdbc.query.state` | 已在目标owner；继续语义/装配验收 |
| [JdbcWriteCommitJournal.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/transaction/JdbcWriteCommitJournal.java) | `org.pangrova.persistence.jdbc.query.transaction` | 已在目标owner；继续语义/装配验收 |
| [JdbcWriteCommitQuarantineStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/transaction/JdbcWriteCommitQuarantineStore.java) | `org.pangrova.persistence.jdbc.query.transaction` | 已在目标owner；继续语义/装配验收 |
| [JdbcAuditEventWriter.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/audit/JdbcAuditEventWriter.java) | `org.pangrova.persistence.jdbc.security.audit` | 已在目标owner；继续语义/装配验收 |
| [DeferredAuthorizationScopeCodec.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/codec/DeferredAuthorizationScopeCodec.java) | `org.pangrova.persistence.jdbc.security.codec` | 已在目标owner；继续语义/装配验收 |
| [PolicyRoleCodec.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/codec/PolicyRoleCodec.java) | `org.pangrova.persistence.jdbc.security.codec` | 已在目标owner；继续语义/装配验收 |
| [JdbcColumnMaskStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/mask/JdbcColumnMaskStore.java) | `org.pangrova.persistence.jdbc.security.mask` | 已在目标owner；继续语义/装配验收 |
| [JdbcCurrentDirectRoleReader.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/policy/JdbcCurrentDirectRoleReader.java) | `org.pangrova.persistence.jdbc.security.policy` | 已在目标owner；继续语义/装配验收 |
| [JdbcCurrentGrantReader.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/policy/JdbcCurrentGrantReader.java) | `org.pangrova.persistence.jdbc.security.policy` | 已在目标owner；继续语义/装配验收 |
| [JdbcCurrentPolicyReader.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/policy/JdbcCurrentPolicyReader.java) | `org.pangrova.persistence.jdbc.security.policy` | 已在目标owner；继续语义/装配验收 |
| [JdbcGrantStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/policy/JdbcGrantStore.java) | `org.pangrova.persistence.jdbc.security.policy` | 已在目标owner；继续语义/装配验收 |
| [JdbcRoleStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/policy/JdbcRoleStore.java) | `org.pangrova.persistence.jdbc.security.policy` | 已在目标owner；继续语义/装配验收 |
| [JdbcRowPolicyStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/rowfilter/JdbcRowPolicyStore.java) | `org.pangrova.persistence.jdbc.security.rowfilter` | 已在目标owner；继续语义/装配验收 |
| [JdbcPolicyStoreSupport.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/support/JdbcPolicyStoreSupport.java) | `org.pangrova.persistence.jdbc.security.support` | 已在目标owner；继续语义/装配验收 |
| [JdbcTenantStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/tenant/JdbcTenantStore.java) | `org.pangrova.persistence.jdbc.security.tenant` | 已在目标owner；继续语义/装配验收 |
| [AuthorizationTransactionGate.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/transaction/AuthorizationTransactionGate.java) | `org.pangrova.persistence.jdbc.security.transaction` | 已在目标owner；继续语义/装配验收 |
| [JdbcTableControlStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/table/control/JdbcTableControlStore.java) | `org.pangrova.persistence.jdbc.table.control` | 已在目标owner；继续语义/装配验收 |
| [JdbcTableControlGovernanceBindingStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/table/governancebinding/JdbcTableControlGovernanceBindingStore.java) | `org.pangrova.persistence.jdbc.table.governancebinding` | 已在目标owner；继续语义/装配验收 |
| [JdbcTableControlMaintenanceStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/table/maintenance/JdbcTableControlMaintenanceStore.java) | `org.pangrova.persistence.jdbc.table.maintenance` | 已在目标owner；继续语义/装配验收 |
| [JdbcTableControlStatisticsStore.java](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/table/statistics/JdbcTableControlStatisticsStore.java) | `org.pangrova.persistence.jdbc.table.statistics` | 已在目标owner；继续语义/装配验收 |

## pangrova-persistence-memory

9 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [InMemoryCoordinatorRegistry.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/coordination/InMemoryCoordinatorRegistry.java) | `org.pangrova.persistence.memory.coordination` | 已在目标owner；继续语义/装配验收 |
| [InMemoryWorkerRegistry.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/coordination/InMemoryWorkerRegistry.java) | `org.pangrova.persistence.memory.coordination` | 已在目标owner；继续语义/装配验收 |
| [InMemoryQueryAuditStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/audit/InMemoryQueryAuditStore.java) | `org.pangrova.persistence.memory.query.audit` | 已在目标owner；继续语义/装配验收 |
| [InMemoryExecutionAnchorStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/coordination/InMemoryExecutionAnchorStore.java) | `org.pangrova.persistence.memory.query.coordination` | 已在目标owner；继续语义/装配验收 |
| [InMemoryWriteCommitQuarantineStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/transaction/InMemoryWriteCommitQuarantineStore.java) | `org.pangrova.persistence.memory.query.transaction` | 已在目标owner；继续语义/装配验收 |
| [InMemoryTableControlStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/table/control/InMemoryTableControlStore.java) | `org.pangrova.persistence.memory.table.control` | 已在目标owner；继续语义/装配验收 |
| [InMemoryTableControlGovernanceBindingStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/table/governancebinding/InMemoryTableControlGovernanceBindingStore.java) | `org.pangrova.persistence.memory.table.governancebinding` | 已在目标owner；继续语义/装配验收 |
| [InMemoryTableControlMaintenanceStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/table/maintenance/InMemoryTableControlMaintenanceStore.java) | `org.pangrova.persistence.memory.table.maintenance` | 已在目标owner；继续语义/装配验收 |
| [InMemoryTableControlStatisticsStore.java](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/table/statistics/InMemoryTableControlStatisticsStore.java) | `org.pangrova.persistence.memory.table.statistics` | 已在目标owner；继续语义/装配验收 |

## pangrova-planner

10 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [InMemoryL2CacheStore.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/cache/InMemoryL2CacheStore.java) | `org.pangrova.planner.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [L2CacheStore.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/cache/L2CacheStore.java) | `org.pangrova.planner.cache` | 候选；待逐文件确认职责并迁移/留调用方 |
| [HepRuleRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/optimization/HepRuleRegistry.java) | `org.pangrova.planner.optimization` | 候选；待逐文件确认职责并迁移/留调用方 |
| [RuntimeFilterRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/runtimefilter/RuntimeFilterRegistry.java) | `org.pangrova.planner.runtimefilter` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PlanBaselineRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/stability/PlanBaselineRegistry.java) | `org.pangrova.planner.stability` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PlanCaptureRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/stability/PlanCaptureRegistry.java) | `org.pangrova.planner.stability` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CardinalityFeedbackStore.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/stats/CardinalityFeedbackStore.java) | `org.pangrova.planner.stats` | 候选；待逐文件确认职责并迁移/留调用方 |
| [FunctionNullabilityRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/substrait/FunctionNullabilityRegistry.java) | `org.pangrova.planner.substrait` | 候选；待逐文件确认职责并迁移/留调用方 |
| [StandardTypeRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/type/StandardTypeRegistry.java) | `org.pangrova.planner.type` | 候选；待逐文件确认职责并迁移/留调用方 |
| [NativeCapabilityRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/validation/NativeCapabilityRegistry.java) | `org.pangrova.planner.validation` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-scheduler

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [SpeculativeTaskRegistry.java](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/speculation/SpeculativeTaskRegistry.java) | `org.pangrova.scheduler.speculation` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcClusterConcurrencySlots.java](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/store/jdbc/admission/JdbcClusterConcurrencySlots.java) | `org.pangrova.scheduler.store.jdbc.admission` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-security

8 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [InMemoryRoleStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/authz/InMemoryRoleStore.java) | `org.pangrova.security.authz` | 候选；待逐文件确认职责并迁移/留调用方 |
| [StaticGrantStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/authz/StaticGrantStore.java) | `org.pangrova.security.authz` | 候选；待逐文件确认职责并迁移/留调用方 |
| [EmptyColumnMaskStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/mask/empty/EmptyColumnMaskStore.java) | `org.pangrova.security.mask.empty` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryEnrichedColumnMaskStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/mask/memory/InMemoryEnrichedColumnMaskStore.java) | `org.pangrova.security.mask.memory` | 候选；待逐文件确认职责并迁移/留调用方 |
| [EmptyRowPolicyStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/rowfilter/EmptyRowPolicyStore.java) | `org.pangrova.security.rowfilter` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SecretResolverRegistry.java](../../../pangrova-security/src/main/java/org/pangrova/security/secret/SecretResolverRegistry.java) | `org.pangrova.security.secret` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CurrentModelAuthority.java](../../../pangrova-security/src/main/java/org/pangrova/security/spi/jdbc/CurrentModelAuthority.java) | `org.pangrova.security.spi.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryTenantStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/tenant/InMemoryTenantStore.java) | `org.pangrova.security.tenant` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-semantic

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [SemanticStoreException.java](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/api/view/SemanticStoreException.java) | `org.pangrova.semantic.api.view` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SemanticViewStore.java](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/api/view/SemanticViewStore.java) | `org.pangrova.semantic.api.view` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcSemanticViewStore.java](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/store/jdbc/JdbcSemanticViewStore.java) | `org.pangrova.semantic.store.jdbc` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-shuffle-core

5 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [SpoolStore.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/SpoolStore.java) | `org.pangrova.shuffle.spool` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SpoolStoreContext.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/SpoolStoreContext.java) | `org.pangrova.shuffle.spool` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SpoolStoreFactory.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/SpoolStoreFactory.java) | `org.pangrova.shuffle.spool` | 候选；待逐文件确认职责并迁移/留调用方 |
| [LocalFsSpoolStore.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/localfs/LocalFsSpoolStore.java) | `org.pangrova.shuffle.spool.localfs` | 候选；待逐文件确认职责并迁移/留调用方 |
| [LocalFsSpoolStoreFactory.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/localfs/LocalFsSpoolStoreFactory.java) | `org.pangrova.shuffle.spool.localfs` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-shuffle-service

8 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [PostgresCommitVerifier.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresCommitVerifier.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PostgresShuffleBatchStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleBatchStore.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PostgresShuffleLifecycleTransitions.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleLifecycleTransitions.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PostgresShuffleRecords.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleRecords.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [PostgresShuffleStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleStore.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ShuffleBatchStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/ShuffleBatchStore.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ShuffleLifecycleStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/ShuffleLifecycleStore.java) | `org.pangrova.shuffle.server` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcTransactions.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/persistence/JdbcTransactions.java) | `org.pangrova.shuffle.server.persistence` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-streaming

14 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [CdcRegistrationStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/cdc/CdcRegistrationStore.java) | `org.pangrova.streaming.api.cdc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [StreamOffsetStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint/StreamOffsetStore.java) | `org.pangrova.streaming.api.checkpoint` | 候选；待逐文件确认职责并迁移/留调用方 |
| [StreamStateStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint/StreamStateStore.java) | `org.pangrova.streaming.api.checkpoint` | 候选；待逐文件确认职责并迁移/留调用方 |
| [StreamingStoreException.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint/StreamingStoreException.java) | `org.pangrova.streaming.api.checkpoint` | 候选；待逐文件确认职责并迁移/留调用方 |
| [WatermarkStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/watermark/WatermarkStore.java) | `org.pangrova.streaming.api.watermark` | 候选；待逐文件确认职责并迁移/留调用方 |
| [CdcRecordConverterRegistry.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/cdc/CdcRecordConverterRegistry.java) | `org.pangrova.streaming.cdc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcCdcCommitStateStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/commit/JdbcCdcCommitStateStore.java) | `org.pangrova.streaming.commit` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcCdcRegistrationStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/cdc/JdbcCdcRegistrationStore.java) | `org.pangrova.streaming.store.jdbc.cdc` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcStreamStateStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/checkpoint/JdbcStreamStateStore.java) | `org.pangrova.streaming.store.jdbc.checkpoint` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcStreamOffsetStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/offset/JdbcStreamOffsetStore.java) | `org.pangrova.streaming.store.jdbc.offset` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcWatermarkPositionStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/watermark/JdbcWatermarkPositionStore.java) | `org.pangrova.streaming.store.jdbc.watermark` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcWatermarkStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/watermark/JdbcWatermarkStore.java) | `org.pangrova.streaming.store.jdbc.watermark` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryCdcRegistrationStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/memory/InMemoryCdcRegistrationStore.java) | `org.pangrova.streaming.store.memory` | 候选；待逐文件确认职责并迁移/留调用方 |
| [InMemoryWatermarkStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/memory/InMemoryWatermarkStore.java) | `org.pangrova.streaming.store.memory` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-testing-kit

4 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [PostgresCommitWireFault.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/PostgresCommitWireFault.java) | `org.pangrova.testing.testkit` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcMutationFault.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres/JdbcMutationFault.java) | `org.pangrova.testing.testkit.postgres` | 候选；待逐文件确认职责并迁移/留调用方 |
| [MigratedPostgres.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres/MigratedPostgres.java) | `org.pangrova.testing.testkit.postgres` | 候选；待逐文件确认职责并迁移/留调用方 |
| [SharedPostgres.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres/SharedPostgres.java) | `org.pangrova.testing.testkit.postgres` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-worker-core

7 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [QueryOutputBufferRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/buffer/QueryOutputBufferRegistry.java) | `org.pangrova.worker.buffer` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ExchangeSourceRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/source/ExchangeSourceRegistry.java) | `org.pangrova.worker.exchange.source` | 候选；待逐文件确认职责并迁移/留调用方 |
| [AiTaskLifecycleRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/execution/AiTaskLifecycleRegistry.java) | `org.pangrova.worker.execution` | 候选；待逐文件确认职责并迁移/留调用方 |
| [FileSpaceObjectStore.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file/FileSpaceObjectStore.java) | `org.pangrova.worker.file` | 候选；待逐文件确认职责并迁移/留调用方 |
| [JdbcFileDirectoryPageSource.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file/JdbcFileDirectoryPageSource.java) | `org.pangrova.worker.file` | 候选；待逐文件确认职责并迁移/留调用方 |
| [ObjectStoreAccessorFactory.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file/ObjectStoreAccessorFactory.java) | `org.pangrova.worker.file` | 候选；待逐文件确认职责并迁移/留调用方 |
| [TaskAdmissionRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/TaskAdmissionRegistry.java) | `org.pangrova.worker.task` | 候选；待逐文件确认职责并迁移/留调用方 |

## pangrova-worker-native-bridge

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [NativeObjectStoreBindings.java](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/write/catalog/NativeObjectStoreBindings.java) | `org.pangrova.worker.nativebridge.jni.write.catalog` | 候选；待逐文件确认职责并迁移/留调用方 |
