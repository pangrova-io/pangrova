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
| [AiInfrastructureStores.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/assembly/AiInfrastructureStores.java) | `org.pangrova.persistence.ai.postgresql.assembly` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ConcurrentMapEmbeddingModelRegistry.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding/ConcurrentMapEmbeddingModelRegistry.java) | `org.pangrova.ai.control.embedding` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [EmbeddingModelRegistry.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding/EmbeddingModelRegistry.java) | `org.pangrova.ai.control.embedding` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [EmbeddingRegistryAccessDeniedException.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding/EmbeddingRegistryAccessDeniedException.java) | `org.pangrova.ai.control.embedding` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [EvaluationStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/evaluation/EvaluationStore.java) | `org.pangrova.ai.control.evaluation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CatalogExportBuildStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/feature/CatalogExportBuildStore.java) | `org.pangrova.ai.control.feature` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [FeatureCatalogStore.java](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/feature/FeatureCatalogStore.java) | `org.pangrova.ai.control.feature` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [EmbeddingModelRoleAuthority.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/embedding/EmbeddingModelRoleAuthority.java) | `org.pangrova.persistence.ai.postgresql.embedding` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcCurrentModelAuthority.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/embedding/JdbcCurrentModelAuthority.java) | `org.pangrova.persistence.ai.postgresql.embedding` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcEmbeddingModelRegistry.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/embedding/JdbcEmbeddingModelRegistry.java) | `org.pangrova.persistence.ai.postgresql.embedding` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcEvaluationReleaseGate.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/evaluation/JdbcEvaluationReleaseGate.java) | `org.pangrova.persistence.ai.postgresql.evaluation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcEvaluationStore.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/evaluation/JdbcEvaluationStore.java) | `org.pangrova.persistence.ai.postgresql.evaluation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcFeatureViewEvaluationEvidence.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/evaluation/JdbcFeatureViewEvaluationEvidence.java) | `org.pangrova.persistence.ai.postgresql.evaluation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcCatalogExportBuildStore.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/feature/JdbcCatalogExportBuildStore.java) | `org.pangrova.persistence.ai.postgresql.feature` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcCatalogExportJobInsertion.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/feature/JdbcCatalogExportJobInsertion.java) | `org.pangrova.persistence.ai.postgresql.feature` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcFeatureCatalogStore.java](../../../pangrova-persistence/pangrova-persistence-ai-postgresql/src/main/java/org/pangrova/persistence/ai/postgresql/feature/JdbcFeatureCatalogStore.java) | `org.pangrova.persistence.ai.postgresql.feature` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-ai-worker

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [GovernedBlobStore.java](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/blob/GovernedBlobStore.java) | `org.pangrova.ai.worker.blob` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [AiModelBudgetRegistry.java](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/invocation/AiModelBudgetRegistry.java) | `org.pangrova.ai.worker.invocation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ConfiguredAiProviderRegistry.java](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/provider/ConfiguredAiProviderRegistry.java) | `org.pangrova.ai.worker.provider` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

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
| [ProcessIncarnationStore.java](../../../pangrova-common/src/main/java/org/pangrova/common/lifecycle/ProcessIncarnationStore.java) | `org.pangrova.common.lifecycle` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CheckpointStore.java](../../../pangrova-common/src/main/java/org/pangrova/common/resilience/api/CheckpointStore.java) | `org.pangrova.common.resilience.api` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-config

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [StateStoreFactory.java](../../../pangrova-config/src/main/java/org/pangrova/config/StateStoreFactory.java) | `org.pangrova.config` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [StateStoreType.java](../../../pangrova-config/src/main/java/org/pangrova/config/StateStoreType.java) | `org.pangrova.config` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-connector-iceberg

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [IcebergResidualRegistry.java](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/IcebergResidualRegistry.java) | `org.pangrova.connector.iceberg` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IcebergViewStore.java](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/IcebergViewStore.java) | `org.pangrova.connector.iceberg` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [QuerySnapshotRegistry.java](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/QuerySnapshotRegistry.java) | `org.pangrova.connector.iceberg` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-connector-lance

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [LanceCatalogStore.java](../../../pangrova-connector/pangrova-connector-lance/src/main/java/org/pangrova/connector/lance/LanceCatalogStore.java) | `org.pangrova.connector.lance` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-connector-paimon

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [PaimonSplitRegistry.java](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/PaimonSplitRegistry.java) | `org.pangrova.connector.paimon` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-connector-runtime

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ConnectorFactoryRegistry.java](../../../pangrova-connector/pangrova-connector-runtime/src/main/java/org/pangrova/connector/runtime/discovery/ConnectorFactoryRegistry.java) | `org.pangrova.connector.runtime.discovery` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-coordination-core

23 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ClusteringMetadataStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/clustering/ClusteringMetadataStore.java) | `org.pangrova.coordinator.clustering` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CoordinatorRegistryClient.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination/CoordinatorRegistryClient.java) | `org.pangrova.coordinator.coordination` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InvalidationProgressRegistryAdapter.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination/InvalidationProgressRegistryAdapter.java) | `org.pangrova.coordinator.coordination` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [QueryOwnershipRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination/QueryOwnershipRegistry.java) | `org.pangrova.coordinator.coordination` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IdempotencyKeyRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/IdempotencyKeyRegistry.java) | `org.pangrova.coordinator.http` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CatalogJdbcMetadataProvider.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/CatalogJdbcMetadataProvider.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcExecutionException.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcExecutionException.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcMetadataProvider.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcMetadataProvider.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcQueryExecutor.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcQueryExecutor.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcQueryResult.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcQueryResult.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcWireProtocolServlet.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/JdbcWireProtocolServlet.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PangrovaJdbcMeta.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc/PangrovaJdbcMeta.java) | `org.pangrova.coordinator.http.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [MetadataBreakerStateStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/indexing/MetadataBreakerStateStore.java) | `org.pangrova.coordinator.indexing` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CoordinatorPhysicalGenerationStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/materialization/CoordinatorPhysicalGenerationStore.java) | `org.pangrova.coordinator.materialization` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [RedisL2CacheStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/cache/RedisL2CacheStore.java) | `org.pangrova.coordinator.query.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TaskTerminalRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/handler/TaskTerminalRegistry.java) | `org.pangrova.coordinator.query.execution.dispatch.dag.handler` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcShuffleCleanupIntentStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle/JdbcShuffleCleanupIntentStore.java) | `org.pangrova.coordinator.query.execution.dispatch.shuffle` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CoordinatorShuffleEvidenceRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle/generation/CoordinatorShuffleEvidenceRegistry.java) | `org.pangrova.coordinator.query.execution.dispatch.shuffle.generation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryCheckpointStore.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/checkpoint/InMemoryCheckpointStore.java) | `org.pangrova.coordinator.resilience.checkpoint` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ResilienceRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/resilience4j/ResilienceRegistry.java) | `org.pangrova.coordinator.resilience.resilience4j` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CoordinatorSpeculativeTaskRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/scheduler/CoordinatorSpeculativeTaskRegistry.java) | `org.pangrova.coordinator.scheduler` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryStorageProfileRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/storage/InMemoryStorageProfileRegistry.java) | `org.pangrova.coordinator.storage` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TaskIdentityRegistry.java](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/task/TaskIdentityRegistry.java) | `org.pangrova.coordinator.task` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-coordination-zookeeper

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [ZooKeeperCoordinatorRegistry.java](../../../pangrova-coordination/pangrova-coordination-zookeeper/src/main/java/org/pangrova/coordination/zookeeper/ZooKeeperCoordinatorRegistry.java) | `org.pangrova.coordination.zookeeper` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

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
| [CatalogTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/catalog/CatalogTagStore.java) | `org.pangrova.governance.api.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [AuditConfigStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/classification/AuditConfigStore.java) | `org.pangrova.governance.api.classification` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ColumnTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/classification/ColumnTagStore.java) | `org.pangrova.governance.api.classification` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [QualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/quality/QualityRuleStore.java) | `org.pangrova.governance.api.quality` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [QualityRuleStoreException.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/quality/QualityRuleStoreException.java) | `org.pangrova.governance.api.quality` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryAuditConfigStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification/InMemoryAuditConfigStore.java) | `org.pangrova.governance.core.classification` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryCatalogTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification/InMemoryCatalogTagStore.java) | `org.pangrova.governance.core.classification` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryColumnTagStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification/InMemoryColumnTagStore.java) | `org.pangrova.governance.core.classification` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [AlertChannelRegistry.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality/AlertChannelRegistry.java) | `org.pangrova.governance.core.quality` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IdentityQualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality/IdentityQualityRuleStore.java) | `org.pangrova.governance.core.quality` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryQualityRuleStore.java](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality/InMemoryQualityRuleStore.java) | `org.pangrova.governance.core.quality` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| ~~JdbcColumnTagStore.java~~ | `org.pangrova.governance.store.jdbc` | 已删除（2026-11-06）：无生产调用方，且使用非 canonical 的 `column_tag` 表；持久化 tag 归 Table Control governance binding |
| [JdbcGovernanceAuditTrail.java](../../../pangrova-persistence/pangrova-persistence-governance-postgresql/src/main/java/org/pangrova/persistence/governance/postgresql/JdbcGovernanceAuditTrail.java) | `org.pangrova.persistence.governance.postgresql` | 已迁移（2026-11-06）至 Persistence Governance PostgreSQL |
| [JdbcQualityRuleStore.java](../../../pangrova-persistence/pangrova-persistence-governance-postgresql/src/main/java/org/pangrova/persistence/governance/postgresql/JdbcQualityRuleStore.java) | `org.pangrova.persistence.governance.postgresql` | 已迁移（2026-11-06）至 Persistence Governance PostgreSQL |
| [QualityRuleStores.java](../../../pangrova-persistence/pangrova-persistence-governance-postgresql/src/main/java/org/pangrova/persistence/governance/postgresql/QualityRuleStores.java) | `org.pangrova.persistence.governance.postgresql` | 已迁移（2026-11-06）至 Persistence Governance PostgreSQL |

## pangrova-indexing-core

30 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [BreakerStateStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/eval/BreakerStateStore.java) | `org.pangrova.indexing.eval` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexEngineRegistry.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/registry/IndexEngineRegistry.java) | `org.pangrova.indexing.registry` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TypeSerializerRegistry.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/serde/TypeSerializerRegistry.java) | `org.pangrova.indexing.serde` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [BreakerState.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/BreakerState.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CacheEvent.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/CacheEvent.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CommitEventRequest.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/CommitEventRequest.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CommitLogEntry.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/CommitLogEntry.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexFileMapping.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexFileMapping.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexFileSelection.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexFileSelection.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexGenerationFormat.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexGenerationFormat.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexGenerationManifest.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexGenerationManifest.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexGenerationSelectionSupport.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexGenerationSelectionSupport.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexMetadataStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexMetadataStore.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexPublicationRequest.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexPublicationRequest.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexSchemaEvolutionHandler.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/IndexSchemaEvolutionHandler.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SchemaChangeEvent.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/SchemaChangeEvent.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SchemaChangeType.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/SchemaChangeType.java) | `org.pangrova.indexing.store` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [GenerationReservation.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/GenerationReservation.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [IndexMetadataMapper.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/IndexMetadataMapper.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexCommitQueue.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexCommitQueue.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexDescriptorReadStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexDescriptorReadStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexDescriptorStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexDescriptorStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexFileMappingStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexFileMappingStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexGcStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexGcStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexMetadataStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexMetadataStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexPublicationStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexPublicationStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexSql.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexSql.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexStateStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexStateStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexStatisticsStore.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcIndexStatisticsStore.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcStoreSupport.java](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc/JdbcStoreSupport.java) | `org.pangrova.indexing.store.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-materialization-core

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [MaterializationExtensionRegistry.java](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/extension/MaterializationExtensionRegistry.java) | `org.pangrova.materialization.extension` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [MaterializationRepository.java](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/port/MaterializationRepository.java) | `org.pangrova.materialization.port` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PhysicalGenerationStore.java](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/port/PhysicalGenerationStore.java) | `org.pangrova.materialization.port` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-persistence-materialization-postgresql

9 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [JdbcMaterializationCatalog.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationCatalog.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationPublications.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationPublications.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationQueries.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationQueries.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationReaderPins.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationReaderPins.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationRepository.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationRepository.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationRows.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationRows.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationSql.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationSql.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [JdbcMaterializationWrites.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/JdbcMaterializationWrites.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |
| [MaterializationJson.java](../../../pangrova-persistence/pangrova-persistence-materialization-postgresql/src/main/java/org/pangrova/persistence/materialization/postgresql/MaterializationJson.java) | `org.pangrova.persistence.materialization.postgresql` | 已迁移（2026-11-07）至 Persistence Materialization PostgreSQL |

## pangrova-metadata

39 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [QualityRuleStoreWiring.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/governance/QualityRuleStoreWiring.java) | `org.pangrova.metadata.adapter.governance` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TableControlQualityRuleStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/governance/TableControlQualityRuleStore.java) | `org.pangrova.metadata.adapter.governance` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TableControlColumnMaskStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/security/TableControlColumnMaskStore.java) | `org.pangrova.metadata.adapter.security` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InvalidationProgressRegistry.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/cache/InvalidationProgressRegistry.java) | `org.pangrova.metadata.api.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [L2CacheStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/cache/L2CacheStore.java) | `org.pangrova.metadata.api.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [MetadataStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/catalog/MetadataStore.java) | `org.pangrova.metadata.api.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ObjectDependencyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/relation/ObjectDependencyStore.java) | `org.pangrova.metadata.api.relation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [GenerationStores.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/assembly/generation/GenerationStores.java) | `org.pangrova.metadata.assembly.generation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ColumnMaskStoreWiring.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/assembly/security/ColumnMaskStoreWiring.java) | `org.pangrova.metadata.assembly.security` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [DisabledL2CacheStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/cache/DisabledL2CacheStore.java) | `org.pangrova.metadata.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryL2CacheStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/cache/InMemoryL2CacheStore.java) | `org.pangrova.metadata.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CatalogRegistry.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/catalog/registry/CatalogRegistry.java) | `org.pangrova.metadata.catalog.registry` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| ~~JdbcCheckpointStore~~ | `org.pangrova.metadata.checkpoint` | 已删除（2026-11-07）：无生产构造点，`checkpoints` 表保留 |
| [GeneratedValueBuildMetadataStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/GeneratedValueBuildMetadataStore.java) | `org.pangrova.metadata.generation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [GeneratedValueBuildSubmissionStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/GeneratedValueBuildSubmissionStore.java) | `org.pangrova.metadata.generation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [GenerationPublicationJob.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/GenerationPublicationJob.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [GenerationPublicationManifestDigest.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/GenerationPublicationManifestDigest.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [GenerationPublicationOutputValidator.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/GenerationPublicationOutputValidator.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcGeneratedValueDescriptorStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcGeneratedValueDescriptorStore.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcGeneratedValuePublicationStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcGeneratedValuePublicationStore.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcIndexGenerationPublicationStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcIndexGenerationPublicationStore.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcTabularExportPublicationStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/JdbcTabularExportPublicationStore.java) | `org.pangrova.metadata.generation.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcGeneratedValueBuildSubmissionStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/submission/JdbcGeneratedValueBuildSubmissionStore.java) | `org.pangrova.metadata.generation.jdbc.submission` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryObjectDependencyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/memory/relation/InMemoryObjectDependencyStore.java) | `org.pangrova.metadata.memory.relation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [RegistryTablePolicyAuthority.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/service/RegistryTablePolicyAuthority.java) | `org.pangrova.metadata.service` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TableControlRowPolicyStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/service/TableControlRowPolicyStore.java) | `org.pangrova.metadata.service` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcManagedSourceCatalog.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcManagedSourceCatalog.java) | `org.pangrova.metadata.source.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcSourceNamespace.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourceNamespace.java) | `org.pangrova.metadata.source.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcSourceObjectCleanup.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourceObjectCleanup.java) | `org.pangrova.metadata.source.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcSourceObjectRegistration.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourceObjectRegistration.java) | `org.pangrova.metadata.source.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcSourcePreparationInvocations.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc/JdbcSourcePreparationInvocations.java) | `org.pangrova.metadata.source.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcInvalidationPoller.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/cache/JdbcInvalidationPoller.java) | `org.pangrova.metadata.store.jdbc.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [DriverManagerDataSource.java](../../../pangrova-persistence/pangrova-persistence-jdbc-support/src/main/java/org/pangrova/persistence/jdbc/support/DriverManagerDataSource.java) | `org.pangrova.persistence.jdbc.support` | 已迁入 JDBC support；保留现有 SQL/连接语义 |
| [JdbcMetadataStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/JdbcMetadataStore.java) | `org.pangrova.metadata.store.jdbc.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [MetadataJsonMapper.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/MetadataJsonMapper.java) | `org.pangrova.metadata.store.jdbc.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PooledDataSourceFactory.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/PooledDataSourceFactory.java) | `org.pangrova.metadata.store.jdbc.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SchemaMigrationRunner.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog/SchemaMigrationRunner.java) | `org.pangrova.metadata.store.jdbc.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcObjectDependencyStore.java](../../../pangrova-persistence/pangrova-persistence-catalog-postgresql/src/main/java/org/pangrova/persistence/catalog/postgresql/dependency/JdbcObjectDependencyStore.java) | `org.pangrova.persistence.catalog.postgresql.dependency` | 已迁移（2026-11-07）至 Persistence Catalog PostgreSQL |
| [JdbcTenantStore.java](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/tenant/JdbcTenantStore.java) | `org.pangrova.metadata.tenant` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-operations

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [DdlJobStore.java](../../../pangrova-operations/src/main/java/org/pangrova/operations/ddl/DdlJobStore.java) | `org.pangrova.operations.ddl` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-parser

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [StatementParserRegistry.java](../../../pangrova-parser/src/main/java/org/pangrova/parser/StatementParserRegistry.java) | `org.pangrova.parser` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SourceLocationRegistry.java](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/SourceLocationRegistry.java) | `org.pangrova.parser.ast` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

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
| [InMemoryL2CacheStore.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/cache/InMemoryL2CacheStore.java) | `org.pangrova.planner.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [L2CacheStore.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/cache/L2CacheStore.java) | `org.pangrova.planner.cache` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [HepRuleRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/optimization/HepRuleRegistry.java) | `org.pangrova.planner.optimization` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [RuntimeFilterRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/runtimefilter/RuntimeFilterRegistry.java) | `org.pangrova.planner.runtimefilter` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PlanBaselineRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/stability/PlanBaselineRegistry.java) | `org.pangrova.planner.stability` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PlanCaptureRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/stability/PlanCaptureRegistry.java) | `org.pangrova.planner.stability` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CardinalityFeedbackStore.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/stats/CardinalityFeedbackStore.java) | `org.pangrova.planner.stats` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [FunctionNullabilityRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/substrait/FunctionNullabilityRegistry.java) | `org.pangrova.planner.substrait` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [StandardTypeRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/type/StandardTypeRegistry.java) | `org.pangrova.planner.type` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [NativeCapabilityRegistry.java](../../../pangrova-planner/src/main/java/org/pangrova/planner/validation/NativeCapabilityRegistry.java) | `org.pangrova.planner.validation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-scheduler

2 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [SpeculativeTaskRegistry.java](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/speculation/SpeculativeTaskRegistry.java) | `org.pangrova.scheduler.speculation` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcClusterConcurrencySlots.java](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/store/jdbc/admission/JdbcClusterConcurrencySlots.java) | `org.pangrova.scheduler.store.jdbc.admission` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-security

8 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [InMemoryRoleStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/authz/InMemoryRoleStore.java) | `org.pangrova.security.authz` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [StaticGrantStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/authz/StaticGrantStore.java) | `org.pangrova.security.authz` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [EmptyColumnMaskStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/mask/empty/EmptyColumnMaskStore.java) | `org.pangrova.security.mask.empty` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryEnrichedColumnMaskStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/mask/memory/InMemoryEnrichedColumnMaskStore.java) | `org.pangrova.security.mask.memory` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [EmptyRowPolicyStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/rowfilter/EmptyRowPolicyStore.java) | `org.pangrova.security.rowfilter` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SecretResolverRegistry.java](../../../pangrova-security/src/main/java/org/pangrova/security/secret/SecretResolverRegistry.java) | `org.pangrova.security.secret` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CurrentModelAuthority.java](../../../pangrova-security/src/main/java/org/pangrova/security/spi/jdbc/CurrentModelAuthority.java) | `org.pangrova.security.spi.jdbc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [InMemoryTenantStore.java](../../../pangrova-security/src/main/java/org/pangrova/security/tenant/InMemoryTenantStore.java) | `org.pangrova.security.tenant` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-semantic

3 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [SemanticStoreException.java](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/api/view/SemanticStoreException.java) | `org.pangrova.semantic.api.view` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SemanticViewStore.java](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/api/view/SemanticViewStore.java) | `org.pangrova.semantic.api.view` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcSemanticViewStore.java](../../../pangrova-persistence/pangrova-persistence-semantic-postgresql/src/main/java/org/pangrova/persistence/semantic/postgresql/JdbcSemanticViewStore.java) | `org.pangrova.persistence.semantic.postgresql` | 已迁移（2026-11-06）至 Persistence Semantic PostgreSQL |

## pangrova-shuffle-core

5 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [SpoolStore.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/SpoolStore.java) | `org.pangrova.shuffle.spool` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SpoolStoreContext.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/SpoolStoreContext.java) | `org.pangrova.shuffle.spool` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SpoolStoreFactory.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/SpoolStoreFactory.java) | `org.pangrova.shuffle.spool` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [LocalFsSpoolStore.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/localfs/LocalFsSpoolStore.java) | `org.pangrova.shuffle.spool.localfs` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [LocalFsSpoolStoreFactory.java](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/localfs/LocalFsSpoolStoreFactory.java) | `org.pangrova.shuffle.spool.localfs` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-shuffle-service

8 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [PostgresCommitVerifier.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresCommitVerifier.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PostgresShuffleBatchStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleBatchStore.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PostgresShuffleLifecycleTransitions.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleLifecycleTransitions.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PostgresShuffleRecords.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleRecords.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [PostgresShuffleStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/PostgresShuffleStore.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ShuffleBatchStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/ShuffleBatchStore.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ShuffleLifecycleStore.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/ShuffleLifecycleStore.java) | `org.pangrova.shuffle.server` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcTransactions.java](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/persistence/JdbcTransactions.java) | `org.pangrova.shuffle.server.persistence` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-streaming

14 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [CdcRegistrationStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/cdc/CdcRegistrationStore.java) | `org.pangrova.streaming.api.cdc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [StreamOffsetStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint/StreamOffsetStore.java) | `org.pangrova.streaming.api.checkpoint` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [StreamStateStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint/StreamStateStore.java) | `org.pangrova.streaming.api.checkpoint` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [StreamingStoreException.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint/StreamingStoreException.java) | `org.pangrova.streaming.api.checkpoint` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [WatermarkStore.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/watermark/WatermarkStore.java) | `org.pangrova.streaming.api.watermark` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [CdcRecordConverterRegistry.java](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/cdc/CdcRecordConverterRegistry.java) | `org.pangrova.streaming.cdc` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcCdcCommitStateStore.java](../../../pangrova-persistence/pangrova-persistence-streaming-postgresql/src/main/java/org/pangrova/persistence/streaming/postgresql/JdbcCdcCommitStateStore.java) | `org.pangrova.persistence.streaming.postgresql` | 已迁移（2026-11-07）至 Persistence Streaming PostgreSQL |
| [JdbcCdcRegistrationStore.java](../../../pangrova-persistence/pangrova-persistence-streaming-postgresql/src/main/java/org/pangrova/persistence/streaming/postgresql/JdbcCdcRegistrationStore.java) | `org.pangrova.persistence.streaming.postgresql` | 已迁移（2026-11-07）至 Persistence Streaming PostgreSQL |
| [JdbcStreamStateStore.java](../../../pangrova-persistence/pangrova-persistence-streaming-postgresql/src/main/java/org/pangrova/persistence/streaming/postgresql/JdbcStreamStateStore.java) | `org.pangrova.persistence.streaming.postgresql` | 已迁移（2026-11-07）至 Persistence Streaming PostgreSQL |
| [JdbcStreamOffsetStore.java](../../../pangrova-persistence/pangrova-persistence-streaming-postgresql/src/main/java/org/pangrova/persistence/streaming/postgresql/JdbcStreamOffsetStore.java) | `org.pangrova.persistence.streaming.postgresql` | 已迁移（2026-11-07）至 Persistence Streaming PostgreSQL |
| [JdbcWatermarkPositionStore.java](../../../pangrova-persistence/pangrova-persistence-streaming-postgresql/src/main/java/org/pangrova/persistence/streaming/postgresql/JdbcWatermarkPositionStore.java) | `org.pangrova.persistence.streaming.postgresql` | 已迁移（2026-11-07）至 Persistence Streaming PostgreSQL |
| ~~JdbcWatermarkStore.java~~ | `org.pangrova.streaming.store.jdbc.watermark` | 已删除（2026-11-06）：无生产调用方的 H2 专用重复实现；canonical owner 为 `JdbcWatermarkPositionStore` / `watermark_positions` |
| [InMemoryCdcRegistrationStore.java](../../../pangrova-streaming/pangrova-streaming-core/src/main/java/org/pangrova/streaming/store/memory/InMemoryCdcRegistrationStore.java) | `org.pangrova.streaming.store.memory` | 保留在 Streaming Core 的显式 in-process 实现 |
| [InMemoryWatermarkStore.java](../../../pangrova-streaming/pangrova-streaming-core/src/main/java/org/pangrova/streaming/store/memory/InMemoryWatermarkStore.java) | `org.pangrova.streaming.store.memory` | 保留在 Streaming Core 的显式 in-process 实现 |

## pangrova-testing-kit

4 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [PostgresCommitWireFault.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/PostgresCommitWireFault.java) | `org.pangrova.testing.testkit` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcMutationFault.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres/JdbcMutationFault.java) | `org.pangrova.testing.testkit.postgres` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [MigratedPostgres.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres/MigratedPostgres.java) | `org.pangrova.testing.testkit.postgres` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [SharedPostgres.java](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres/SharedPostgres.java) | `org.pangrova.testing.testkit.postgres` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-worker-core

7 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [QueryOutputBufferRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/buffer/QueryOutputBufferRegistry.java) | `org.pangrova.worker.buffer` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ExchangeSourceRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/source/ExchangeSourceRegistry.java) | `org.pangrova.worker.exchange.source` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [AiTaskLifecycleRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/execution/AiTaskLifecycleRegistry.java) | `org.pangrova.worker.execution` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [FileSpaceObjectStore.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file/FileSpaceObjectStore.java) | `org.pangrova.worker.file` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [JdbcFileDirectoryPageSource.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file/JdbcFileDirectoryPageSource.java) | `org.pangrova.worker.file` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [ObjectStoreAccessorFactory.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file/ObjectStoreAccessorFactory.java) | `org.pangrova.worker.file` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
| [TaskAdmissionRegistry.java](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/TaskAdmissionRegistry.java) | `org.pangrova.worker.task` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |

## pangrova-worker-native-bridge

1 个主源候选。

| 文件 | 当前能力包 | 状态 |
|---|---|---|
| [NativeObjectStoreBindings.java](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/write/catalog/NativeObjectStoreBindings.java) | `org.pangrova.worker.nativebridge.jni.write.catalog` | 已迁移（2026-11-07）至 Persistence AI PostgreSQL |
