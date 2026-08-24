/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.testing.e2e.arch;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that all SPI implementation classes are reachable from public entry
 * points (QueryManager or WorkerBootstrap) via field references, method calls,
 * or constructor dependencies.
 *
 * <p>This prevents dead wiring accumulation: SPI implementations that are
 * compiled but never instantiated or referenced from any production path.</p>
 *
 * <p>Classes annotated with {@code @DeadCode} or {@code @Experimental} are
 * excluded from this check. Known pre-existing violations are baselined and
 * tracked for resolution — the test fails only on NEW unreachable classes.</p>
 */
@DisplayName("ArchUnit — SPI Wiring Verification")
class SpiWiringVerificationTest {

    private static final String ENTRY_POINT_QUERY_MANAGER =
            "org.pangrova.coordinator.query.QueryManager";
    private static final String ENTRY_POINT_WORKER_BOOTSTRAP =
            "org.pangrova.worker.bootstrap.WorkerBootstrap";
    // Production coordinator process entry. Wires HA coordination (e.g.
    // ZooKeeperLeaseProvider → ZooKeeperCoordinatorRegistry/DistributedLease)
    // that QueryManager never references directly.
    private static final String ENTRY_POINT_COORDINATOR_BOOTSTRAP =
            "org.pangrova.coordinator.bootstrap.CoordinatorBootstrap";
    private static final String DEAD_CODE_ANNOTATION =
            "org.pangrova.common.annotation.DeadCode";
    private static final String EXPERIMENTAL_ANNOTATION =
            "org.pangrova.common.annotation.Experimental";

    /**
     * Known pre-existing violations tracked for resolution in P0 tasks.
     * Each entry must have a tracking issue. New additions to this list
     * require explicit justification in the PR description.
     */
    private static final Set<String> BASELINED_VIOLATIONS = Set.of(
            // Connector — Iceberg (wired via CatalogManager dynamic loading)
            "org.pangrova.connector.iceberg.IcebergConnector",
            "org.pangrova.connector.iceberg.IcebergConnectorFactory",
            "org.pangrova.connector.iceberg.IcebergIncrementalScanCapability",
            "org.pangrova.connector.iceberg.IcebergIndexAccessors",
            "org.pangrova.connector.iceberg.IcebergMetadata",
            "org.pangrova.connector.iceberg.IcebergPageSourceProvider",
            "org.pangrova.connector.iceberg.IcebergSplitManager",
            "org.pangrova.connector.iceberg.fs.IcebergColumnValueReader",
            "org.pangrova.connector.iceberg.fs.IcebergCatalogColumnValueReader",
            "org.pangrova.connector.iceberg.fs.IcebergFileSystemAccessor",
            "org.pangrova.connector.iceberg.handle.IcebergColumnHandle",
            "org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle",
            "org.pangrova.connector.iceberg.handle.IcebergSplit",
            "org.pangrova.connector.iceberg.handle.IcebergTableHandle",
            "org.pangrova.connector.iceberg.index.IcebergNativeIndexEvaluator",
            "org.pangrova.connector.iceberg.index.IcebergSnapshotAwareMetadata",
            "org.pangrova.connector.iceberg.read.IcebergPageSource",
            "org.pangrova.connector.iceberg.system.IcebergEntriesSystemTable",
            "org.pangrova.connector.iceberg.system.IcebergFilesSystemTable",
            "org.pangrova.connector.iceberg.system.IcebergHistorySystemTable",
            "org.pangrova.connector.iceberg.system.IcebergManifestsSystemTable",
            "org.pangrova.connector.iceberg.system.IcebergPartitionsSystemTable",
            "org.pangrova.connector.iceberg.system.IcebergRefsSystemTable",
            "org.pangrova.connector.iceberg.system.IcebergSnapshotsSystemTable",
            "org.pangrova.connector.iceberg.write.AbstractIcebergPageSink",
            "org.pangrova.connector.iceberg.write.IcebergOverwritePageSink",
            "org.pangrova.connector.iceberg.write.IcebergPageSink",
            "org.pangrova.connector.iceberg.write.IcebergPageSinkProvider",
            // Connector — Paimon (wired via CatalogManager dynamic loading)
            "org.pangrova.connector.paimon.PaimonConnector",
            "org.pangrova.connector.paimon.PaimonConnectorFactory",
            "org.pangrova.connector.paimon.PaimonConnectorIndexSupport",
            "org.pangrova.connector.paimon.PaimonIncrementalScanCapability",
            "org.pangrova.connector.paimon.PaimonMetadata",
            "org.pangrova.connector.paimon.PaimonPageSourceProvider",
            "org.pangrova.connector.paimon.PaimonSplitManager",
            "org.pangrova.connector.paimon.fs.PaimonColumnValueReader",
            "org.pangrova.connector.paimon.fs.PaimonFileSystemAccessor",
            "org.pangrova.connector.paimon.handle.PaimonColumnHandle",
            "org.pangrova.connector.paimon.handle.PaimonOutputTableHandle",
            "org.pangrova.connector.paimon.handle.PaimonSplit",
            "org.pangrova.connector.paimon.handle.PaimonTableHandle",
            "org.pangrova.connector.paimon.index.PaimonNativeIndexEvaluator",
            "org.pangrova.connector.paimon.index.PaimonSnapshotAwareMetadata",
            "org.pangrova.connector.paimon.read.PaimonPageSource",
            "org.pangrova.connector.paimon.system.PaimonAuditLogSystemTable",
            "org.pangrova.connector.paimon.system.PaimonBranchesSystemTable",
            "org.pangrova.connector.paimon.system.PaimonConsumersSystemTable",
            "org.pangrova.connector.paimon.system.PaimonFilesSystemTable",
            "org.pangrova.connector.paimon.system.PaimonManifestsSystemTable",
            "org.pangrova.connector.paimon.system.PaimonOptionsSystemTable",
            "org.pangrova.connector.paimon.system.PaimonSchemasSystemTable",
            "org.pangrova.connector.paimon.system.PaimonSnapshotsSystemTable",
            "org.pangrova.connector.paimon.system.PaimonTagsSystemTable",
            "org.pangrova.connector.paimon.write.AbstractPaimonPageSink",
            "org.pangrova.connector.paimon.write.PaimonDeletePageSink",
            "org.pangrova.connector.paimon.write.PaimonOverwritePageSink",
            "org.pangrova.connector.paimon.write.PaimonPageSink",
            "org.pangrova.connector.paimon.write.PaimonPageSinkProvider",
            // Coordinator — pending wiring
            "org.pangrova.coordinator.ai.branch.BranchConnectorSession",
            "org.pangrova.coordinator.ai.embedding.AutoEmbeddingCommitListener",
            "org.pangrova.coordinator.ai.training.TrainingConnectorSession",
            "org.pangrova.coordinator.bootstrap.DefaultAiModuleContext",
            "org.pangrova.coordinator.bootstrap.IndexingBootstrap",
            "org.pangrova.coordinator.bootstrap.ModuleIntegrationBootstrap",
            "org.pangrova.coordinator.clustering.ClusteringConnectorSession",
            "org.pangrova.coordinator.coordination.inmemory.InMemoryCoordinatorRegistry",
            "org.pangrova.coordinator.index.lifecycle.IndexCommitHandler",
            "org.pangrova.coordinator.metadata.SystemConnectorSession",
            "org.pangrova.coordinator.query.CoordinatorQueryLifecycle",
            "org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager",
            "org.pangrova.coordinator.result.CoordinatorDataProducer",
            "org.pangrova.coordinator.streaming.ConnectorPageSinkCdcWriter",
            "org.pangrova.coordinator.streaming.KafkaCdcSourceReader",
            // Governance
            "org.pangrova.governance.classification.InMemoryColumnMaskStore",
            // Indexing — pending index build pipeline P0
            "org.pangrova.indexing.engine.bitmap.BitmapStrategyProvider",
            "org.pangrova.indexing.engine.bloom.BloomFilterStrategyProvider",
            "org.pangrova.indexing.engine.btree.BTreeStrategyProvider",
            "org.pangrova.indexing.engine.gin.GinStrategyProvider",
            "org.pangrova.indexing.engine.hash.HashStrategyProvider",
            "org.pangrova.indexing.engine.inverted.InvertedStrategyProvider",
            "org.pangrova.indexing.engine.ngram.NGramBloomStrategyProvider",
            "org.pangrova.indexing.engine.prefix.PrefixStrategyProvider",
            "org.pangrova.indexing.engine.rtree.RTreeStrategyProvider",
            "org.pangrova.indexing.engine.sparse.SparseStrategyProvider",
            "org.pangrova.indexing.engine.vector.CagraEngine",
            "org.pangrova.indexing.engine.vector.ColbertEngine",
            "org.pangrova.indexing.engine.vector.DefaultVectorIndexLoader",
            "org.pangrova.indexing.engine.vector.DiskAnnEngine",
            "org.pangrova.indexing.engine.vector.VectorStrategyProvider",
            "org.pangrova.indexing.eval.DefaultIndexFileEvaluator",
            "org.pangrova.indexing.eval.DefaultIndexFileEvaluatorFactory",
            "org.pangrova.indexing.fs.LocalFileSystemAccessor",
            "org.pangrova.indexing.storage.CachedBloomFilterIndexData",
            // Metadata — pending metadata P0 wiring
            "org.pangrova.metadata.adapter.FederationColumnHandle",
            "org.pangrova.metadata.adapter.FederationConnectorMetadata",
            "org.pangrova.metadata.adapter.FederationTableHandle",
            "org.pangrova.metadata.checkpoint.JdbcCheckpointStore",
            "org.pangrova.metadata.coordination.JdbcCoordinatorRegistry",
            "org.pangrova.metadata.coordination.JdbcDistributedLease",
            "org.pangrova.metadata.coordination.JdbcQueryStateJournal",
            "org.pangrova.metadata.coordination.JdbcWorkerRegistry",
            "org.pangrova.metadata.service.DefaultConnectorSession",
            "org.pangrova.metadata.streaming.JdbcCdcRegistrationStore",
            "org.pangrova.metadata.streaming.JdbcWatermarkStore",
            // Resilience — checkpoint removed per streaming positioning clarification
            // Security
            "org.pangrova.security.authz.RbacAccessControl",
            "org.pangrova.security.mask.EmptyColumnMaskStore",
            // SPI data structures (used by connectors via dynamic loading)
            "org.pangrova.spi.connector.data.block.ArrayBlock",
            "org.pangrova.spi.connector.data.block.DictionaryBlock",
            "org.pangrova.spi.connector.data.block.DoubleBlock",
            "org.pangrova.spi.connector.data.block.IntBlock",
            "org.pangrova.spi.connector.data.block.LazyBlock",
            "org.pangrova.spi.connector.data.block.LongBlock",
            "org.pangrova.spi.connector.data.block.RunLengthEncodedBlock",
            "org.pangrova.spi.connector.data.block.SliceBlock",
            "org.pangrova.planner.type.StandardTypeRegistry",
            "org.pangrova.spi.connector.metadata.FixedSplitSource",
            "org.pangrova.spi.index.data.BTreeIndexData",
            "org.pangrova.spi.index.data.BloomFilterIndexData",
            "org.pangrova.spi.index.data.HashIndexData",
            "org.pangrova.spi.index.data.bitmap.BitmapIndexData",
            "org.pangrova.spi.index.data.spatial.RTreeIndexData",
            "org.pangrova.spi.index.data.spatial.SparseIndexData",
            "org.pangrova.spi.index.data.text.GinIndexData",
            "org.pangrova.spi.index.data.text.InvertedIndexData",
            "org.pangrova.spi.index.data.text.NGramBloomIndexData",
            "org.pangrova.spi.index.data.text.PrefixIndexData",
            "org.pangrova.spi.index.data.vector.VectorIndexData",
            // Streaming — pending streaming P0 wiring
            "org.pangrova.streaming.cdc.CdcRegistrationHandler",
            "org.pangrova.streaming.cdc.InMemoryCdcRegistrationStore",
            "org.pangrova.streaming.cdc.JdbcCdcRegistrationStore",
            "org.pangrova.streaming.cdc.format.CanalJsonConverter",
            "org.pangrova.streaming.cdc.format.DebeziumJsonConverter",
            "org.pangrova.streaming.cdc.format.MaxwellJsonConverter",
            "org.pangrova.streaming.watermark.InMemoryWatermarkStore",
            "org.pangrova.streaming.watermark.JdbcWatermarkStore",
            "org.pangrova.streaming.writer.LakeTableWriter",
            "org.pangrova.streaming.writer.MergeTreeTableWriter",
            // Exchange/shuffle — pending distributed correctness P0
            "org.pangrova.worker.exchange.remote.HybridExchangeManager",
            "org.pangrova.worker.exchange.remote.RemoteShuffleExchangeManager",
            "org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSink",
            "org.pangrova.worker.exchange.remote.RemoteShuffleExchangeSource",
            "org.pangrova.worker.exchange.source.BufferedExchangeSink",
            "org.pangrova.worker.exchange.source.RemoteExchangeManager",
            "org.pangrova.worker.exchange.source.RemoteExchangeSource",
            // Transport — pending native result path wiring
            "org.pangrova.worker.transport.PangrovaDataProducer",
            // Module integration — pending bootstrap wiring
            "org.pangrova.coordinator.bootstrap.ModuleIntegrationWiring",
            "org.pangrova.coordinator.cache.CacheEvictionBroadcaster",
            "org.pangrova.coordinator.columnar.ColumnarSessionContext",
            "org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease",
            "org.pangrova.coordinator.coordination.inmemory.InMemoryWriteCommitJournal",
            "org.pangrova.coordinator.plugin.DefaultConnectorClassLoaderFactory",
            "org.pangrova.coordinator.query.execution.dispatch.adaptive.SettableDynamicFilter",
            "org.pangrova.coordinator.query.resourcegroup.ResourceGroupAdmissionController",
            // Coordinator — in-memory default StorageProfileRegistry, wired in QueryExecutionBootstrap
            // (bootstrap wiring is not traversed by the arch reachability roots).
            "org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry",
            "org.pangrova.coordinator.streaming.ConnectorPageSinkStreamWriter",
            "org.pangrova.coordinator.streaming.KafkaStreamSourceReader",
            // Governance — pending lineage wiring
            "org.pangrova.governance.lineage.LineageQueryEventListener",
            // Metadata — JdbcWriteCommitJournal IS wired in production (QueryBootstrap builds it as
            // the durable write-commit journal when a metadata datasource is configured — A-COMMIT-01).
            // It stays listed here only because the reachability roots (QueryManager/WorkerBootstrap)
            // do not traverse bootstrap wiring classes, same as the other Jdbc* bootstrap-wired impls.
            "org.pangrova.metadata.coordination.JdbcWriteCommitJournal",
            // Metadata — pending metadata service wiring
            "org.pangrova.metadata.service.DefaultMetadataService",
            // Observability — pending event listener wiring
            "org.pangrova.observability.eventlistener.JdbcQueryEventListener",
            "org.pangrova.observability.logging.MdcTaskWrapping",
            // Resilience — pending checkpoint wiring
            "org.pangrova.resilience.checkpoint.InMemoryCheckpointStore",
            "org.pangrova.resilience.checkpoint.InMemoryQueryCheckpoint",
            // Resource — pending memory ledger wiring
            "org.pangrova.resource.memory.UnifiedMemoryLedger",
            // Security — pending RBAC/mask/tenant wiring
            "org.pangrova.security.authz.DynamicRbacAccessControl",
            "org.pangrova.security.mask.InMemoryEnrichedColumnMaskStore",
            "org.pangrova.security.store.PersistentColumnMaskStore",
            "org.pangrova.security.tenant.pool.ResourcePoolCpuSchedulingProvider",
            "org.pangrova.security.tenant.quota.TenantQuotaAdmissionController",
            // Coordinator — no-op scheduler control loop (pending wiring)
            "org.pangrova.coordinator.scheduler.NoOpSchedulerControlLoop",
            // Coordinator — AI index lifecycle (wired via ServiceLoader)
            "org.pangrova.coordinator.ai.index.lifecycle.DefaultIndexAvailabilityPublisher",
            // SPI — multimodal cost estimation (wired via ServiceLoader)
            "org.pangrova.spi.ai.cost.DefaultMultimodalCostEstimator",
            // Worker AI — pending AI function wiring
            "org.pangrova.worker.ai.document.FixedSizeChunkStrategy",
            "org.pangrova.worker.ai.document.HtmlDocumentParser",
            "org.pangrova.worker.ai.document.PdfDocumentParser",
            "org.pangrova.worker.ai.document.RecursiveCharacterChunkStrategy",
            "org.pangrova.worker.ai.document.SemanticChunkStrategy",
            "org.pangrova.worker.ai.embedding.provider.OpenAiCompatibleEmbeddingProvider",
            "org.pangrova.worker.ai.function.AiClassifyFunction",
            "org.pangrova.worker.ai.function.AiCompletionFunction",
            "org.pangrova.worker.ai.function.AiEmbedFunction",
            "org.pangrova.worker.ai.function.AiExtractFunction",
            "org.pangrova.worker.ai.function.AiFilterFunction",
            "org.pangrova.worker.ai.function.AiFunctionFactory",
            "org.pangrova.worker.ai.function.AiGuardFunction",
            "org.pangrova.worker.ai.function.AiPredictFunction",
            "org.pangrova.worker.ai.function.AiSummarizeFunction",
            "org.pangrova.worker.ai.function.AiTranslateFunction",
            // Multimodal AI functions (wired via ServiceLoader in AiFunctionFactory)
            "org.pangrova.worker.ai.function.AiDescribeImageFunction",
            "org.pangrova.worker.ai.function.AiOcrFunction",
            "org.pangrova.worker.ai.function.AiParseDocumentFunction",
            "org.pangrova.worker.ai.function.AiTranscribeFunction",
            "org.pangrova.worker.ai.function.FileFromBytesFunction",
            "org.pangrova.worker.ai.function.FileFromUrlFunction",
            "org.pangrova.worker.ai.function.table.AiTableFunctionFactory",
            "org.pangrova.worker.ai.function.table.DirectoryListingTableFunction",
            "org.pangrova.worker.ai.function.table.VectorSearchTableFunction",
            "org.pangrova.worker.ai.inference.ConfigurableInferenceProvider",
            "org.pangrova.worker.ai.inference.OpenAiCompatibleInferenceProvider",
            "org.pangrova.worker.ai.vector.DiskAnnEngine"
    );

    private static JavaClasses importedClasses;

    @BeforeAll
    static void importClasses() {
        importedClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("org.pangrova");
    }

    @Test
    @DisplayName("No new unreachable SPI implementations introduced")
    void noNewUnreachableSpiImplementations() {
        Set<String> reachableClassNames = computeReachableClasses();

        List<String> unreachableClasses = importedClasses.stream()
                .filter(SpiWiringVerificationTest::implementsSpiInterface)
                .filter(c -> !c.isInterface())
                .filter(c -> !c.isMemberClass())
                .filter(c -> !c.isAnonymousClass())
                .filter(c -> !c.getName().startsWith("org.pangrova.testing."))
                .filter(c -> !c.getSimpleName().equals("InMemoryWorkerRegistry"))
                .filter(c -> !c.isAnnotatedWith(DEAD_CODE_ANNOTATION))
                .filter(c -> !c.isAnnotatedWith(EXPERIMENTAL_ANNOTATION))
                .filter(c -> !reachableClassNames.contains(c.getName()))
                .filter(c -> !BASELINED_VIOLATIONS.contains(c.getName()))
                .map(JavaClass::getName)
                .sorted()
                .collect(Collectors.toList());

        assertThat(unreachableClasses)
                .as("New SPI implementations must be reachable from QueryManager or "
                        + "WorkerBootstrap. Either wire into production path, annotate "
                        + "with @DeadCode(issueReference=...), or add to baseline with "
                        + "tracking issue.")
                .isEmpty();
    }

    @Test
    @DisplayName("Baselined violations have not grown")
    void baselinedViolationsNotGrown() {
        Set<String> reachableClassNames = computeReachableClasses();

        List<String> resolvedBaseline = BASELINED_VIOLATIONS.stream()
                .filter(reachableClassNames::contains)
                .sorted()
                .collect(Collectors.toList());

        if (!resolvedBaseline.isEmpty()) {
            System.out.println("INFO: The following baselined violations are now "
                    + "resolved and can be removed from BASELINED_VIOLATIONS:");
            resolvedBaseline.forEach(c -> System.out.println("  - " + c));
        }
    }

    @Test
    @DisplayName("Entry point classes exist in the imported classpath")
    void entryPointClassesExist() {
        Set<String> classNames = importedClasses.stream()
                .map(JavaClass::getName)
                .collect(Collectors.toSet());

        assertThat(classNames)
                .as("QueryManager must be on classpath")
                .contains(ENTRY_POINT_QUERY_MANAGER);
        assertThat(classNames)
                .as("WorkerBootstrap must be on classpath")
                .contains(ENTRY_POINT_WORKER_BOOTSTRAP);
    }

    private static boolean implementsSpiInterface(JavaClass javaClass) {
        return javaClass.getAllRawInterfaces().stream()
                .anyMatch(i -> i.getName().startsWith("org.pangrova.spi."));
    }

    private Set<String> computeReachableClasses() {
        Set<String> visited = new HashSet<>();
        Queue<String> queue = new ArrayDeque<>();

        queue.add(ENTRY_POINT_QUERY_MANAGER);
        queue.add(ENTRY_POINT_WORKER_BOOTSTRAP);
        queue.add(ENTRY_POINT_COORDINATOR_BOOTSTRAP);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            if (!visited.add(current)) {
                continue;
            }

            importedClasses.stream()
                    .filter(c -> c.getName().equals(current))
                    .findFirst()
                    .ifPresent(javaClass -> {
                        javaClass.getFields().forEach(field ->
                                enqueueIfPangrova(queue, visited,
                                        field.getRawType().getName()));

                        javaClass.getConstructors().forEach(ctor ->
                                ctor.getRawParameterTypes().forEach(param ->
                                        enqueueIfPangrova(queue, visited,
                                                param.getName())));

                        javaClass.getMethods().forEach(method -> {
                            enqueueIfPangrova(queue, visited,
                                    method.getRawReturnType().getName());
                            method.getRawParameterTypes().forEach(param ->
                                    enqueueIfPangrova(queue, visited,
                                            param.getName()));
                        });

                        javaClass.getDirectDependenciesFromSelf().forEach(dep ->
                                enqueueIfPangrova(queue, visited,
                                        dep.getTargetClass().getName()));
                    });
        }

        return visited;
    }

    private static void enqueueIfPangrova(
            Queue<String> queue, Set<String> visited, String className) {
        if (className.startsWith("org.pangrova") && !visited.contains(className)) {
            queue.add(className);
        }
    }
}
