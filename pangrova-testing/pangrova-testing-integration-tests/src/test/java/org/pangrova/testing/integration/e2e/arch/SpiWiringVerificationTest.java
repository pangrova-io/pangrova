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
package org.pangrova.testing.integration.e2e.arch;

import org.pangrova.scheduler.api.placement.NoOpSchedulerControlLoop;
import org.pangrova.testing.testkit.TestProjectPaths;
import org.pangrova.persistence.jdbc.query.state.JdbcQueryStateJournal;
import org.pangrova.persistence.jdbc.query.transaction.JdbcWriteCommitJournal;
import org.pangrova.persistence.memory.query.transaction.InMemoryWriteCommitJournal;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that all SPI implementation classes are reachable from public entry
 * points (QueryManager or WorkerBootstrap) via field references, method calls,
 * constructor dependencies, or a production {@code ServiceLoader} site.
 *
 * <p>This prevents dead wiring accumulation: SPI implementations that are
 * compiled but never instantiated or referenced from any production path.</p>
 *
 * <p>SPI contracts are the package roots of central {@code pangrova-spi} and every {@code -api}/{@code -spi}
 * artifact declared in {@code .pangrova/modules.yaml}; adding a contract
 * artifact to the manifest enrolls it here automatically.</p>
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
    private static final Set<String> CONTRACT_PACKAGE_ROOTS = loadContractPackageRoots();

    private static final Set<String> BASELINED_VIOLATIONS = Set.of(
            // AI runtime providers declared in modules.yaml and loaded through
            // META-INF/services by the optional Worker AI plane; the ServiceLoader
            // site is reached only when AI execution is enabled.
            "org.pangrova.ai.worker.invocation.WorkerAiTaskLifecycle",
            "org.pangrova.ai.worker.provider.WorkerAiModelCatalog",
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
            "org.pangrova.connector.iceberg.system.IcebergRefsSystemTable",
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
            "org.pangrova.coordinator.bootstrap.DefaultAiModuleContext",
            "org.pangrova.coordinator.bootstrap.IndexingBootstrap",
            "org.pangrova.coordinator.bootstrap.ModuleIntegrationBootstrap",
            "org.pangrova.coordinator.clustering.ClusteringConnectorSession",
            "org.pangrova.persistence.memory.coordination.InMemoryCoordinatorRegistry",
            "org.pangrova.coordinator.adapter.indexing.lifecycle.IndexCommitHandler",
            "org.pangrova.coordinator.query.lifecycle.CoordinatorQueryLifecycle",
            "org.pangrova.coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager",
            "org.pangrova.coordinator.result.CoordinatorDataProducer",
            "org.pangrova.streaming.adapter.coordinator.ConnectorPageSinkCdcWriter",
            "org.pangrova.streaming.adapter.coordinator.KafkaCdcSourceReader",
            // Indexing — pending index build pipeline P0
            "org.pangrova.indexing.engine.bitmap.provider.BitmapStrategyProvider",
            "org.pangrova.indexing.engine.bloom.provider.BloomFilterStrategyProvider",
            "org.pangrova.indexing.engine.btree.provider.BTreeStrategyProvider",
            "org.pangrova.indexing.engine.gin.provider.GinStrategyProvider",
            "org.pangrova.indexing.engine.hash.provider.HashStrategyProvider",
            "org.pangrova.indexing.engine.ngram.provider.NGramBloomStrategyProvider",
            "org.pangrova.indexing.engine.prefix.provider.PrefixStrategyProvider",
            "org.pangrova.indexing.engine.rtree.provider.RTreeStrategyProvider",
            "org.pangrova.indexing.engine.sparse.provider.SparseStrategyProvider",
            "org.pangrova.indexing.core.eval.defaults.DefaultIndexFileEvaluator",
            "org.pangrova.indexing.core.eval.defaults.DefaultIndexFileEvaluatorFactory",
            "org.pangrova.indexing.adapter.fs.LocalFileSystemAccessor",
            "org.pangrova.indexing.core.storage.CachedBloomFilterIndexData",
            // Metadata — pending metadata P0 wiring
            "org.pangrova.metadata.adapter.FederationColumnHandle",
            "org.pangrova.metadata.adapter.FederationConnectorMetadata",
            "org.pangrova.metadata.adapter.FederationTableHandle",
            "org.pangrova.metadata.checkpoint.JdbcCheckpointStore",
            "org.pangrova.persistence.jdbc.coordination.JdbcCoordinatorRegistry",
            "org.pangrova.persistence.jdbc.coordination.JdbcDistributedLease",
            "org.pangrova.persistence.jdbc.query.state.JdbcQueryStateJournal",
            "org.pangrova.persistence.jdbc.coordination.JdbcWorkerRegistry",
            "org.pangrova.metadata.service.DefaultConnectorSession",
            // Resilience — checkpoint removed per streaming positioning clarification
            // Security
            "org.pangrova.security.core.mask.provider.empty.EmptyColumnMaskStore",
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
            "org.pangrova.spi.indexing.data.BTreeIndexData",
            "org.pangrova.spi.indexing.data.BloomFilterIndexData",
            "org.pangrova.spi.indexing.data.HashIndexData",
            "org.pangrova.spi.indexing.data.bitmap.BitmapIndexData",
            "org.pangrova.spi.indexing.data.spatial.RTreeIndexData",
            "org.pangrova.spi.indexing.data.spatial.SparseIndexData",
            "org.pangrova.spi.indexing.data.text.GinIndexData",
            "org.pangrova.spi.indexing.data.text.NGramBloomIndexData",
            "org.pangrova.spi.indexing.data.text.PrefixIndexData",
            // Streaming — pending streaming P0 wiring
            "org.pangrova.streaming.core.cdc.CdcRegistrationHandler",
            "org.pangrova.streaming.store.memory.InMemoryCdcRegistrationStore",
            "org.pangrova.streaming.store.jdbc.cdc.JdbcCdcRegistrationStore",
            "org.pangrova.streaming.store.jdbc.watermark.JdbcWatermarkPositionStore",
            "org.pangrova.streaming.provider.cdcformat.canal.CanalJsonConverter",
            "org.pangrova.streaming.provider.cdcformat.debezium.DebeziumJsonConverter",
            "org.pangrova.streaming.provider.cdcformat.maxwell.MaxwellJsonConverter",
            "org.pangrova.streaming.store.memory.InMemoryWatermarkStore",
            "org.pangrova.streaming.store.jdbc.watermark.JdbcWatermarkStore",
            "org.pangrova.streaming.provider.writer.lake.LakeTableWriter",
            "org.pangrova.streaming.provider.writer.mergetree.MergeTreeTableWriter",
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
            "org.pangrova.coordinator.cache.CacheEvictionBroadcaster",
            "org.pangrova.coordinator.columnar.ColumnarSessionContext",
            "org.pangrova.persistence.memory.coordination.InMemoryDistributedLease",
            "org.pangrova.persistence.memory.query.transaction.InMemoryWriteCommitJournal",
            "org.pangrova.connector.runtime.classloader.DefaultConnectorClassLoaderFactory",
            "org.pangrova.coordinator.query.execution.dispatch.adaptive.SettableDynamicFilter",
            "org.pangrova.coordinator.query.resourcegroup.ResourceGroupAdmissionController",
            // Coordinator — in-memory default StorageProfileRegistry, wired in QueryExecutionBootstrap
            // (bootstrap wiring is not traversed by the arch reachability roots).
            "org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry",
            "org.pangrova.streaming.adapter.coordinator.KafkaStreamSourceReader",
            // Governance — pending lineage wiring
            "org.pangrova.governance.core.lineage.LineageQueryEventListener",
            // Metadata — JdbcWriteCommitJournal IS wired in production (QueryBootstrap builds it as
            // the durable write-commit journal when a metadata datasource is configured — A-COMMIT-01).
            // It stays listed here only because the reachability roots (QueryManager/WorkerBootstrap)
            // do not traverse bootstrap wiring classes, same as the other Jdbc* bootstrap-wired impls.
            "org.pangrova.persistence.jdbc.query.transaction.JdbcWriteCommitJournal",
            // Metadata — pending metadata service wiring
            "org.pangrova.metadata.service.DefaultMetadataService",
            // Observability — pending event listener wiring
            "org.pangrova.persistence.jdbc.query.event.JdbcQueryEventListener",
            "org.pangrova.observability.sdk.logging.provider.mdc.MdcTaskWrapping",
            // Resilience — pending checkpoint wiring
            "org.pangrova.coordinator.resilience.checkpoint.InMemoryCheckpointStore",
            "org.pangrova.coordinator.resilience.checkpoint.InMemoryQueryCheckpoint",
            // Resource — pending memory ledger wiring
            "org.pangrova.worker.resource.memory.UnifiedMemoryLedger",
            // Security — pending RBAC/mask/tenant wiring
            "org.pangrova.security.core.authz.DynamicRbacAccessControl",
            "org.pangrova.security.core.mask.provider.memory.InMemoryEnrichedColumnMaskStore",
            "org.pangrova.persistence.jdbc.security.mask.JdbcColumnMaskStore",
            // Coordinator — no-op scheduler control loop (pending wiring)
            "org.pangrova.scheduler.api.placement.NoOpSchedulerControlLoop",
            // Coordinator — AI index lifecycle (wired via ServiceLoader)
            "org.pangrova.coordinator.adapter.indexing.ai.lifecycle.DefaultIndexAvailabilityPublisher",
            // AI Worker — optional runtime installed on the Worker classpath; its
            // ServiceLoader adapters are not reachable from the Worker bootstrap roots.
            "org.pangrova.ai.worker.embedding.provider.openai.OpenAiCompatibleEmbeddingProvider",
            "org.pangrova.ai.worker.function.AiClassifyFunction",
            "org.pangrova.ai.worker.function.AiCompletionFunction",
            "org.pangrova.ai.worker.function.AiEmbedFunction",
            "org.pangrova.ai.worker.function.AiExtractFunction",
            "org.pangrova.ai.worker.function.AiFilterFunction",
            "org.pangrova.ai.worker.function.AiFunctionFactory",
            "org.pangrova.ai.worker.function.AiGuardFunction",
            "org.pangrova.ai.worker.function.AiPredictFunction",
            "org.pangrova.ai.worker.function.AiSummarizeFunction",
            "org.pangrova.ai.worker.function.AiTranslateFunction",
            // Multimodal AI functions (wired via ServiceLoader in AiFunctionFactory)
            "org.pangrova.ai.worker.function.AiDescribeImageFunction",
            "org.pangrova.ai.worker.function.AiOcrFunction",
            "org.pangrova.ai.worker.function.AiParseDocumentFunction",
            "org.pangrova.ai.worker.function.AiTranscribeFunction",
            "org.pangrova.ai.worker.function.file.FileFromBytesFunction",
            "org.pangrova.ai.worker.function.file.FileFromUrlFunction",
            "org.pangrova.ai.worker.inference.provider.openai.OpenAiCompatibleInferenceProvider",
            "org.pangrova.ai.worker.provider.ConfiguredAiProviderRegistry"
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
                .anyMatch(i -> liesUnderContractRoot(i.getName()));
    }

    private static boolean liesUnderContractRoot(String className) {
        for (String root : CONTRACT_PACKAGE_ROOTS) {
            if (className.equals(root) || className.startsWith(root + ".")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads contract package roots from the module manifest so the gate follows
     * declared ownership instead of a hard-coded legacy package prefix.
     */
    private static Set<String> loadContractPackageRoots() {
        Path manifest = TestProjectPaths.findProjectRoot().resolve(".pangrova/modules.yaml");
        List<String> lines;
        try {
            lines = Files.readAllLines(manifest);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + manifest, e);
        }
        Set<String> roots = new LinkedHashSet<>();
        boolean contractArtifact = false;
        boolean blockRoots = false;
        for (String line : lines) {
            if (line.startsWith("  ") && !line.startsWith("    ") && line.endsWith(":")) {
                String artifact = line.trim();
                artifact = artifact.substring(0, artifact.length() - 1);
                contractArtifact = artifact.equals("pangrova-spi")
                        || artifact.endsWith("-api") || artifact.endsWith("-spi");
                blockRoots = false;
                continue;
            }
            if (!contractArtifact) {
                continue;
            }
            if (line.startsWith("    packageRoots:")) {
                String rest = line.substring("    packageRoots:".length()).trim();
                blockRoots = !rest.startsWith("[");
                if (!blockRoots && rest.length() > 2) {
                    for (String root : rest.substring(1, rest.length() - 1).split(",")) {
                        if (!root.isBlank()) {
                            roots.add(root.trim());
                        }
                    }
                }
            } else if (blockRoots && line.startsWith("      - ")) {
                roots.add(line.substring("      - ".length()).trim());
            }
        }
        return roots;
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

            ServiceLoaderReachability.enqueueProviders(
                    current, importedClasses, queue, visited);

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
