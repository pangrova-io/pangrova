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

import java.util.Map;
import java.util.UUID;
import org.pangrova.batch.api.block.Block;
import org.pangrova.governance.runtime.lineage.LineageQueryEventListener;
import org.pangrova.persistence.query.postgresql.event.JdbcQueryEventListener;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.event.QueryCompletedEvent;
import org.pangrova.query.api.event.QueryCreatedEvent;
import org.pangrova.query.api.event.QueryStatistics;
import org.pangrova.query.event.QueryEventDispatcher;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.pangrova.coordinator.metadata.MaintenanceLineageListener;
import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;
import org.pangrova.governance.runtime.lineage.LineageEventEmitter;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.enums.ObjectKind;
import org.pangrova.metadata.api.relation.ObjectRef;
import org.pangrova.metadata.relation.ObjectDependencyService;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.persistence.catalog.postgresql.dependency.JdbcObjectDependencyStore;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresSchemaProjectionRepository;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.TestProjectPaths;
import org.pangrova.common.annotation.DeadCode;

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
 * <p>SPI contracts are the package roots of every {@code -api}/{@code -spi}
 * artifact declared in {@code .pangrova/modules.yaml}; adding a contract
 * artifact to the manifest enrolls it here automatically.</p>
 *
 * <p>Published independent library entry points must be constructed and exercised by
 * the explicit library acceptance test. Historic unreachable classes remain listed separately.
 * A class must leave that list once its role entry point becomes reachable.</p>
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
            "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap";

    /**
     * Known pre-existing violations tracked for resolution in P0 tasks.
     * Each entry must have a tracking issue. New additions to this list
     * require explicit justification in the PR description.
     */
    private static final Set<String> CONTRACT_PACKAGE_ROOTS = loadContractPackageRoots();

    private static final Set<String> BASELINED_VIOLATIONS = Set.of(
            "org.pangrova.persistence.coordination.memory.membership.InMemoryCoordinatorRegistry",
            "org.pangrova.coordinator.query.lifecycle.CoordinatorQueryLifecycle",
            "org.pangrova.indexing.storage.LocalFileSystemAccessor",
            "org.pangrova.planner.type.StandardTypeRegistry",
            "org.pangrova.streaming.writer.lake.LakeTableWriter",
            "org.pangrova.streaming.writer.mergetree.MergeTreeTableWriter",
            "org.pangrova.worker.resource.memory.UnifiedMemoryLedger");

    // Each independently callable component is exercised against the real database below.
    // A library class may be excluded from the process-only check only if that test constructs it.
    private static Set<String> independentLibraryImplementations;
    private static JavaClasses importedClasses;
    private static Set<String> reachableClassNames;

    @BeforeAll
    static void importClasses() {
        importedClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .withImportOption(location -> !location.asURI().toString()
                        .matches(".*-(?:tests|native-fixtures|query-fixtures)\\.jar!.*"))
                .importPackages("org.pangrova");
        reachableClassNames = computeReachableClasses();
        var client = new ClassFileImporter().importClasses(SpiWiringVerificationTest.class)
                .get(SpiWiringVerificationTest.class);
        independentLibraryImplementations = client.getConstructorCallsFromSelf().stream()
                .filter(call -> Set.of("explicitCatalogLibrariesWorkWithoutStartingAServer",
                        "explicitQueryObserversConsumePublishedEvents").contains(call.getOrigin().getName()))
                .map(call -> call.getTarget().getOwner().getName())
                .filter(importedClasses::contain)
                .filter(name -> !importedClasses.get(name).isMemberClass() && !importedClasses.get(name).isAnonymousClass())
                .filter(name -> implementsSpiInterface(importedClasses.get(name)))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Test
    @DisplayName("No new unreachable SPI implementations introduced")
    void noNewUnreachableSpiImplementations() {

        List<String> unreachableClasses = importedClasses.stream()
                .filter(SpiWiringVerificationTest::implementsSpiInterface)
                .filter(c -> !c.isInterface())
                .filter(c -> !c.isMemberClass())
                .filter(c -> !c.isAnonymousClass())
                .filter(c -> !c.getName().startsWith("org.pangrova.testing."))
                .filter(c -> !c.getSimpleName().equals("InMemoryWorkerRegistry"))
                .filter(c -> !c.isAnnotatedWith(DeadCode.class))
                .filter(c -> !reachableClassNames.contains(c.getName()))
                .filter(c -> !independentLibraryImplementations.contains(c.getName()))
                .filter(c -> !BASELINED_VIOLATIONS.contains(c.getName()))
                .map(JavaClass::getName)
                .sorted()
                .collect(Collectors.toList());

        assertThat(unreachableClasses)
                .as("SPI implementations require a reachable role caller or an exercised independent library contract")
                .isEmpty();
    }

    @Test
    @DisplayName("Baselined violations have not grown")
    void baselinedViolationsNotGrown() {

        List<String> resolvedBaseline = BASELINED_VIOLATIONS.stream()
                .filter(reachableClassNames::contains)
                .sorted()
                .collect(Collectors.toList());

        assertThat(resolvedBaseline).as("Remove newly reachable classes from the historical baseline").isEmpty();
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
                .anyMatch(i -> liesUnderContractRoot(i.getName())
                        && !i.getName().equals(org.pangrova.metadata.api.object.Asset.class.getName())
                        && !(i.getName().equals(Block.class.getName())
                                && javaClass.getPackageName().equals(Block.class.getPackageName())));
    }

    @Test
    void libraryConsumersAndSealedValuesAreDistinctFromDiscoveredRoleServices() {
        assertThat(importedClasses.get(org.pangrova.metadata.api.object.Asset.class).reflect().isSealed()).isTrue();
        var variants = org.pangrova.metadata.api.object.Asset.class.getPermittedSubclasses();
        assertThat(variants).allSatisfy(value -> {
            assertThat(value.isRecord()).isTrue();
            assertThat(implementsSpiInterface(importedClasses.get(value))).isFalse();
        });
        assertThat(implementsSpiInterface(importedClasses.get(org.pangrova.batch.api.block.DictionaryBlock.class)))
                .isFalse();
        assertThat(implementsSpiInterface(new ClassFileImporter().importClasses(ExternalBlock.class)
                .get(ExternalBlock.class))).isTrue();
        assertThat(independentLibraryImplementations).containsExactlyInAnyOrder(
                JdbcQueryEventListener.class.getName(), LineageQueryEventListener.class.getName(),
                "org.pangrova.persistence.catalog.postgresql.dependency.JdbcObjectDependencyStore",
                "org.pangrova.persistence.catalog.postgresql.projection.PostgresSchemaProjectionRepository",
                "org.pangrova.coordinator.metadata.MaintenanceLineageListener");
        assertThat(independentLibraryImplementations).allSatisfy(name -> {
            assertThat(importedClasses.contain(name)).isTrue();
            assertThat(implementsSpiInterface(importedClasses.get(name))).isTrue();
            assertThat(BASELINED_VIOLATIONS).doesNotContain(name);
        });
        importedClasses.stream().filter(c -> c.isAnnotatedWith(DeadCode.class)).forEach(c -> {
            assertThat(c.getAnnotationOfType(DeadCode.class).issueReference())
                    .as("tracked unsupported implementation " + c.getName()).isNotBlank();
            assertThat(reachableClassNames).as("remove obsolete DeadCode marker").doesNotContain(c.getName());
        });
        assertThat(importedClasses.contain("org.pangrova.metadata.testinfra.TestCatalogRegistry")).isFalse();
        assertThat(importedClasses.contain("org.pangrova.coordinator.query.execution.dispatch.dag.handler.ShuffleOutputCollector")).isFalse();
        assertThat(importedClasses.contain("org.pangrova.coordinator.query.execution.dispatch.dag.handler.RuntimeFilterStageHandler")).isFalse();
        assertThat(importedClasses.contain("org.pangrova.coordinator.query.execution.dispatch.adaptive.SettableDynamicFilter")).isFalse();
        assertThat(reachableClassNames).contains(
                "org.pangrova.ai.worker.context.WorkerAiCallbackScope",
                "org.pangrova.ai.worker.function.AiFunctionFailureProjector",
                "org.pangrova.ai.worker.invocation.WorkerAiTaskLifecycle",
                "org.pangrova.streaming.source.kafka.KafkaCdcSourceFactory");
    }

    @Test
    void explicitCatalogLibrariesWorkWithoutStartingAServer() throws java.sql.SQLException {
        var source = MigratedPostgres.create();
        observeExternalSchema(source);
        var schemas = new PostgresSchemaProjectionRepository(source);
        assertThat(schemas.loadSchemas("catalog")).containsExactly("observed");

        var store = new JdbcObjectDependencyStore(source);
        var graph = new ObjectDependencyService(store);
        var events = new CopyOnWriteArrayList<LineageEvent>();
        var parent = graphRef("parent", ObjectKind.TABLE);
        var child = graphRef("child", ObjectKind.VIEW);
        try (var emitter = new LineageEventEmitter(
                new LineageBackend() {
                    public void send(LineageEvent event) { events.add(event); }
                    public void close() {}
                })) {
            var listener = new MaintenanceLineageListener(emitter);
            graph.addMaintenanceHook(listener);
            graph.recordDependency(child, parent);
            assertThat(graph.removeMaintenanceHook(listener)).isTrue();
        }
        assertThat(store.getParentsOf(child)).containsExactly(parent);
        assertThat(events).extracting(LineageEvent::eventType)
                .containsExactly(LineageEventType.START,
                        LineageEventType.COMPLETE);
        assertThat(events.get(1).inputDatasets().get(0).name()).isEqualTo("parent");
        assertThat(events.get(1).outputDatasets().get(0).name()).isEqualTo("child");
    }

    private abstract static class ExternalBlock implements Block {}

    @Test
    void explicitQueryObserversConsumePublishedEvents() throws java.sql.SQLException {
        var source = MigratedPostgres.create();
        var events = new CopyOnWriteArrayList<LineageEvent>();
        var id = UUID.randomUUID().toString();
        var terminal = new QueryCompletedEvent(id, "SELECT 1", "alice", QueryState.FINISHED,
                10, 15, new QueryStatistics(5, 3, 1, 8), java.util.Optional.empty());
        try (var audit = new JdbcQueryEventListener(source);
             var emitter = new LineageEventEmitter(new LineageBackend() {
                 public void send(LineageEvent event) { events.add(event); }
                 public void close() {}
             })) {
            var observers = QueryEventDispatcher.of(audit, new LineageQueryEventListener(emitter));
            observers.fireQueryCreated(new QueryCreatedEvent(id, "SELECT 1", "alice", 10));
            observers.fireQueryCompleted(terminal);
        }
        try (var reopened = new JdbcQueryEventListener(source)) {
            assertThat(reopened.findByQueryId(id)).contains(terminal);
        }
        assertThat(events).extracting(LineageEvent::eventType)
                .containsExactly(LineageEventType.START, LineageEventType.COMPLETE);
    }

    private static ObjectRef graphRef(String name, ObjectKind kind) {
        return new ObjectRef("catalog", "observed", name, kind);
    }

    private static void observeExternalSchema(DataSource source) {
        var descriptor = new CatalogDescriptor("catalog",
                CatalogType.FEDERATION,
                "inventory", CatalogState.ACTIVE,
                Map.of(), "", Set.of("tenant"));
        new PostgresCatalogDeploymentRepository(source)
                .saveCatalogDescriptor(descriptor);
        var transactions = new PostgresMetadataProjectionTransactions(source);
        long observationEpoch = transactions.beginObservation(descriptor.identity());
        transactions.apply(observationEpoch, new MetadataProjectionMutation.ObserveSchemas(
                UUID.randomUUID(), descriptor.identity(), List.of("observed")));
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
        Set<String> roots = new LinkedHashSet<>(List.of("org.pangrova.parser.api", "org.pangrova.batch.api",
                "org.pangrova.file.api", "org.pangrova.planner.api", "org.pangrova.query.api",
                "org.pangrova.table.api", "org.pangrova.semantic.api"));
        boolean contractArtifact = false;
        boolean blockRoots = false;
        for (String line : lines) {
            if (line.startsWith("  ") && !line.startsWith("    ") && line.endsWith(":")) {
                String artifact = line.trim();
                artifact = artifact.substring(0, artifact.length() - 1);
                contractArtifact = artifact.endsWith("-api");
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

    private static Set<String> computeReachableClasses() {
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

            if (importedClasses.contain(current)) {
                var javaClass = importedClasses.get(current);
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
            }
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
