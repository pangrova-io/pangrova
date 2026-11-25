package org.pangrova.coordinator.indexing.ddl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.generation.GeneratedBuildTestConnectors;
import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.pangrova.connector.api.index.SnapshotPinReference;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.submission.GenerationStageGraphFactory;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.persistence.generation.api.job.SnapshotPinState;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.codec.authorization.DeferredAuthorizationScopeCodec;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL intent authority with a recording connector pin boundary. */
class DurableIndexBuildSnapshotPinTest extends ContainerTestBase {
    private static final String TENANT = "native-snapshot-tenant";
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "sales", "documents");
    private static final String SNAPSHOT = "41";
    private static final Instant NOW = Instant.parse("2026-11-25T00:00:00Z");

    private DataSource dataSource;
    private PostgresGenerationJobStore store;
    private RecordingPinProvider pins;
    private GeneratedBuildTestConnectors.SnapshotConnector connector;
    private DurableIndexBuildService service;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("native_snapshot_pin");
        store = ModelAuthorityFixtures.artifacts().builds(dataSource);
        GenerationAuthorityFixtures.provision(dataSource, TENANT, "alice", authorizationScope());
        pins = new RecordingPinProvider(store);
        connector = new GeneratedBuildTestConnectors.SnapshotConnector(Map.of(), SNAPSHOT, pins::requirePinned);
        var catalog = new org.pangrova.metadata.connector.ConnectorCatalogHandle(
                new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                        "lake", org.pangrova.metadata.api.enums.CatalogType.CONNECTOR, "test",
                        org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", Set.of()),
                connector);
        service = new DurableIndexBuildService(store,
                new GeneratedBuildTestConnectors.FixedCatalogRegistry(catalog),
                "s3://warehouse/indexes", session -> pins);
    }

    @Test
    void snapshotIsPinnedBeforeEnumerationAndBoundToDurableJob() {
        String jobId = service.submit(descriptor(), TABLE, session());

        assertThat(pins.events).containsExactly("pin", "validate", "enumerate");
        assertThat(pins.tokens).containsExactly(SNAPSHOT);
        assertThat(pins.owner).isEqualTo(new SnapshotPinOwner("index-1", 0, "submission"));
        assertThat(store.findJob(TENANT, jobId)).hasValueSatisfying(job -> {
            assertThat(job.sourceSnapshot()).isEqualTo(SNAPSHOT);
            assertThat(job.state()).isEqualTo(GenerationJobState.QUEUED);
            assertThat(job.expectedPages()).isEqualTo(3);
        });
        assertThat(store.findSnapshotPinForJob(TENANT, jobId)).hasValueSatisfying(intent -> {
            assertThat(intent.state()).isEqualTo(SnapshotPinState.PINNED);
            assertThat(intent.sourceSnapshot()).isEqualTo(SNAPSHOT);
            assertThat(intent.pinOwner()).isEqualTo(pins.owner.id());
        });
    }

    @Test
    void emptySnapshotUsesALogicalPinWithoutConnectorCalls() {
        String snapshot = org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner.EMPTY_SNAPSHOT;
        var emptyConnector = new GeneratedBuildTestConnectors.SnapshotConnector(
                Map.of(), snapshot, () -> { throw new AssertionError("empty snapshot enumerated source splits"); });
        var catalog = new org.pangrova.metadata.connector.ConnectorCatalogHandle(
                new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                        "lake", org.pangrova.metadata.api.enums.CatalogType.CONNECTOR, "test",
                        org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", Set.of()),
                emptyConnector);
        var emptyService = new DurableIndexBuildService(store,
                new GeneratedBuildTestConnectors.FixedCatalogRegistry(catalog),
                "s3://warehouse/indexes", session -> {
                    throw new AssertionError("empty snapshot invoked connector pin provider");
                });

        String jobId = emptyService.submit(descriptor(snapshot), TABLE, session());
        var job = store.findJob(TENANT, jobId).orElseThrow();
        var intent = store.findSnapshotPinForJob(TENANT, jobId).orElseThrow();

        assertThat(job.sourceSnapshot()).isEqualTo(snapshot);
        assertThat(job.expectedPages()).isZero();
        assertThat(intent.state()).isEqualTo(SnapshotPinState.PINNED);
        emptyService.validateSnapshotPin(job);
        emptyService.releaseSnapshotPin(intent);
        assertThat(store.findSnapshotPinForJob(TENANT, jobId).orElseThrow().state())
                .isEqualTo(SnapshotPinState.RELEASED);
        assertThat(emptyConnector.enumerations()).isZero();
    }

    @Test
    void legacyPublishedJobAcquiresARealPinBeforeRetention() throws Exception {
        var scope = authorizationScope();
        var page = new GenerationPage(
                "legacy-job", "legacy-page", 0, 0, 0,
                GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of("source-1", new byte[] {1})), List.of(), 3);
        var graph = GenerationStageGraphFactory.forArtifactPages(
                new byte[] {1}, new byte[] {2}, List.of(page), List.of());
        var spec = GenerationDispatchSpecFactory.create(
                "legacy-job", graph, TENANT, "alice", scope,
                new GenerationDispatchSpec.ResourceGrant(1024, 1, 1024, 60_000));
        var job = new GenerationJob(
                TENANT, "legacy-job", "index-1", GenerationOutputTypeIds.TEXT_INDEX, "0",
                "lake.sales.documents", SNAPSHOT, GenerationExecutionKind.BUILD,
                GenerationDependencySet.sourceSnapshot("lake.sales.documents", SNAPSHOT),
                GenerationEmptyPartitionPolicy.FORBID, "s3://warehouse/indexes/legacy-job",
                "pangrova.bm25.segment", 1, 0, "a".repeat(64), "writer-a", "",
                "legacy-job", 1, 0, GenerationJobState.QUEUED, 0, NOW, NOW, NOW);
        store.createJob(job, spec, List.of(page));
        String pinOwner = "index-1:generation:0:attempt:legacy-job";
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement(
                    "UPDATE artifact_build_jobs SET state='PUBLISHED' WHERE tenant_id=? AND job_id=?")) {
                statement.setString(1, TENANT);
                statement.setString(2, job.jobId());
                statement.executeUpdate();
            }
            try (var statement = connection.prepareStatement(
                    "INSERT INTO artifact_snapshot_pins (tenant_id,pin_owner,index_id,generation,attempt_id,"
                            + "source_id,source_snapshot,principal,authorization_scope_bytes,state,job_id,"
                            + "created_at,updated_at) VALUES (?,?,'index-1',0,'legacy-job',"
                            + "'lake.sales.documents',?,'alice',?,'INTENDED',?,?,?)")) {
                statement.setString(1, TENANT);
                statement.setString(2, pinOwner);
                statement.setString(3, SNAPSHOT);
                statement.setBytes(4, DeferredAuthorizationScopeCodec.encode(scope));
                statement.setString(5, job.jobId());
                statement.setTimestamp(6, java.sql.Timestamp.from(NOW));
                statement.setTimestamp(7, java.sql.Timestamp.from(NOW));
                statement.executeUpdate();
            }
        }

        assertThat(store.listSnapshotPinsForAdoption(10)).singleElement()
                .satisfies(intent -> assertThat(intent.pinOwner()).isEqualTo(pinOwner));
        assertThat(service.adoptLegacySnapshotPins()).isOne();
        assertThat(pins.events).containsExactly("pin", "validate");
        assertThat(store.findSnapshotPin(TENANT, pinOwner).orElseThrow().state())
                .isEqualTo(SnapshotPinState.PINNED);
        assertThat(store.listSnapshotPinsForAdoption(10)).isEmpty();
    }

    @Test
    void pinFailurePreventsEnumerationAndJobCreation() {
        pins.fail = true;

        assertThatThrownBy(() -> service.submit(descriptor(), TABLE, session()))
                .hasMessageContaining("pin denied");

        assertThat(pins.events).containsExactly("pin");
        assertThat(connector.enumerations()).isZero();
        assertThat(store.findJob(TENANT, DurableIndexBuildService.jobId(TENANT, descriptor()))).isEmpty();
        assertThat(store.findSnapshotPin(TENANT, new SnapshotPinOwner("index-1", 0, "submission").id()))
                .hasValueSatisfying(intent -> assertThat(intent.state().name()).isEqualTo("INTENDED"));
    }

    @Test
    void ambiguousPinResultRetriesTheSameDurableOwnerAndCreatesOneJob() {
        pins.failAfterPin = true;
        assertThatThrownBy(() -> service.submit(descriptor(), TABLE, session()))
                .hasMessageContaining("pin outcome unknown");
        SnapshotPinOwner firstOwner = pins.owner;

        pins.failAfterPin = false;
        String jobId = service.submit(descriptor(), TABLE, session());

        assertThat(pins.owner).isEqualTo(firstOwner);
        assertThat(pins.pins.get()).isEqualTo(2);
        assertThat(connector.enumerations()).isEqualTo(1);
        assertThat(store.findJob(TENANT, jobId)).isPresent();
        assertThat(store.listSnapshotPinCleanupCandidates(10, NOW.plusSeconds(1), NOW.plusSeconds(1)))
                .isEmpty();
    }

    @Test
    void revokedBuilderCannotReserveOrPinSnapshot() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM pangrova_user_roles WHERE user_id='alice'");
        }

        assertThatThrownBy(() -> service.submit(descriptor(), TABLE, session()))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(pins.pins.get()).isZero();
        assertThat(connector.enumerations()).isZero();
    }

    private static DeferredAuthorizationScope authorizationScope() {
        return DeferredAuthorizationScope.raw(TABLE, Set.of("body"), TABLE);
    }

    private static IndexDescriptor descriptor() {
        return descriptor(SNAPSHOT);
    }

    private static IndexDescriptor descriptor(String snapshot) {
        return new IndexDescriptor("index-1", "body_search", TABLE.catalog(), TABLE.schema(), TABLE.table(),
                List.of("body"), IndexType.INVERTED, Map.of("algorithm", "BM25"), IndexState.BUILDING,
                null, 0, snapshot, null, NOW, NOW, TENANT);
    }

    private static QuerySession session() {
        var user = new UserIdentity("alice", Set.of(), Map.of("tenant_id", TENANT), Instant.EPOCH);
        return new QuerySession("native-snapshot-query", Map.of(), user, TENANT, Optional.empty(),
                new OtelExecutionTraceContextFactory().capture());
    }

    private static final class RecordingPinProvider implements SnapshotPinProvider {
        private final PostgresGenerationJobStore store;
        private final List<String> events = new ArrayList<>();
        private final List<String> tokens = new ArrayList<>();
        private final AtomicInteger pins = new AtomicInteger();
        private SnapshotPinOwner owner;
        private boolean fail;
        private boolean failAfterPin;

        private RecordingPinProvider(PostgresGenerationJobStore store) {
            this.store = store;
        }

        @Override
        public void pinSnapshot(String catalog, String schema, String table, String snapshot, SnapshotPinOwner pinOwner) {
            pins.incrementAndGet();
            events.add("pin");
            tokens.add(snapshot);
            owner = pinOwner;
            assertThat(store.findSnapshotPin(TENANT, pinOwner.id())).isPresent();
            if (fail) {
                throw new IllegalStateException("pin denied");
            }
            if (failAfterPin) {
                throw new IllegalStateException("pin outcome unknown");
            }
        }

        @Override
        public void unpinSnapshot(String catalog, String schema, String table, String snapshot, SnapshotPinOwner pinOwner) {
        }

        @Override
        public List<String> getPinnedSnapshots(String catalog, String schema, String table) {
            return List.of();
        }

        @Override
        public List<SnapshotPinReference> getSnapshotPinsForIndex(String catalog, String schema, String table, String indexId) {
            return List.of();
        }

        @Override
        public void validateSnapshotPin(String catalog, String schema, String table, String snapshot, SnapshotPinOwner pinOwner) {
            events.add("validate");
        }

        private void requirePinned() {
            events.add("enumerate");
            assertThat(pins.get()).isPositive();
        }
    }
}
