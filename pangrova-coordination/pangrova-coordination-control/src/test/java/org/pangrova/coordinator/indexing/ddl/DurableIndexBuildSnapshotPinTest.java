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
import org.pangrova.generation.api.job.GenerationSubmissionAuthority;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.submission.GenerationStageGraphFactory;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.persistence.generation.api.job.SnapshotPinIntent;
import org.pangrova.persistence.generation.api.job.SnapshotPinReservation;
import org.pangrova.persistence.generation.api.enums.SnapshotPinState;
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
    private IndexMetadataStore indexes;
    private RecordingPinProvider pins;
    private GeneratedBuildTestConnectors.SnapshotConnector connector;
    private DurableIndexBuildService service;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("native_snapshot_pin");
        store = ModelAuthorityFixtures.artifacts().builds(dataSource);
        indexes = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        indexes.registerIndex(descriptor());
        GenerationAuthorityFixtures.provision(dataSource, TENANT, "alice", authorizationScope());
        pins = new RecordingPinProvider(store);
        connector = new GeneratedBuildTestConnectors.SnapshotConnector(Map.of(), SNAPSHOT, pins::requirePinned);
        var catalog = new org.pangrova.metadata.connector.ConnectorCatalogHandle(
                new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                        "lake", org.pangrova.metadata.api.enums.CatalogType.CONNECTOR, "test",
                        org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", Set.of()),
                connector);
        service = new DurableIndexBuildService(store, indexes,
                new GeneratedBuildTestConnectors.FixedCatalogRegistry(catalog),
                "s3://warehouse/indexes", session -> pins);
    }

    @Test
    void snapshotIsPinnedBeforeEnumerationAndBoundToDurableJob() {
        String jobId = service.submit(descriptor(), TABLE, session());

        assertThat(pins.events).containsExactly("pin", "validate", "enumerate");
        assertThat(pins.tokens).containsExactly(SNAPSHOT);
        assertThat(pins.owner).isEqualTo(new SnapshotPinOwner("index-1", 0, attempt(descriptor())));
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
    void emptySnapshotUsesALogicalPinWithoutConnectorCalls() throws Exception {
        String snapshot = org.pangrova.coordinator.generation.SnapshotGenerationSplitPlanner.EMPTY_SNAPSHOT;
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "UPDATE index_descriptors SET build_snapshot_id=? WHERE index_id='index-1'")) {
            statement.setString(1, snapshot);
            statement.executeUpdate();
        }
        var emptyConnector = new GeneratedBuildTestConnectors.SnapshotConnector(
                Map.of(), snapshot, () -> { throw new AssertionError("empty snapshot enumerated source splits"); });
        var catalog = new org.pangrova.metadata.connector.ConnectorCatalogHandle(
                new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                        "lake", org.pangrova.metadata.api.enums.CatalogType.CONNECTOR, "test",
                        org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", Set.of()),
                emptyConnector);
        var emptyService = new DurableIndexBuildService(store, indexes,
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
        assertThat(emptyService.cleanupSnapshotPins()).isZero();
        assertThat(emptyService.cancel(job.artifactId(), TENANT, Instant.EPOCH)).isOne();
        assertThat(emptyService.cleanupSnapshotPins()).isOne();
        assertThat(store.findSnapshotPinForJob(TENANT, jobId).orElseThrow().state())
                .isEqualTo(SnapshotPinState.RELEASED);
        assertThat(emptyConnector.enumerations()).isZero();
    }

    @Test
    void opaqueSnapshotTokenIsPreservedThroughPinAndSplitPlanning() throws Exception {
        String snapshot = "snapshot:opaque/42";
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "UPDATE index_descriptors SET build_snapshot_id=? WHERE index_id='index-1'")) {
            statement.setString(1, snapshot);
            statement.executeUpdate();
        }
        var opaqueConnector = new GeneratedBuildTestConnectors.SnapshotConnector(
                Map.of(), snapshot, pins::requirePinned);
        var catalog = new org.pangrova.metadata.connector.ConnectorCatalogHandle(
                new org.pangrova.metadata.api.catalog.CatalogDescriptor(
                        "lake", org.pangrova.metadata.api.enums.CatalogType.CONNECTOR, "test",
                        org.pangrova.metadata.api.enums.CatalogState.ACTIVE, Map.of(), "", Set.of()),
                opaqueConnector);
        var opaqueService = new DurableIndexBuildService(store, indexes,
                new GeneratedBuildTestConnectors.FixedCatalogRegistry(catalog),
                "s3://warehouse/indexes", session -> pins);

        String jobId = opaqueService.submit(descriptor(snapshot), TABLE, session());

        assertThat(pins.tokens).containsExactly(snapshot);
        assertThat(opaqueConnector.enumerations()).isOne();
        assertThat(store.findJob(TENANT, jobId).orElseThrow().sourceSnapshot()).isEqualTo(snapshot);
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
        pins.failValidation = true;
        assertThat(service.adoptLegacySnapshotPins()).isZero();
        assertThat(store.findSnapshotPin(TENANT, pinOwner).orElseThrow().state())
                .isEqualTo(SnapshotPinState.INTENDED);
        assertThat(store.listSnapshotPinsForAdoption(10)).singleElement()
                .satisfies(intent -> assertThat(intent.pinOwner()).isEqualTo(pinOwner));

        pins.failValidation = false;
        assertThat(service.adoptLegacySnapshotPins()).isOne();
        assertThat(pins.events).containsExactly("pin", "validate", "pin", "validate");
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
        assertThat(store.findJob(TENANT, jobId(descriptor()))).isEmpty();
        assertThat(store.findSnapshotPin(TENANT, new SnapshotPinOwner("index-1", 0, attempt(descriptor())).id()))
                .hasValueSatisfying(intent -> assertThat(intent.state().name()).isEqualTo("INTENDED"));
    }

    @Test
    void validationFailureKeepsSnapshotPinRetryableAndPreventsJobCreation() {
        pins.failValidation = true;

        assertThatThrownBy(() -> service.submit(descriptor(), TABLE, session()))
                .hasMessageContaining("pin validation denied");

        assertThat(pins.events).containsExactly("pin", "validate");
        assertThat(connector.enumerations()).isZero();
        assertThat(store.findJob(TENANT, jobId(descriptor()))).isEmpty();
        assertThat(store.findSnapshotPin(TENANT, new SnapshotPinOwner("index-1", 0, attempt(descriptor())).id()))
                .hasValueSatisfying(intent -> assertThat(intent.state()).isEqualTo(SnapshotPinState.INTENDED));
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
    void maintenanceFencesAbandonedReservationWithoutInventingAnActor() {
        assertThat(service.fenceAbandonedNativeReservations(() -> false)).isZero();
        assertThat(service.fenceAbandonedNativeReservations(() -> true)).isOne();
        assertThat(service.fenceAbandonedNativeReservations(() -> true)).isZero();
        IndexDescriptor failed = indexes.findIndex("index-1").orElseThrow();
        assertThat(failed.state()).isEqualTo(IndexState.STALE);
        assertThat(failed.staleReason()).isEqualTo("native_submission_missing_durable_intent");
        assertThat(pins.events).isEmpty();
        assertThat(connector.enumerations()).isZero();

        // An explicitly authorized rebuild is still possible; no old job or pin exists.
        Instant started = Instant.now();
        assertThat(indexes.beginIndexGeneration(failed.indexId(), 1, SNAPSHOT, started,
                failed.updatedAt(), failed.state(), failed.generation())).isTrue();
        var rebuild = failed.withGeneration(1, started).withBuildSnapshotId(SNAPSHOT, started);
        assertThat(store.findJob(TENANT, service.submit(rebuild, TABLE, session()))).isPresent();
    }

    @Test
    void maintenanceRecoversTheExactPersistedUnboundSubmission() throws Exception {
        pins.failAfterPin = true;
        assertThatThrownBy(() -> service.submit(descriptor(), TABLE, session()))
                .hasMessageContaining("pin outcome unknown");
        SnapshotPinOwner owner = pins.owner;
        Instant old = Instant.now().minus(java.time.Duration.ofHours(2));
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "UPDATE artifact_snapshot_pins SET created_at=?,updated_at=? WHERE pin_owner=?")) {
            statement.setObject(1, java.time.OffsetDateTime.ofInstant(old, java.time.ZoneOffset.UTC));
            statement.setObject(2, java.time.OffsetDateTime.ofInstant(old, java.time.ZoneOffset.UTC));
            statement.setString(3, owner.id());
            statement.executeUpdate();
        }
        pins.failAfterPin = false;
        assertThat(service.fenceAbandonedNativeReservations(() -> true)).isZero();
        assertThat(service.recoverUnboundSnapshotPins(() -> false)).isZero();
        assertThat(service.recoverUnboundSnapshotPins(() -> true)).isOne();
        assertThat(service.recoverUnboundSnapshotPins(() -> true)).isZero();
        assertThat(pins.owner).isEqualTo(owner);
        assertThat(store.findSnapshotPin(TENANT, owner.id())).hasValueSatisfying(intent ->
                assertThat(intent.jobId()).isEqualTo(jobId(descriptor())));
    }

    @Test
    void onlineReplacementUsesItsExactStagingReservation() {
        service.submit(descriptor(), TABLE, session());
        Instant readyAt = NOW.plusSeconds(1);
        indexes.updateIndexState("index-1", "READY", readyAt);
        IndexDescriptor previous = indexes.findIndex("index-1").orElseThrow();
        Instant started = readyAt.plusSeconds(1);
        assertThat(indexes.beginIndexGeneration("index-1", 1, SNAPSHOT, started,
                previous.updatedAt(), previous.state(), previous.generation())).isTrue();
        IndexDescriptor replacement = previous.withGeneration(1, started).withBuildSnapshotId(SNAPSHOT, started);
        assertThat(indexes.isIndexGenerationSnapshotBuildable("index-1", 1, SNAPSHOT, started)).isTrue();
        assertThat(indexes.isIndexGenerationSnapshotBuildable("index-1", 1, "other-snapshot", started)).isFalse();
        assertThat(indexes.isIndexGenerationSnapshotBuildable("index-1", 1, SNAPSHOT, started.plusSeconds(1)))
                .isFalse();
        String jobId = service.submitReplacement(previous, replacement);
        assertThat(store.findJob(TENANT, jobId)).hasValueSatisfying(job -> {
            assertThat(job.stagingGeneration()).isEqualTo("1");
            assertThat(job.publicationExpectedUpdatedAt()).isEqualTo(started);
        });
        assertThat(indexes.findIndex("index-1").orElseThrow().generation()).isZero();
    }

    @Test
    void terminalReplacementCanRetrySameLogicalGenerationWithNewAttemptAndArtifactNamespace() throws Exception {
        service.submit(descriptor(), TABLE, session());
        indexes.updateIndexState("index-1", "READY", NOW.plusSeconds(1));
        IndexDescriptor previous = indexes.findIndex("index-1").orElseThrow();
        Instant firstStarted = NOW.plusSeconds(2);
        assertThat(indexes.beginIndexGeneration("index-1", 1, SNAPSHOT, firstStarted,
                previous.updatedAt(), previous.state(), previous.generation())).isTrue();
        var first = previous.withGeneration(1, firstStarted).withBuildSnapshotId(SNAPSHOT, firstStarted);
        String firstAttempt = attempt(first);
        String firstJobId = service.submitReplacement(previous, first);
        SnapshotPinOwner firstOwner = pins.owner;
        var firstJob = store.findJob(TENANT, firstJobId).orElseThrow();
        int firstEnumerations = connector.enumerations();
        assertThat(service.submitReplacement(previous, first)).isEqualTo(firstJobId);
        assertThat(connector.enumerations()).isEqualTo(firstEnumerations);

        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='FAILED',updated_at=? WHERE tenant_id=? AND job_id=?")) {
            statement.setObject(1, java.time.OffsetDateTime.ofInstant(NOW.plusSeconds(3), java.time.ZoneOffset.UTC));
            statement.setString(2, TENANT);
            statement.setString(3, firstJobId);
            assertThat(statement.executeUpdate()).isOne();
        }
        assertThat(indexes.failIndexGeneration("index-1", 1, firstStarted, NOW.plusSeconds(3), "failed")).isTrue();
        Instant secondStarted = firstStarted;
        assertThat(indexes.beginIndexGeneration("index-1", 1, SNAPSHOT, secondStarted,
                previous.updatedAt(), previous.state(), previous.generation())).isTrue();
        var second = previous.withGeneration(1, secondStarted).withBuildSnapshotId(SNAPSHOT, secondStarted);
        String secondAttempt = attempt(second);
        assertThat(indexes.failNativeSubmissionAttempt("index-1", 1, firstAttempt,
                firstStarted, NOW.plusSeconds(5), "obsolete cleanup")).isFalse();
        String secondJobId = service.submitReplacement(previous, second);
        var secondJob = store.findJob(TENANT, secondJobId).orElseThrow();

        assertThat(secondAttempt).isNotEqualTo(firstAttempt);
        assertThat(secondJobId).isNotEqualTo(firstJobId);
        assertThat(secondJob.stagingGeneration()).isEqualTo(firstJob.stagingGeneration());
        assertThat(secondJob.stagingUri()).isNotEqualTo(firstJob.stagingUri())
                .endsWith("/attempt-" + secondAttempt + "/generation-1");
        assertThat(secondJob.idempotencyKey()).isNotEqualTo(firstJob.idempotencyKey());
        assertThat(pins.owner).isNotEqualTo(firstOwner);
        assertThat(store.findSnapshotPinForJob(TENANT, firstJobId)).hasValueSatisfying(old ->
                assertThat(old.pinOwner()).isEqualTo(firstOwner.id()));
        assertThat(store.findSnapshotPinForJob(TENANT, secondJobId)).hasValueSatisfying(current ->
                assertThat(current.pinOwner()).isEqualTo(pins.owner.id()));
        assertThat(store.listSnapshotPinCleanupCandidates(20, Instant.now(), Instant.now()))
                .extracting(SnapshotPinIntent::pinOwner).contains(firstOwner.id()).doesNotContain(pins.owner.id());
        assertThat(service.submitReplacement(previous, second)).isEqualTo(secondJobId);
    }

    @Test
    void oldUnboundAttemptDoesNotRecoverOrBlockNewReservation() throws Exception {
        service.submit(descriptor(), TABLE, session());
        indexes.updateIndexState("index-1", "READY", NOW.plusSeconds(1));
        var previous = indexes.findIndex("index-1").orElseThrow();
        Instant firstStarted = NOW.plusSeconds(2);
        assertThat(indexes.beginIndexGeneration("index-1", 1, SNAPSHOT, firstStarted,
                previous.updatedAt(), previous.state(), previous.generation())).isTrue();
        var first = previous.withGeneration(1, firstStarted).withBuildSnapshotId(SNAPSHOT, firstStarted);
        pins.failAfterPin = true;
        assertThatThrownBy(() -> service.submitReplacement(previous, first))
                .hasMessageContaining("pin outcome unknown");
        var old = pins.owner;
        assertThat(indexes.failIndexGeneration("index-1", 1, firstStarted, NOW.plusSeconds(3), "failed")).isTrue();
        Instant secondStarted = NOW.plusSeconds(4);
        assertThat(indexes.beginIndexGeneration("index-1", 1, SNAPSHOT, secondStarted,
                previous.updatedAt(), previous.state(), previous.generation())).isTrue();
        var second = previous.withGeneration(1, secondStarted).withBuildSnapshotId(SNAPSHOT, secondStarted);
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE artifact_snapshot_pins SET created_at=?,updated_at=? WHERE pin_owner=?")) {
            var oldTime = java.time.OffsetDateTime.ofInstant(NOW, java.time.ZoneOffset.UTC);
            statement.setObject(1, oldTime);
            statement.setObject(2, oldTime);
            statement.setString(3, old.id());
            assertThat(statement.executeUpdate()).isOne();
        }
        pins.failAfterPin = false;
        assertThat(service.recoverUnboundSnapshotPins(() -> true)).isZero();
        assertThat(store.listSnapshotPinCleanupCandidates(20, Instant.now(), Instant.now()))
                .extracting(SnapshotPinIntent::pinOwner).contains(old.id());
        assertThat(indexes.listAbandonedNativeReservations(20, Instant.now()))
                .extracting(candidate -> candidate.attemptId()).contains(attempt(second));
        String newJobId = service.submitReplacement(previous, second);
        assertThat(pins.owner).isNotEqualTo(old);
        assertThat(store.findSnapshotPinForJob(TENANT, newJobId)).isPresent();
        assertThat(service.fenceAbandonedNativeReservations(() -> true)).isZero();
        assertThat(service.cleanupSnapshotPins()).isOne();
        assertThat(store.findSnapshotPin(TENANT, old.id())).hasValueSatisfying(intent ->
                assertThat(intent.state()).isEqualTo(SnapshotPinState.RELEASED));
        assertThat(store.findSnapshotPinForJob(TENANT, newJobId)).hasValueSatisfying(intent ->
                assertThat(intent.state()).isEqualTo(SnapshotPinState.PINNED));
    }

    private String jobId(IndexDescriptor descriptor) {
        return DurableIndexBuildService.jobId(TENANT, descriptor, attempt(descriptor));
    }

    private String attempt(IndexDescriptor descriptor) {
        return indexes.findIndexGenerationSubmissionAttempt(
                descriptor.indexId(), descriptor.generation(), descriptor.buildSnapshotId()).orElseThrow();
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

    @Test
    void droppedDescriptorCannotCreateANewNativeJobOrPin() {
        indexes.dropIndex("index-1", NOW.plusSeconds(1));

        assertThatThrownBy(() -> service.submit(descriptor(), TABLE, session()))
                .hasMessageContaining("no longer buildable");

        assertThat(pins.pins.get()).isZero();
        assertThat(connector.enumerations()).isZero();
        assertThat(store.findJob(TENANT, jobId(descriptor()))).isEmpty();
    }

    @Test
    void snapshotPinnedJobRejectsInvalidAdmissionBeforeBinding() {
        var intent = reservePinnedPin("invalid-admission");
        var job = emptyJob("invalid-admission-job", SNAPSHOT,
                GenerationJobState.QUEUED, GenerationEmptyPartitionPolicy.FORBID);

        assertThatThrownBy(() -> store.createSnapshotPinnedJob(
                job, emptySpec(job.jobId()), List.of(), intent.pinOwner()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty artifact generation is forbidden");

        assertThat(store.findJob(TENANT, job.jobId())).isEmpty();
        assertThat(store.findSnapshotPin(TENANT, intent.pinOwner()).orElseThrow().jobId()).isNull();
    }

    @Test
    void snapshotPinnedJobRequiresExactSourceAndGenerationIdentity() {
        var intent = reservePinnedPin("mismatched-source");
        var job = emptyJob("mismatched-source-job", "another-snapshot",
                GenerationJobState.READY_TO_PUBLISH, GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS);

        assertThatThrownBy(() -> store.createSnapshotPinnedJob(
                job, emptySpec(job.jobId()), List.of(), intent.pinOwner()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exact PINNED snapshot intent");

        assertThat(store.findJob(TENANT, job.jobId())).isEmpty();
        assertThat(store.findSnapshotPin(TENANT, intent.pinOwner()).orElseThrow().jobId()).isNull();
    }

    private SnapshotPinIntent reservePinnedPin(String attempt) {
        var authority = new GenerationSubmissionAuthority(TENANT, "alice", authorizationScope());
        var intent = store.reserveSnapshotPin(new SnapshotPinReservation(
                TENANT, "index-1", 0, attempt, "lake.sales.documents", SNAPSHOT, authority, NOW));
        assertThat(store.markSnapshotPinPinned(TENANT, intent.pinOwner(), NOW)).isTrue();
        return intent;
    }

    private static GenerationJob emptyJob(
            String jobId, String snapshot, GenerationJobState state,
            GenerationEmptyPartitionPolicy emptyPartitionPolicy) {
        return new GenerationJob(
                TENANT, jobId, "index-1", GenerationOutputTypeIds.TEXT_INDEX, "0",
                "lake.sales.documents", snapshot, GenerationExecutionKind.BUILD,
                GenerationDependencySet.sourceSnapshot("lake.sales.documents", snapshot),
                emptyPartitionPolicy, "s3://warehouse/indexes/" + jobId,
                "pangrova.bm25.segment", 1, 0, "a".repeat(64), "writer-a", "",
                jobId, 0, 0, state, 0, NOW, NOW, NOW);
    }

    private static GenerationDispatchSpec emptySpec(String jobId) {
        var graph = GenerationStageGraphFactory.forArtifactPages(
                new byte[] {1}, new byte[] {2}, List.of(), List.of());
        return GenerationDispatchSpecFactory.create(
                jobId, graph, TENANT, "alice", authorizationScope(),
                new GenerationDispatchSpec.ResourceGrant(1024, 1, 1024, 60_000));
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
        private boolean failValidation;

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
            if (failValidation) {
                throw new IllegalStateException("pin validation denied");
            }
        }

        private void requirePinned() {
            events.add("enumerate");
            assertThat(pins.get()).isPositive();
        }
    }
}
