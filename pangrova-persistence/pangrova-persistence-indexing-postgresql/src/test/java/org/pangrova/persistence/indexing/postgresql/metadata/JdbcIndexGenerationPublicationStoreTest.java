package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;

import org.pangrova.generation.submission.GenerationDispatchSpecFactory;

import org.pangrova.persistence.generation.postgresql.publication.JdbcIndexGenerationPublicationStore;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcIndexGenerationPublicationStoreTest {

    private org.pangrova.metadata.testinfra.GenerationExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-06T00:00:00Z");
    private static final String TENANT = "tenant-1";

    private PostgresGenerationJobStore buildStore;
    private JdbcIndexGenerationPublicationStore publicationStore;
    private JdbcIndexMetadataStore indexStore;

    @BeforeEach
    void setUp() throws Exception {
        initialize(descriptor());
    }

    private void initialize(IndexDescriptor descriptor) throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource("artifact_publication");
        Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration")
                .load().migrate();
        buildStore = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.metadata.testinfra.GenerationExecutionFixtures(dataSource);
        publicationStore = GenerationAuthorityFixtures.artifacts().indexPublications(dataSource);
        indexStore = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        indexStore.registerIndex(descriptor);
        GenerationAuthorityFixtures.provision(
                dataSource, TENANT, "principal-1", dispatch().authorizationScope());
    }

    @Test
    void publishesCompleteDiskAnnBundleAndIndexPointerInOneTransaction() {
        GenerationJob claim = completedAndClaimedJob(completeBundle());

        var manifest = publicationStore.publishClaimed(
                claim, "maintenance-a", 7, START.plusSeconds(3));

        assertThat(manifest.objectCount()).isEqualTo(3);
        assertThat(manifest.rowCount()).isEqualTo(11);
        assertThat(manifest.totalBytes()).isEqualTo(600);
        assertThat(buildStore.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
        assertThat(buildStore.findPublishedGeneration(
                TENANT, "index-1", "0", "snapshot-1"))
                .contains(manifest);
        assertThat(indexStore.findIndex("index-1").orElseThrow())
                .satisfies(index -> {
                    assertThat(index.state()).isEqualTo(IndexState.READY);
                    assertThat(index.generation()).isZero();
                    assertThat(index.lastIndexedSnapshotId()).isEqualTo("snapshot-1");
                });
        assertThat(indexStore.findActiveIndexGeneration("index-1").orElseThrow())
                .satisfies(generation -> {
                    assertThat(generation.manifestSha256())
                            .isEqualTo(manifest.manifestSha256());
                    assertThat(generation.objectCount()).isEqualTo(3);
                });
    }

    @Test
    void invalidBundleRollsBackEveryCatalogPointer() {
        buildStore.createJob(job(1), dispatch(), List.of(page()));
        var lease = buildStore.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30), START)
                .orElseThrow();
        byte[] report = "canonical-native-report".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> completePage(buildStore, 
                lease.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(),
                report, sha256(report), List.of(
                        output("graph", "DISKANN_GRAPH", 0),
                        output("vectors", "RAW_VECTORS", 1)),
                START.plusSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("work-page role contract");

        assertThat(buildStore.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.RUNNING);
        assertThat(indexStore.findIndex("index-1").orElseThrow().state())
                .isEqualTo(IndexState.BUILDING);
        assertThat(indexStore.findActiveIndexGeneration("index-1")).isEmpty();
    }

    @Test
    void stalePublisherCannotCommitAfterLeaseTakeover() {
        GenerationJob stale = completedAndClaimedJob(completeBundle());
        GenerationJob current = buildStore.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-b", 8), 8, Duration.ofSeconds(30),
                START.plusSeconds(40)).orElseThrow();

        assertThatThrownBy(() -> publicationStore.publishClaimed(
                stale, "maintenance-a", 7, START.plusSeconds(41)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stale or mismatched");

        var manifest = publicationStore.publishClaimed(
                current, "maintenance-b", 8, START.plusSeconds(41));
        assertThat(manifest.generation()).isEqualTo("0");
        assertThat(buildStore.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
    }

    @Test
    void rejectsEmptySnapshotWhenJobDoesNotOptIn() {
        assertThatThrownBy(() -> buildStore.createJob(job(0), dispatch(List.of()), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty artifact generation is forbidden");
    }

    @Test
    void publishesExplicitEmptyVectorGenerationThroughTheSamePublicationFence() {
        var empty = new GenerationJob(
                TENANT, "job-1", "index-1", GenerationOutputTypeIds.VECTOR_INDEX,
                "0", "catalog.schema.items", "snapshot-1",
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://bucket/staging/index-1/0", "pangrova.diskann.segment", 2, 0,
                "a".repeat(64), "native-build-1", "kms/key-1", "create-index-request-1",
                0, 0, GenerationJobState.READY_TO_PUBLISH, 0, START, START, START);
        buildStore.createJob(empty, dispatch(List.of()), List.of());
        var claim = buildStore.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30), START)
                .orElseThrow();
        var manifest = publicationStore.publishClaimed(
                claim, "maintenance-a", 7, START.plusSeconds(1));
        assertThat(manifest.objectCount()).isZero();
        assertThat(manifest.rowCount()).isZero();
        assertThat(manifest.totalBytes()).isZero();
        assertThat(indexStore.findIndex("index-1").orElseThrow().state())
                .isEqualTo(IndexState.READY);
        assertThat(buildStore.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(GenerationJobState.PUBLISHED);
    }

    @Test
    void publishesBm25SourceObjectsAndGlobalStatsAtomically() throws Exception {
        initialize(textDescriptor());
        buildStore.createJob(textJob(), dispatch(List.of(textMapPage(), textFinalPage())), List.of(textMapPage(), textFinalPage()));

        var map = buildStore.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30), START)
                .orElseThrow();
        byte[] report = "bm25-map-report".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(buildStore, 
                map.jobId(), map.pageId(), map.fencingToken(), map.ownerEpoch(),
                report, sha256(report), List.of(
                        textOutput("postings", "BM25_POSTINGS", "data-object-1", 0,
                                GenerationOutputScope.SOURCE_OBJECT, 300, 11),
                        textOutput("locators", "ROW_LOCATORS", "data-object-1", 1,
                                GenerationOutputScope.SOURCE_OBJECT, 200, 11),
                        textOutput("stats-shard", "BM25_STATS_SHARD", "page-map", 2,
                                GenerationOutputScope.WORK_UNIT, 100, 7)),
                START.plusSeconds(1))).isTrue();

        var finalizer = buildStore.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30),
                START.plusSeconds(2)).orElseThrow();
        assertThat(finalizer.kind()).isEqualTo(GenerationPageKind.GENERATION_FINALIZE);
        byte[] finalReport = "bm25-final-report".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(buildStore, 
                finalizer.jobId(), finalizer.pageId(), finalizer.fencingToken(),
                finalizer.ownerEpoch(), finalReport, sha256(finalReport),
                List.of(textOutput(
                        "corpus-stats", "BM25_CORPUS_STATS", "0", 0,
                        GenerationOutputScope.GENERATION, 150, 11)),
                START.plusSeconds(3))).isTrue();

        GenerationJob claim = buildStore.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30),
                START.plusSeconds(4)).orElseThrow();
        var manifest = publicationStore.publishClaimed(
                claim, "maintenance-a", 7, START.plusSeconds(5));

        assertThat(manifest.objectCount()).isEqualTo(3);
        assertThat(manifest.rowCount()).isEqualTo(11);
        assertThat(manifest.totalBytes()).isEqualTo(650);
        assertThat(buildStore.loadPublishedOutputs(
                TENANT, "job-1", java.util.Set.of("data-object-1")))
                .extracting(GenerationOutput::objectRole)
                .containsExactly("BM25_POSTINGS", "ROW_LOCATORS", "BM25_CORPUS_STATS");
        assertThat(indexStore.findIndex("index-1").orElseThrow())
                .satisfies(index -> {
                    assertThat(index.indexType()).isEqualTo(IndexType.INVERTED);
                    assertThat(index.state()).isEqualTo(IndexState.READY);
                });
    }

    private GenerationJob completedAndClaimedJob(List<GenerationOutput> outputs) {
        buildStore.createJob(job(1), dispatch(), List.of(page()));
        var page = buildStore.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30), START)
                .orElseThrow();
        byte[] report = "canonical-native-report".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(buildStore, 
                page.jobId(), page.pageId(), page.fencingToken(), page.ownerEpoch(),
                report, sha256(report), outputs, START.plusSeconds(1))).isTrue();
        return buildStore.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30),
                START.plusSeconds(2)).orElseThrow();
    }

    private static GenerationJob job(int pages) {
        return new GenerationJob(
                TENANT, "job-1", "index-1", GenerationOutputTypeIds.VECTOR_INDEX,
                "0", "catalog.schema.items", "snapshot-1",
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/index-1/0",
                "pangrova.diskann.segment", 2, 0, "a".repeat(64),
                "native-build-1", "kms/key-1", "create-index-request-1",
                pages, 0, pages == 0 ? GenerationJobState.READY_TO_PUBLISH
                        : GenerationJobState.QUEUED,
                0, START, START, START);
    }

    private static GenerationJob textJob() {
        return new GenerationJob(
                TENANT, "job-1", "index-1", GenerationOutputTypeIds.TEXT_INDEX,
                "0", "catalog.schema.items", "snapshot-1",
                org.pangrova.generation.api.enums.GenerationExecutionKind.BUILD,
                org.pangrova.generation.api.dependency.GenerationDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/index-1/0",
                "pangrova.bm25.segment", 1, 0, "a".repeat(64),
                "native-build-1", "kms/key-1", "create-index-request-1",
                2, 0, GenerationJobState.QUEUED, 0, START, START, START);
    }

    private static GenerationPage page() {
        return new GenerationPage(
                "job-1", "page-0", 0, 0, 0,
                GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(
                        "data-object-1",
                        "canonical-v2-split".getBytes(StandardCharsets.UTF_8))),
                List.of(),
                5);
    }

    private static GenerationPage textMapPage() {
        return new GenerationPage(
                "job-1", "page-map", 0, 0, 0,
                GenerationPageKind.SOURCE_MAP,
                List.of(GenerationSplit.of(
                        "data-object-1",
                        "canonical-v2-split".getBytes(StandardCharsets.UTF_8))),
                List.of(), 5);
    }

    private static GenerationPage textFinalPage() {
        return new GenerationPage(
                "job-1", "page-final", 1, 0, 1,
                GenerationPageKind.GENERATION_FINALIZE,
                List.of(), List.of("page-map"), 5);
    }

    private static GenerationDispatchSpec dispatch() {
        return dispatch(List.of(page()));
    }

    private static GenerationDispatchSpec dispatch(List<GenerationPage> pages) {
        return GenerationDispatchSpecFactory.create(
                "job-1", org.pangrova.generation.submission.GenerationStageGraphFactory.forArtifactPages(
                        "substrait-plan".getBytes(StandardCharsets.UTF_8), "operator-binding".getBytes(StandardCharsets.UTF_8), pages, java.util.List.of()),
                TENANT, "principal-1",
                org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static List<GenerationOutput> completeBundle() {
        return List.of(
                output("graph", "DISKANN_GRAPH", 0),
                output("vectors", "RAW_VECTORS", 1),
                output("locators", "ROW_LOCATORS", 2));
    }

    private static GenerationOutput output(String id, String role, int ordinal) {
        return new GenerationOutput(
                id, "s3://bucket/staging/index-1/0/" + id, role,
                "data-object-1", ordinal, GenerationOutputScope.SOURCE_OBJECT,
                200, 11, "b".repeat(64), "kms/key-1");
    }

    private static GenerationOutput textOutput(
            String id, String role, String scopeId, int ordinal,
            GenerationOutputScope scope, long sizeBytes, long rowCount) {
        return new GenerationOutput(
                id, "s3://bucket/staging/index-1/0/" + id, role,
                scopeId, ordinal, scope, sizeBytes, rowCount,
                "b".repeat(64), "kms/key-1");
    }

    private static IndexDescriptor descriptor() {
        return new IndexDescriptor(
                "index-1", "vector_idx", "catalog", "schema", "table",
                List.of("embedding"), IndexType.VECTOR,
                Map.of("algorithm", "DISK_ANN"), IndexState.BUILDING,
                null, 0, "snapshot-1", null, START, START, "tenant-1");
    }

    private static IndexDescriptor textDescriptor() {
        return new IndexDescriptor(
                "index-1", "text_idx", "catalog", "schema", "table",
                List.of("body"), IndexType.INVERTED,
                Map.of("algorithm", "BM25"), IndexState.BUILDING,
                null, 0, "snapshot-1", null, START, START, "tenant-1");
    }

    private boolean completePage(
            PostgresGenerationJobStore store,
            String jobId, String pageId, long fencingToken, long ownerEpoch,
            byte[] reportBytes, String reportSha256,
            List<GenerationOutput> outputs, Instant now) {
        return store.completePage(new GenerationPageCompletion(
                TENANT, jobId, pageId, fencingToken, ownerEpoch,
                reportBytes, reportSha256, outputs, now, executions.reportedOwner(TENANT, jobId, ownerEpoch)));
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value));
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }
}
