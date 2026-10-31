package org.pangrova.indexing.store.jdbc;


import org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures;

import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.pangrova.metadata.databuild.jdbc.JdbcIndexGenerationPublicationStore;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildOutput;
import org.pangrova.databuild.api.DataBuildOutputScope;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.build.DataBuildPageCompletion;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.state.IndexState;
import org.pangrova.indexing.api.descriptor.IndexType;

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

    private org.pangrova.metadata.testinfra.DataBuildExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-06T00:00:00Z");
    private static final String TENANT = "tenant-1";

    private JdbcDataBuildStore buildStore;
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
        buildStore = DataBuildAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.metadata.testinfra.DataBuildExecutionFixtures(dataSource);
        publicationStore = DataBuildAuthorityFixtures.artifacts().indexPublications(dataSource);
        indexStore = new JdbcIndexMetadataStore(dataSource);
        indexStore.registerIndex(descriptor);
        DataBuildAuthorityFixtures.provision(
                dataSource, TENANT, "principal-1", dispatch().authorizationScope());
    }

    @Test
    void publishesCompleteDiskAnnBundleAndIndexPointerInOneTransaction() {
        DataBuildJob claim = completedAndClaimedJob(completeBundle());

        var manifest = publicationStore.publishClaimed(
                claim, "maintenance-a", 7, START.plusSeconds(3));

        assertThat(manifest.objectCount()).isEqualTo(3);
        assertThat(manifest.rowCount()).isEqualTo(11);
        assertThat(manifest.totalBytes()).isEqualTo(600);
        assertThat(buildStore.findJob(TENANT, "job-1").orElseThrow().state())
                .isEqualTo(DataBuildJobState.PUBLISHED);
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
                .isEqualTo(DataBuildJobState.RUNNING);
        assertThat(indexStore.findIndex("index-1").orElseThrow().state())
                .isEqualTo(IndexState.BUILDING);
        assertThat(indexStore.findActiveIndexGeneration("index-1")).isEmpty();
    }

    @Test
    void stalePublisherCannotCommitAfterLeaseTakeover() {
        DataBuildJob stale = completedAndClaimedJob(completeBundle());
        DataBuildJob current = buildStore.claimReadyForPublication(
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
                .isEqualTo(DataBuildJobState.PUBLISHED);
    }

    @Test
    void rejectsEmptySnapshotWhenJobDoesNotOptIn() {
        assertThatThrownBy(() -> buildStore.createJob(job(0), dispatch(List.of()), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty artifact generation is forbidden");
    }

    @Test
    void publishesExplicitEmptyVectorGenerationThroughTheSamePublicationFence() {
        var empty = new DataBuildJob(
                TENANT, "job-1", "index-1", DataBuildOutputTypeIds.VECTOR_INDEX,
                "0", "catalog.schema.items", "snapshot-1",
                org.pangrova.databuild.api.DataBuildExecutionKind.BUILD,
                org.pangrova.databuild.api.DataBuildDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://bucket/staging/index-1/0", "pangrova.diskann.segment", 2, 0,
                "a".repeat(64), "native-build-1", "kms/key-1", "create-index-request-1",
                0, 0, DataBuildJobState.READY_TO_PUBLISH, 0, START, START, START);
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
                .isEqualTo(DataBuildJobState.PUBLISHED);
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
                                DataBuildOutputScope.SOURCE_OBJECT, 300, 11),
                        textOutput("locators", "ROW_LOCATORS", "data-object-1", 1,
                                DataBuildOutputScope.SOURCE_OBJECT, 200, 11),
                        textOutput("stats-shard", "BM25_STATS_SHARD", "page-map", 2,
                                DataBuildOutputScope.WORK_UNIT, 100, 7)),
                START.plusSeconds(1))).isTrue();

        var finalizer = buildStore.claimNextPage(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30),
                START.plusSeconds(2)).orElseThrow();
        assertThat(finalizer.kind()).isEqualTo(DataBuildPageKind.GENERATION_FINALIZE);
        byte[] finalReport = "bm25-final-report".getBytes(StandardCharsets.UTF_8);
        assertThat(completePage(buildStore, 
                finalizer.jobId(), finalizer.pageId(), finalizer.fencingToken(),
                finalizer.ownerEpoch(), finalReport, sha256(finalReport),
                List.of(textOutput(
                        "corpus-stats", "BM25_CORPUS_STATS", "0", 0,
                        DataBuildOutputScope.GENERATION, 150, 11)),
                START.plusSeconds(3))).isTrue();

        DataBuildJob claim = buildStore.claimReadyForPublication(
                TENANT, "job-1", executions.takeOwnership(TENANT, "job-1", "maintenance-a", 7), 7, Duration.ofSeconds(30),
                START.plusSeconds(4)).orElseThrow();
        var manifest = publicationStore.publishClaimed(
                claim, "maintenance-a", 7, START.plusSeconds(5));

        assertThat(manifest.objectCount()).isEqualTo(3);
        assertThat(manifest.rowCount()).isEqualTo(11);
        assertThat(manifest.totalBytes()).isEqualTo(650);
        assertThat(buildStore.loadPublishedOutputs(
                TENANT, "job-1", java.util.Set.of("data-object-1")))
                .extracting(DataBuildOutput::objectRole)
                .containsExactly("BM25_POSTINGS", "ROW_LOCATORS", "BM25_CORPUS_STATS");
        assertThat(indexStore.findIndex("index-1").orElseThrow())
                .satisfies(index -> {
                    assertThat(index.indexType()).isEqualTo(IndexType.INVERTED);
                    assertThat(index.state()).isEqualTo(IndexState.READY);
                });
    }

    private DataBuildJob completedAndClaimedJob(List<DataBuildOutput> outputs) {
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

    private static DataBuildJob job(int pages) {
        return new DataBuildJob(
                TENANT, "job-1", "index-1", DataBuildOutputTypeIds.VECTOR_INDEX,
                "0", "catalog.schema.items", "snapshot-1",
                org.pangrova.databuild.api.DataBuildExecutionKind.BUILD,
                org.pangrova.databuild.api.DataBuildDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/index-1/0",
                "pangrova.diskann.segment", 2, 0, "a".repeat(64),
                "native-build-1", "kms/key-1", "create-index-request-1",
                pages, 0, pages == 0 ? DataBuildJobState.READY_TO_PUBLISH
                        : DataBuildJobState.QUEUED,
                0, START, START, START);
    }

    private static DataBuildJob textJob() {
        return new DataBuildJob(
                TENANT, "job-1", "index-1", DataBuildOutputTypeIds.TEXT_INDEX,
                "0", "catalog.schema.items", "snapshot-1",
                org.pangrova.databuild.api.DataBuildExecutionKind.BUILD,
                org.pangrova.databuild.api.DataBuildDependencySet.sourceSnapshot(
                        "catalog.schema.items", "snapshot-1"),
                org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy.FORBID,
                "s3://bucket/staging/index-1/0",
                "pangrova.bm25.segment", 1, 0, "a".repeat(64),
                "native-build-1", "kms/key-1", "create-index-request-1",
                2, 0, DataBuildJobState.QUEUED, 0, START, START, START);
    }

    private static DataBuildPage page() {
        return new DataBuildPage(
                "job-1", "page-0", 0, 0, 0,
                DataBuildPageKind.SOURCE_MAP,
                List.of(DataBuildSplit.of(
                        "data-object-1",
                        "canonical-v2-split".getBytes(StandardCharsets.UTF_8))),
                List.of(),
                5);
    }

    private static DataBuildPage textMapPage() {
        return new DataBuildPage(
                "job-1", "page-map", 0, 0, 0,
                DataBuildPageKind.SOURCE_MAP,
                List.of(DataBuildSplit.of(
                        "data-object-1",
                        "canonical-v2-split".getBytes(StandardCharsets.UTF_8))),
                List.of(), 5);
    }

    private static DataBuildPage textFinalPage() {
        return new DataBuildPage(
                "job-1", "page-final", 1, 0, 1,
                DataBuildPageKind.GENERATION_FINALIZE,
                List.of(), List.of("page-map"), 5);
    }

    private static DataBuildDispatchSpec dispatch() {
        return dispatch(List.of(page()));
    }

    private static DataBuildDispatchSpec dispatch(List<DataBuildPage> pages) {
        return DataBuildDispatchSpecFactory.create(
                "job-1", org.pangrova.databuild.assembly.DataBuildStageGraphFactory.forArtifactPages(
                        "substrait-plan".getBytes(StandardCharsets.UTF_8), "operator-binding".getBytes(StandardCharsets.UTF_8), pages, java.util.List.of()),
                TENANT, "principal-1",
                org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items"),
                        java.util.Set.of("embedding"),
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("catalog", "schema", "items")),
                new DataBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
    }

    private static List<DataBuildOutput> completeBundle() {
        return List.of(
                output("graph", "DISKANN_GRAPH", 0),
                output("vectors", "RAW_VECTORS", 1),
                output("locators", "ROW_LOCATORS", 2));
    }

    private static DataBuildOutput output(String id, String role, int ordinal) {
        return new DataBuildOutput(
                id, "s3://bucket/staging/index-1/0/" + id, role,
                "data-object-1", ordinal, DataBuildOutputScope.SOURCE_OBJECT,
                200, 11, "b".repeat(64), "kms/key-1");
    }

    private static DataBuildOutput textOutput(
            String id, String role, String scopeId, int ordinal,
            DataBuildOutputScope scope, long sizeBytes, long rowCount) {
        return new DataBuildOutput(
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
            JdbcDataBuildStore store,
            String jobId, String pageId, long fencingToken, long ownerEpoch,
            byte[] reportBytes, String reportSha256,
            List<DataBuildOutput> outputs, Instant now) {
        return store.completePage(new DataBuildPageCompletion(
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
