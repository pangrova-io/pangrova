package org.pangrova.indexing.artifact.jdbc;

import org.pangrova.indexing.store.jdbc.ArtifactExecutionFence;
import org.pangrova.indexing.store.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.indexing.store.jdbc.JdbcIndexArtifactPublicationStore;
import org.pangrova.indexing.store.jdbc.JdbcTabularExportPublicationStore;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.indexing.control.ArtifactPublicationStore;
import org.pangrova.indexing.api.ArtifactGenerationManifest;
import org.pangrova.indexing.api.GeneratedValueArtifactDescriptor;
import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.ArtifactBuildJob;
import org.pangrova.indexing.control.ArtifactBuildJobKey;
import org.pangrova.indexing.control.ArtifactBuildJobState;
import org.pangrova.indexing.control.ArtifactBuildOutput;
import org.pangrova.indexing.control.ArtifactBuildPage;
import org.pangrova.indexing.control.ArtifactBuildSplit;
import org.pangrova.indexing.control.ArtifactExecutionOwner;
import org.pangrova.indexing.control.ArtifactPageCompletion;
import org.pangrova.indexing.control.ArtifactStageGraph;
import org.pangrova.indexing.control.ArtifactStageTemplate;
import org.pangrova.indexing.api.ArtifactBuildOutputScope;
import org.pangrova.indexing.api.ArtifactBuildPageKind;
import org.pangrova.indexing.api.ArtifactDependencySet;
import org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy;
import org.pangrova.indexing.api.ArtifactExecutionKind;
import org.pangrova.indexing.api.ArtifactTypeIds;
import org.pangrova.indexing.store.jdbc.JdbcArtifactBuildStore;
import org.pangrova.query.transaction.JdbcExecutionAnchorStore;
import org.pangrova.indexing.store.jdbc.JdbcIndexMetadataStore;
import org.pangrova.indexing.testinfra.ArtifactAuthorityTestDoubles;
import org.pangrova.indexing.testinfra.SharedPostgresContainer;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL publishers and metadata transitions; no mocked clock, provider or execution store. */
class ArtifactPublicationFenceLockIntegrationTest {
    enum Kind { INDEX, GENERATED_VALUES, TABULAR_EXPORT }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void publisherRollsBackAllPointersWhenJobLockWaitOutlivesExecutionLease(Kind kind) throws Exception {
        var fixture = Fixture.create(kind);
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = fixture.dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            lockJob(blocker);
            int blockerPid = ArtifactExecutionFenceLockIntegrationTest.backendPid(blocker);
            var result = executor.submit(() -> fixture.publisher.publishClaimed(
                    fixture.claim, "publisher", 1, fixture.publicationTime));
            ArtifactExecutionFenceLockIntegrationTest.awaitBlockedBeforeExpiry(
                    fixture.dataSource, blockerPid, fixture.anchor);
            ArtifactExecutionFenceLockIntegrationTest.waitUntilExpired(blocker, fixture.anchor);
            blocker.commit();
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ArtifactExecutionFence.LostOwnerException.class);
            assertThat(fixture.buildStore.findJob("tenant", "job").orElseThrow().state())
                    .isEqualTo(ArtifactBuildJobState.READY_TO_PUBLISH);
            assertThat(fixture.buildStore.findPublishedGeneration("tenant", "artifact", "0", "snapshot"))
                    .isEmpty();
            fixture.assertPointerUnpublished();
            // Rollback preserves the exact expired execution identity; it does not invent takeover success.
            assertThat(fixture.anchors.current(fixture.anchor.executionId())).isEmpty();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(Kind.class)
    void publisherCommitsAllPointersWhenJobLockWaitEndsBeforeExecutionExpiry(Kind kind) throws Exception {
        var fixture = Fixture.create(kind);
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = fixture.dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            lockJob(blocker);
            var result = executor.submit(() -> fixture.publisher.publishClaimed(
                    fixture.claim, "publisher", 1, fixture.publicationTime));
            ArtifactExecutionFenceLockIntegrationTest.awaitBlockedBeforeExpiry(
                    fixture.dataSource, ArtifactExecutionFenceLockIntegrationTest.backendPid(blocker), fixture.anchor);
            blocker.commit();
            var manifest = result.get(10, TimeUnit.SECONDS);
            assertThat(manifest.objectCount()).isEqualTo(kind == Kind.INDEX ? 3 : kind == Kind.GENERATED_VALUES ? 2 : 1);
            assertThat(manifest.rowCount()).isEqualTo(4);
            assertThat(fixture.buildStore.findJob("tenant", "job").orElseThrow().state())
                    .isEqualTo(ArtifactBuildJobState.PUBLISHED);
            assertThat(fixture.buildStore.findPublishedGeneration("tenant", "artifact", "0", "snapshot"))
                    .contains(manifest);
            fixture.assertPointerPublished();
            fixture.assertStoredGeneration(manifest);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Kind.class, names = {"INDEX", "GENERATED_VALUES"})
    void publisherRollsBackWhenDescriptorLockWaitOutlivesExecutionLease(Kind kind) throws Exception {
        var fixture = Fixture.create(kind);
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = fixture.dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) {
                statement.execute(kind == Kind.INDEX
                        ? "SELECT index_id FROM index_descriptors WHERE index_id='artifact' FOR UPDATE"
                        : "SELECT artifact_id FROM generated_value_artifacts "
                                + "WHERE tenant_id='tenant' AND artifact_id='artifact' FOR UPDATE");
            }
            var result = executor.submit(() -> fixture.publisher.publishClaimed(
                    fixture.claim, "publisher", 1, fixture.publicationTime));
            ArtifactExecutionFenceLockIntegrationTest.awaitBlockedBeforeExpiry(
                    fixture.dataSource, ArtifactExecutionFenceLockIntegrationTest.backendPid(blocker), fixture.anchor);
            ArtifactExecutionFenceLockIntegrationTest.waitUntilExpired(blocker, fixture.anchor);
            blocker.commit();
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ArtifactExecutionFence.LostOwnerException.class);
            assertThat(fixture.buildStore.findJob("tenant", "job").orElseThrow().state())
                    .isEqualTo(ArtifactBuildJobState.READY_TO_PUBLISH);
            assertThat(fixture.buildStore.findPublishedGeneration("tenant", "artifact", "0", "snapshot")).isEmpty();
            fixture.assertPointerUnpublished();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void lockJob(Connection connection) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.execute("SELECT job_id FROM artifact_build_jobs WHERE tenant_id='tenant' AND job_id='job' FOR UPDATE");
        }
    }

    static final class Fixture {
        final DataSource dataSource;
        final Kind kind;
        final JdbcArtifactBuildStore buildStore;
        final JdbcExecutionAnchorStore anchors;
        final ArtifactPublicationStore publisher;
        final ArtifactBuildJob claim;
        final ExecutionAnchor anchor;
        final Instant publicationTime;
        final JdbcIndexMetadataStore indexes;

        private Fixture(DataSource dataSource, Kind kind, JdbcArtifactBuildStore buildStore,
                JdbcExecutionAnchorStore anchors, ArtifactPublicationStore publisher, ArtifactBuildJob claim,
                ExecutionAnchor anchor, Instant publicationTime, JdbcIndexMetadataStore indexes) {
            this.dataSource = dataSource;
            this.kind = kind;
            this.buildStore = buildStore;
            this.anchors = anchors;
            this.publisher = publisher;
            this.claim = claim;
            this.anchor = anchor;
            this.publicationTime = publicationTime;
            this.indexes = indexes;
        }

        static Fixture create(Kind kind) throws Exception {
            var dataSource = SharedPostgresContainer.migratedDataSource("artifact_final_clock");
            ArtifactAuthorityTestDoubles.provisionTenant(dataSource, "tenant", "builder");
            Instant start = ArtifactExecutionFenceLockIntegrationTest.dbNow(dataSource);
            var source = new QualifiedTableName("lake", "raw", "items");
            var scope = DeferredAuthorizationScope.raw(source, Set.of("embedding"), source);
            boolean export = kind == Kind.TABULAR_EXPORT;
            String type = kind == Kind.INDEX ? ArtifactTypeIds.VECTOR_INDEX
                    : export ? ArtifactTypeIds.TABULAR_EXPORT : ArtifactTypeIds.GENERATED_COLUMN_VALUES;
            String format = kind == Kind.INDEX ? "pangrova.diskann.segment"
                    : export ? "pangrova.tabular-export.columnar-stream" : "pangrova.generated-values.segment";
            var page = new ArtifactBuildPage("job", "page", 0, 0, 0,
                    export ? ArtifactBuildPageKind.RELATIONAL_PARTITION : ArtifactBuildPageKind.SOURCE_MAP,
                    export ? List.of() : List.of(ArtifactBuildSplit.of("source-object", new byte[] {1, 2, 3})),
                    List.of(), 3);
            var graph = export ? new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION,
                    ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                    List.of(ArtifactStageGraph.StagePlan.of(0, 1, page.kind(), new byte[] {1},
                            ArtifactStageTemplate.of(new byte[] {2}, List.of(), List.of()), List.of(), List.of(), true)),
                    List.of(), List.of())
                    : ArtifactStageGraph.forArtifactPages(new byte[] {1}, new byte[] {2}, List.of(page), List.of());
            var dispatch = ArtifactBuildDispatchSpec.of("job", graph, "tenant", "builder", scope,
                    new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000));
            var job = new ArtifactBuildJob("tenant", "job", "artifact", type, "0", "lake.raw.items", "snapshot",
                    export ? ArtifactExecutionKind.EXPORT : ArtifactExecutionKind.BUILD,
                    ArtifactDependencySet.sourceSnapshot("lake.raw.items", "snapshot"), ArtifactEmptyPartitionPolicy.FORBID,
                    "s3://warehouse/staging/job", format, export ? 1 : 2, 0, "a".repeat(64), "native-build", "", "request",
                    1, 0, ArtifactBuildJobState.QUEUED, 0, start, start, start);
            var buildStore = new JdbcArtifactBuildStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
            var anchors = new JdbcExecutionAnchorStore(dataSource);
            JdbcIndexMetadataStore indexes = null;
            ArtifactPublicationStore publisher;
            if (kind == Kind.INDEX) {
                indexes = new JdbcIndexMetadataStore(dataSource);
                indexes.registerIndex(new IndexDescriptor("artifact", "vector_index", "lake", "raw", "items",
                        List.of("embedding"), IndexType.VECTOR, Map.of("algorithm", "DISK_ANN"), IndexState.BUILDING,
                        null, 0, "snapshot", null, start, start, "tenant"));
                publisher = new JdbcIndexArtifactPublicationStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
            } else if (kind == Kind.GENERATED_VALUES) {
                var generated = new JdbcGeneratedValueArtifactPublicationStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
                generated.register(new GeneratedValueArtifactDescriptor("tenant", "artifact", "lake.raw.items",
                        "lake", "raw", "items", "embedding", "d".repeat(64), "0", "snapshot", start));
                publisher = generated;
            } else {
                publisher = new JdbcTabularExportPublicationStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
            }
            buildStore.createJob(job, dispatch, List.of(page));
            var key = new ArtifactBuildJobKey("tenant", "job");
            var producer = anchors.tryAcquire(key.executionId(), "producer", "producer-process", Duration.ofSeconds(30))
                    .orElseThrow();
            var lease = buildStore.claimNextPage("tenant", "job", producer, 1, Duration.ofSeconds(30), start).orElseThrow();
            byte[] report = "metadata-publication-report".getBytes(StandardCharsets.UTF_8);
            List<String> roles = kind == Kind.INDEX ? List.of("DISKANN_GRAPH", "RAW_VECTORS", "ROW_LOCATORS")
                    : export ? List.of("TABULAR_DATA") : List.of("GENERATED_VALUES", "ROW_LOCATORS");
            var outputs = new java.util.ArrayList<ArtifactBuildOutput>();
            for (int ordinal = 0; ordinal < roles.size(); ordinal++) {
                outputs.add(new ArtifactBuildOutput("output-" + ordinal, "s3://warehouse/staging/job/output-" + ordinal,
                        roles.get(ordinal), export ? "page" : "source-object", ordinal,
                        export ? ArtifactBuildOutputScope.WORK_UNIT : ArtifactBuildOutputScope.SOURCE_OBJECT,
                        32, 4, "b".repeat(64), ""));
            }
            assertThat(buildStore.completePage(new ArtifactPageCompletion("tenant", "job", lease.pageId(),
                    lease.fencingToken(), lease.ownerEpoch(), report, DigestUtils.sha256Hex(report), outputs, start,
                    ArtifactExecutionOwner.from(producer)))).isTrue();
            assertThat(anchors.yield(producer)).isTrue();
            var anchor = anchors.tryAcquire(key.executionId(), "publisher", "publisher-process", Duration.ofSeconds(5))
                    .orElseThrow();
            Instant publicationTime = ArtifactExecutionFenceLockIntegrationTest.dbNow(dataSource);
            var claim = buildStore.claimReadyForPublication("tenant", "job", anchor, 1,
                    Duration.ofSeconds(30), publicationTime).orElseThrow();
            return new Fixture(dataSource, kind, buildStore, anchors, publisher, claim, anchor, publicationTime, indexes);
        }

        void assertPointerUnpublished() throws Exception {
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "SELECT COUNT(*) FROM " + generationTable() + " WHERE " + generationKey() + "=?")) {
                statement.setString(1, "artifact");
                try (var row = statement.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    assertThat(row.getLong(1)).isZero();
                }
            }
            if (kind == Kind.INDEX) {
                assertThat(indexes.findIndex("artifact").orElseThrow().state()).isEqualTo(IndexState.BUILDING);
                assertThat(indexes.findActiveIndexGeneration("artifact")).isEmpty();
            } else if (kind == Kind.GENERATED_VALUES) {
                assertThat(((JdbcGeneratedValueArtifactPublicationStore) publisher)
                        .findGeneration("tenant", "lake.raw.items", "embedding", "snapshot")).isEmpty();
                assertThat(descriptorEpoch()).isZero();
            }
        }

        void assertPointerPublished() throws Exception {
            if (kind == Kind.INDEX) {
                assertThat(indexes.findIndex("artifact").orElseThrow().state()).isEqualTo(IndexState.READY);
                assertThat(indexes.findActiveIndexGeneration("artifact")).isPresent();
            } else if (kind == Kind.GENERATED_VALUES) {
                assertThat(((JdbcGeneratedValueArtifactPublicationStore) publisher)
                        .findGeneration("tenant", "lake.raw.items", "embedding", "snapshot")).isPresent();
                assertThat(descriptorEpoch()).isOne();
            }
        }

        void assertStoredGeneration(ArtifactGenerationManifest manifest) throws Exception {
            assertThat(manifest.manifestUri()).isEqualTo("metadata://artifact-generation/" + manifest.manifestSha256());
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "SELECT source_id,source_snapshot,manifest_sha256,object_count,row_count,total_bytes,published_at"
                            + (kind == Kind.TABULAR_EXPORT ? ",manifest_uri" : "")
                            + " FROM " + generationTable() + " WHERE " + generationKey()
                            + "=? AND generation::text=? AND state='ACTIVE'")) {
                statement.setString(1, manifest.artifactId());
                statement.setString(2, manifest.generation());
                try (var row = statement.executeQuery()) {
                    assertThat(row.next()).isTrue();
                    assertThat(row.getString("source_id")).isEqualTo(manifest.sourceId());
                    assertThat(row.getString("source_snapshot")).isEqualTo(manifest.sourceSnapshot());
                    assertThat(row.getString("manifest_sha256")).isEqualTo(manifest.manifestSha256());
                    assertThat(row.getLong("object_count")).isEqualTo(manifest.objectCount());
                    assertThat(row.getLong("row_count")).isEqualTo(manifest.rowCount());
                    assertThat(row.getLong("total_bytes")).isEqualTo(manifest.totalBytes());
                    assertThat(row.getTimestamp("published_at").toInstant()).isEqualTo(manifest.publishedAt());
                    if (kind == Kind.TABULAR_EXPORT) {
                        assertThat(row.getString("manifest_uri")).isEqualTo(manifest.manifestUri());
                    }
                    assertThat(row.next()).isFalse();
                }
            }
        }

        private String generationTable() {
            return switch (kind) {
                case INDEX -> "index_generation_manifests";
                case GENERATED_VALUES -> "generated_value_generation_manifests";
                case TABULAR_EXPORT -> "tabular_export_generations";
            };
        }

        private String generationKey() {
            return kind == Kind.INDEX ? "index_id" : "artifact_id";
        }

        private long descriptorEpoch() throws Exception {
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT publication_epoch FROM generated_value_artifacts "
                         + "WHERE tenant_id='tenant' AND artifact_id='artifact'")) {
                assertThat(result.next()).isTrue();
                return result.getLong(1);
            }
        }
    }
}
