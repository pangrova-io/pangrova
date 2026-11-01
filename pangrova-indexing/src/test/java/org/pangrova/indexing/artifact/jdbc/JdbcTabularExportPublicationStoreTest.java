package org.pangrova.indexing.artifact.jdbc;

import org.pangrova.indexing.store.jdbc.JdbcTabularExportPublicationStore;

import org.pangrova.indexing.control.ArtifactStageTemplate;
import org.pangrova.indexing.control.ArtifactStageGraph;

import org.pangrova.indexing.store.jdbc.JdbcArtifactBuildStore;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.indexing.control.ArtifactBuildDispatchSpec;
import org.pangrova.indexing.control.ArtifactBuildJob;
import org.pangrova.indexing.control.ArtifactBuildJobState;
import org.pangrova.indexing.api.ArtifactDependencySet;
import org.pangrova.indexing.api.ArtifactEmptyPartitionPolicy;
import org.pangrova.indexing.api.ArtifactExecutionKind;
import org.pangrova.indexing.api.ArtifactTypeIds;
import org.pangrova.indexing.testinfra.ArtifactAuthorityTestDoubles;
import org.pangrova.indexing.testinfra.SharedPostgresContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

class JdbcTabularExportPublicationStoreTest {

    private org.pangrova.indexing.testinfra.ArtifactExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-19T00:00:00Z");
    private static final String TENANT = "tenant-empty-export";

    private JdbcArtifactBuildStore buildStore;
    private JdbcTabularExportPublicationStore publicationStore;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource(
                "empty_tabular_export_publication");
        ArtifactAuthorityTestDoubles.provisionTenant(dataSource, TENANT, "feature-owner");
        buildStore = new JdbcArtifactBuildStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
        executions = new org.pangrova.indexing.testinfra.ArtifactExecutionFixtures(dataSource);
        publicationStore = new JdbcTabularExportPublicationStore(dataSource, ArtifactAuthorityTestDoubles.permissive());
    }

    @Test
    void atomicallyPublishesEmptySnapshotAsDistinctSuccessfulManifest() {
        ArtifactBuildJob job = emptyJob();
        buildStore.createJob(job, dispatch(), List.of(new org.pangrova.indexing.control.ArtifactBuildPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.indexing.api.ArtifactBuildPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = buildStore.claimNextPage(TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "worker-dispatch", 1), 1,
                Duration.ofSeconds(30), START).orElseThrow();
        byte[] report = "validated-terminal-report".getBytes(StandardCharsets.UTF_8);
        var output = new org.pangrova.indexing.control.ArtifactBuildOutput(
                "terminal-object", "s3://warehouse/staging/empty-export-job/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                org.pangrova.indexing.api.ArtifactBuildOutputScope.WORK_UNIT, 7, 0, "a".repeat(64), "");
        assertThat(buildStore.completePage(new org.pangrova.indexing.control.ArtifactPageCompletion(
                TENANT, job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report), List.of(output), START, executions.reportedOwner(TENANT, job.jobId(), lease.ownerEpoch())))).isTrue();
        ArtifactBuildJob claimed = buildStore.claimReadyForPublication(
                TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "publisher-a", 9), 9,
                Duration.ofSeconds(30), START.plusSeconds(1)).orElseThrow();

        var manifest = publicationStore.publishClaimed(
                claimed, "publisher-a", 9, START.plusSeconds(2));

        assertThat(manifest.emptyPartitionPolicy())
                .isEqualTo(ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS);
        assertThat(manifest.objectCount()).isEqualTo(1);
        assertThat(manifest.rowCount()).isZero();
        assertThat(manifest.totalBytes()).isEqualTo(7);
        assertThat(manifest.manifestSha256()).hasSize(64).isNotEqualTo("0".repeat(64));
        assertThat(buildStore.findJob(TENANT, job.jobId())).hasValueSatisfying(stored ->
                assertThat(stored.state()).isEqualTo(ArtifactBuildJobState.PUBLISHED));
        assertThat(buildStore.findPublishedGeneration(
                TENANT, job.artifactId(), job.stagingGeneration(), job.sourceSnapshot()))
                .hasValueSatisfying(stored -> {
                    assertThat(stored.manifestSha256())
                            .isEqualTo(manifest.manifestSha256());
                    assertThat(stored.objectCount()).isEqualTo(1);
                    assertThat(stored.rowCount()).isZero();
                });
    }

    private static ArtifactBuildJob emptyJob() {
        return new ArtifactBuildJob(
                TENANT, "empty-export-job", "feature-view/customer_features/v1",
                ArtifactTypeIds.TABULAR_EXPORT, "generation-empty",
                "lake.raw.customers", "snapshot-empty", ArtifactExecutionKind.EXPORT,
                ArtifactDependencySet.sourceSnapshot(
                        "lake.raw.customers", "snapshot-empty"),
                ArtifactEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/staging/empty-export-job",
                "pangrova.tabular-export.columnar-stream", 1, 0, "a".repeat(64),
                "pangrova-native-0.1.0", "", "empty-export-job",
                1, 0, ArtifactBuildJobState.QUEUED,
                0, START, START, START);
    }

    private static ArtifactBuildDispatchSpec dispatch() {
        return ArtifactBuildDispatchSpec.of(
                "empty-export-job", new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION,
                        ArtifactStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(ArtifactStageGraph.StagePlan.of(0, 1,
                                org.pangrova.indexing.api.ArtifactBuildPageKind.RELATIONAL_PARTITION,
                                "substrait-plan".getBytes(StandardCharsets.UTF_8), ArtifactStageTemplate.of("operator-binding".getBytes(StandardCharsets.UTF_8), List.of(), List.of()),
                                List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "feature-owner",
                org.pangrova.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "raw", "customers"),
                        java.util.Set.of("customer_id"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "features", "customer_features")),
                new ArtifactBuildDispatchSpec.ResourceGrant(
                        4096, 1, 4096, 60_000));
    }
}
