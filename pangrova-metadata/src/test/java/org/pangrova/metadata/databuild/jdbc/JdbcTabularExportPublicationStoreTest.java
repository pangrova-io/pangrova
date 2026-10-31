package org.pangrova.metadata.databuild.jdbc;

import org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures;


import org.pangrova.databuild.assembly.DataBuildDispatchSpecFactory;

import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;

import static org.assertj.core.api.Assertions.assertThat;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

class JdbcTabularExportPublicationStoreTest {

    private org.pangrova.metadata.testinfra.DataBuildExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-19T00:00:00Z");
    private static final String TENANT = "tenant-empty-export";

    private JdbcDataBuildStore buildStore;
    private JdbcTabularExportPublicationStore publicationStore;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource(
                "empty_tabular_export_publication");
        Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration").load().migrate();
        buildStore = DataBuildAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.metadata.testinfra.DataBuildExecutionFixtures(dataSource);
        publicationStore = DataBuildAuthorityFixtures.artifacts().tabularPublications(dataSource);
        org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures.provision(
                dataSource, TENANT, "feature-owner", dispatch().authorizationScope());
    }

    @Test
    void atomicallyPublishesEmptySnapshotAsDistinctSuccessfulManifest() {
        DataBuildJob job = emptyJob();
        buildStore.createJob(job, dispatch(), List.of(new org.pangrova.databuild.api.build.DataBuildPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.databuild.api.DataBuildPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = buildStore.claimNextPage(TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "worker-dispatch", 1), 1,
                Duration.ofSeconds(30), START).orElseThrow();
        byte[] report = "validated-terminal-report".getBytes(StandardCharsets.UTF_8);
        var output = new org.pangrova.databuild.api.build.DataBuildOutput(
                "terminal-object", "s3://warehouse/staging/empty-export-job/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                org.pangrova.databuild.api.DataBuildOutputScope.WORK_UNIT, 7, 0, "a".repeat(64), "");
        assertThat(buildStore.completePage(new org.pangrova.databuild.api.build.DataBuildPageCompletion(
                TENANT, job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report), List.of(output), START, executions.reportedOwner(TENANT, job.jobId(), lease.ownerEpoch())))).isTrue();
        DataBuildJob claimed = buildStore.claimReadyForPublication(
                TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "publisher-a", 9), 9,
                Duration.ofSeconds(30), START.plusSeconds(1)).orElseThrow();

        var manifest = publicationStore.publishClaimed(
                claimed, "publisher-a", 9, START.plusSeconds(2));

        assertThat(manifest.emptyPartitionPolicy())
                .isEqualTo(DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS);
        assertThat(manifest.objectCount()).isEqualTo(1);
        assertThat(manifest.rowCount()).isZero();
        assertThat(manifest.totalBytes()).isEqualTo(7);
        assertThat(manifest.manifestSha256()).hasSize(64).isNotEqualTo("0".repeat(64));
        assertThat(buildStore.findJob(TENANT, job.jobId())).hasValueSatisfying(stored ->
                assertThat(stored.state()).isEqualTo(DataBuildJobState.PUBLISHED));
        assertThat(buildStore.findPublishedGeneration(
                TENANT, job.artifactId(), job.stagingGeneration(), job.sourceSnapshot()))
                .hasValueSatisfying(stored -> {
                    assertThat(stored.manifestSha256())
                            .isEqualTo(manifest.manifestSha256());
                    assertThat(stored.objectCount()).isEqualTo(1);
                    assertThat(stored.rowCount()).isZero();
                });
    }

    private static DataBuildJob emptyJob() {
        return new DataBuildJob(
                TENANT, "empty-export-job", "feature-view/customer_features/v1",
                DataBuildOutputTypeIds.TABULAR_EXPORT, "generation-empty",
                "lake.raw.customers", "snapshot-empty", DataBuildExecutionKind.EXPORT,
                DataBuildDependencySet.sourceSnapshot(
                        "lake.raw.customers", "snapshot-empty"),
                DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/staging/empty-export-job",
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "pangrova-native-0.1.0", "", "empty-export-job",
                1, 0, DataBuildJobState.QUEUED,
                0, START, START, START);
    }

    private static DataBuildDispatchSpec dispatch() {
        return DataBuildDispatchSpecFactory.create(
                "empty-export-job", new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION,
                        DataBuildStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(DataBuildStageGraph.StagePlan.of(0, 1,
                                org.pangrova.databuild.api.DataBuildPageKind.RELATIONAL_PARTITION,
                                "substrait-plan".getBytes(StandardCharsets.UTF_8), DataBuildStageTemplate.of("operator-binding".getBytes(StandardCharsets.UTF_8), List.of(), List.of()),
                                List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "feature-owner",
                org.pangrova.security.api.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "raw", "customers"),
                        java.util.Set.of("customer_id"),
                        new org.pangrova.connector.spi.metadata.table.QualifiedTableName("lake", "features", "customer_features")),
                new DataBuildDispatchSpec.ResourceGrant(
                        4096, 1, 4096, 60_000));
    }
}
