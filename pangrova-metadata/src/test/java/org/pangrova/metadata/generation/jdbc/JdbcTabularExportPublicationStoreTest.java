package org.pangrova.metadata.generation.jdbc;

import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;

import org.pangrova.generation.assembly.GenerationDispatchSpecFactory;

import org.pangrova.generation.api.execution.template.GenerationStageTemplate;
import org.pangrova.generation.api.execution.GenerationStageGraph;
import org.pangrova.generation.store.postgresql.job.PostgresGenerationJobStore;

import static org.assertj.core.api.Assertions.assertThat;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.GenerationDependencySet;
import org.pangrova.generation.api.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.GenerationExecutionKind;
import org.pangrova.generation.api.GenerationOutputTypeIds;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

class JdbcTabularExportPublicationStoreTest {

    private org.pangrova.metadata.testinfra.GenerationExecutionFixtures executions;

    private static final Instant START = Instant.parse("2026-09-19T00:00:00Z");
    private static final String TENANT = "tenant-empty-export";

    private PostgresGenerationJobStore buildStore;
    private JdbcTabularExportPublicationStore publicationStore;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.isolatedDataSource(
                "empty_tabular_export_publication");
        Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration").load().migrate();
        buildStore = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.metadata.testinfra.GenerationExecutionFixtures(dataSource);
        publicationStore = GenerationAuthorityFixtures.artifacts().tabularPublications(dataSource);
        org.pangrova.metadata.testinfra.GenerationAuthorityFixtures.provision(
                dataSource, TENANT, "feature-owner", dispatch().authorizationScope());
    }

    @Test
    void atomicallyPublishesEmptySnapshotAsDistinctSuccessfulManifest() {
        GenerationJob job = emptyJob();
        buildStore.createJob(job, dispatch(), List.of(new org.pangrova.generation.api.job.GenerationPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.generation.api.GenerationPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = buildStore.claimNextPage(TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "worker-dispatch", 1), 1,
                Duration.ofSeconds(30), START).orElseThrow();
        byte[] report = "validated-terminal-report".getBytes(StandardCharsets.UTF_8);
        var output = new org.pangrova.generation.api.job.GenerationOutput(
                "terminal-object", "s3://warehouse/staging/empty-export-job/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                org.pangrova.generation.api.GenerationOutputScope.WORK_UNIT, 7, 0, "a".repeat(64), "");
        assertThat(buildStore.completePage(new org.pangrova.generation.api.job.GenerationPageCompletion(
                TENANT, job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                org.pangrova.common.util.DigestUtils.sha256Hex(report), List.of(output), START, executions.reportedOwner(TENANT, job.jobId(), lease.ownerEpoch())))).isTrue();
        GenerationJob claimed = buildStore.claimReadyForPublication(
                TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "publisher-a", 9), 9,
                Duration.ofSeconds(30), START.plusSeconds(1)).orElseThrow();

        var manifest = publicationStore.publishClaimed(
                claimed, "publisher-a", 9, START.plusSeconds(2));

        assertThat(manifest.emptyPartitionPolicy())
                .isEqualTo(GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS);
        assertThat(manifest.objectCount()).isEqualTo(1);
        assertThat(manifest.rowCount()).isZero();
        assertThat(manifest.totalBytes()).isEqualTo(7);
        assertThat(manifest.manifestSha256()).hasSize(64).isNotEqualTo("0".repeat(64));
        assertThat(buildStore.findJob(TENANT, job.jobId())).hasValueSatisfying(stored ->
                assertThat(stored.state()).isEqualTo(GenerationJobState.PUBLISHED));
        assertThat(buildStore.findPublishedGeneration(
                TENANT, job.artifactId(), job.stagingGeneration(), job.sourceSnapshot()))
                .hasValueSatisfying(stored -> {
                    assertThat(stored.manifestSha256())
                            .isEqualTo(manifest.manifestSha256());
                    assertThat(stored.objectCount()).isEqualTo(1);
                    assertThat(stored.rowCount()).isZero();
                });
    }

    private static GenerationJob emptyJob() {
        return new GenerationJob(
                TENANT, "empty-export-job", "feature-view/customer_features/v1",
                GenerationOutputTypeIds.TABULAR_EXPORT, "generation-empty",
                "lake.raw.customers", "snapshot-empty", GenerationExecutionKind.EXPORT,
                GenerationDependencySet.sourceSnapshot(
                        "lake.raw.customers", "snapshot-empty"),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS,
                "s3://warehouse/staging/empty-export-job",
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64),
                "pangrova-native-0.1.0", "", "empty-export-job",
                1, 0, GenerationJobState.QUEUED,
                0, START, START, START);
    }

    private static GenerationDispatchSpec dispatch() {
        return GenerationDispatchSpecFactory.create(
                "empty-export-job", new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                        List.of(GenerationStageGraph.StagePlan.of(0, 1,
                                org.pangrova.generation.api.GenerationPageKind.RELATIONAL_PARTITION,
                                "substrait-plan".getBytes(StandardCharsets.UTF_8), GenerationStageTemplate.of("operator-binding".getBytes(StandardCharsets.UTF_8), List.of(), List.of()),
                                List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "feature-owner",
                org.pangrova.spi.security.authz.DeferredAuthorizationScope.raw(
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "raw", "customers"),
                        java.util.Set.of("customer_id"),
                        new org.pangrova.spi.connector.metadata.table.QualifiedTableName("lake", "features", "customer_features")),
                new GenerationDispatchSpec.ResourceGrant(
                        4096, 1, 4096, 60_000));
    }
}
