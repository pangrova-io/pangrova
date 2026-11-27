package org.pangrova.metadata.generation.jdbc;

import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.generation.submission.GenerationDispatchSpecFactory;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.generation.postgresql.publication.JdbcTabularExportPublicationStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.enums.GenerationJobState;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.enums.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.enums.GenerationExecutionKind;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.generation.api.enums.GenerationOutputScope;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.output.GenerationOutput;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationPageCompletion;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

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

    private void configure(DataSource dataSource) throws Exception {
        Flyway.configure().dataSource(dataSource)
                .locations("classpath:db/migration").load().migrate();
        buildStore = GenerationAuthorityFixtures.artifacts().builds(dataSource);
        executions = new org.pangrova.metadata.testinfra.GenerationExecutionFixtures(dataSource);
        publicationStore = GenerationAuthorityFixtures.artifacts().tabularPublications(dataSource);
        org.pangrova.metadata.testinfra.GenerationAuthorityFixtures.provision(
                dataSource, TENANT, "feature-owner", dispatch(1).authorizationScope());
    }

    @Test
    void atomicallyPublishesEmptySnapshotAsDistinctSuccessfulManifest() throws Exception {
        configure(SharedPostgresContainer.isolatedDataSource("empty_tabular_export_publication"));
        GenerationJob job = exportJob(1);
        buildStore.createJob(job, dispatch(1), List.of(new org.pangrova.generation.api.page.GenerationPage(
                job.jobId(), "terminal-0", 0, 0, 0,
                org.pangrova.generation.api.enums.GenerationPageKind.RELATIONAL_PARTITION,
                List.of(), List.of(), 3)));
        var lease = buildStore.claimNextPage(TENANT, job.jobId(), executions.takeOwnership(TENANT, job.jobId(), "worker-dispatch", 1), 1,
                Duration.ofSeconds(30), START).orElseThrow();
        byte[] report = "validated-terminal-report".getBytes(StandardCharsets.UTF_8);
        var output = new org.pangrova.generation.api.output.GenerationOutput(
                "terminal-object", "s3://warehouse/staging/empty-export-job/terminal-0", "TABULAR_DATA", "terminal-0", 0,
                org.pangrova.generation.api.enums.GenerationOutputScope.WORK_UNIT, 7, 0, "a".repeat(64), "");
        assertThat(buildStore.completePage(new org.pangrova.generation.api.page.GenerationPageCompletion(
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

    @Test
    void validatesEightCompletedPartitionsWithoutReloadingDispatchOrReplayInputs() throws Exception {
        try (var postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                .withCommand("postgres", "-c", "shared_preload_libraries=pg_stat_statements")) {
            postgres.start();
            var dataSource = new PGSimpleDataSource();
            dataSource.setURL(postgres.getJdbcUrl());
            dataSource.setUser(postgres.getUsername());
            dataSource.setPassword(postgres.getPassword());
            configure(dataSource);
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.execute("CREATE EXTENSION pg_stat_statements WITH SCHEMA public");
            }

            int partitions = 8;
            GenerationJob job = exportJob(partitions);
            var pages = java.util.stream.IntStream.range(0, partitions)
                    .mapToObj(partition -> new GenerationPage(job.jobId(), "terminal-" + partition, 0,
                            partition, partition, GenerationPageKind.RELATIONAL_PARTITION, List.of(), List.of(), 3))
                    .toList();
            buildStore.createJob(job, dispatch(partitions), pages);
            var worker = executions.takeOwnership(TENANT, job.jobId(), "worker-dispatch", 1);
            byte[] report = "validated-terminal-report".getBytes(StandardCharsets.UTF_8);
            String reportChecksum = org.pangrova.common.util.DigestUtils.sha256Hex(report);
            for (int partition = 0; partition < partitions; partition++) {
                var lease = buildStore.claimNextPage(TENANT, job.jobId(), worker, 1, Duration.ofSeconds(30), START)
                        .orElseThrow();
                assertThat(lease.pageId()).isEqualTo(pages.get(partition).pageId());
                var output = new GenerationOutput(
                        "object-" + partition, job.stagingUri() + "/" + lease.pageId(), "TABULAR_DATA",
                        lease.pageId(), 0, GenerationOutputScope.WORK_UNIT, 7, 1, "a".repeat(64), "");
                assertThat(buildStore.completePage(new GenerationPageCompletion(
                        TENANT, job.jobId(), lease.pageId(), lease.fencingToken(), lease.ownerEpoch(), report,
                        reportChecksum, List.of(output), START,
                        executions.reportedOwner(TENANT, job.jobId(), lease.ownerEpoch())))).isTrue();
            }
            var owner = executions.takeOwnership(TENANT, job.jobId(), "publisher-a", 9);
            var claim = buildStore.claimReadyForPublication(
                    TENANT, job.jobId(), owner, 9, Duration.ofSeconds(30), START.plusSeconds(1)).orElseThrow();

            String lastPage = pages.get(partitions - 1).pageId();
            setPageState(dataSource, job, lastPage, "FAILED");
            assertThatThrownBy(() -> publicationStore.publishClaimed(claim, "publisher-a", 9, START.plusSeconds(2)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("completion barrier");
            setPageState(dataSource, job, lastPage, "SUCCEEDED");
            setOutputPage(dataSource, job, lastPage, false);
            assertThatThrownBy(() -> publicationStore.publishClaimed(claim, "publisher-a", 9, START.plusSeconds(2)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("terminal partition cohort");
            assertThat(buildStore.findJob(TENANT, job.jobId()).orElseThrow().state())
                    .isEqualTo(GenerationJobState.READY_TO_PUBLISH);
            assertThat(buildStore.findPublishedGeneration(
                    TENANT, job.artifactId(), job.stagingGeneration(), job.sourceSnapshot())).isEmpty();
            setOutputPage(dataSource, job, lastPage, true);

            String spec = "SELECT%FROM artifact_build_dispatch_specs%";
            String completionJoin = "SELECT%FROM artifact_build_pages p LEFT JOIN artifact_build_outputs o%";
            String pageReads = "SELECT%FROM artifact_build_pages%";
            String splits = "SELECT%FROM artifact_build_page_sources%";
            String dependencies = "SELECT%FROM artifact_build_page_dependencies%";
            long specBefore = statementCalls(dataSource, spec);
            long joinBefore = statementCalls(dataSource, completionJoin);
            long pagesBefore = statementCalls(dataSource, pageReads);
            long splitsBefore = statementCalls(dataSource, splits);
            long dependenciesBefore = statementCalls(dataSource, dependencies);

            var manifest = publicationStore.publishClaimed(claim, "publisher-a", 9, START.plusSeconds(2));

            assertThat(manifest.objectCount()).isEqualTo(partitions);
            assertThat(manifest.rowCount()).isEqualTo(partitions);
            assertThat(statementCalls(dataSource, spec) - specBefore).isEqualTo(1);
            assertThat(statementCalls(dataSource, completionJoin) - joinBefore).isEqualTo(1);
            assertThat(statementCalls(dataSource, pageReads) - pagesBefore).isEqualTo(1);
            assertThat(statementCalls(dataSource, splits) - splitsBefore).isZero();
            assertThat(statementCalls(dataSource, dependencies) - dependenciesBefore).isZero();
        }
    }

    private static void setPageState(DataSource source, GenerationJob job, String pageId, String state)
            throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                UPDATE artifact_build_pages SET state=? WHERE tenant_id=? AND job_id=? AND page_id=?
                """)) {
            statement.setString(1, state);
            statement.setString(2, job.tenantId());
            statement.setString(3, job.jobId());
            statement.setString(4, pageId);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private static void setOutputPage(DataSource source, GenerationJob job, String pageId, boolean restore)
            throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                UPDATE artifact_build_outputs SET page_id=?,object_ordinal=?
                WHERE tenant_id=? AND job_id=? AND page_id=? AND source_object_id=?
                """)) {
            statement.setString(1, restore ? pageId : "terminal-0");
            statement.setInt(2, restore ? 0 : 1);
            statement.setString(3, job.tenantId());
            statement.setString(4, job.jobId());
            statement.setString(5, restore ? "terminal-0" : pageId);
            statement.setString(6, pageId);
            assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private static long statementCalls(DataSource source, String pattern) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.prepareStatement("""
                SELECT COALESCE(sum(calls),0) FROM public.pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query LIKE ?
                """)) {
            statement.setString(1, pattern);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                return rows.getLong(1);
            }
        }
    }

    private static GenerationJob exportJob(int partitions) {
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
                partitions, 0, GenerationJobState.QUEUED,
                0, START, START, START);
    }

    private static GenerationDispatchSpec dispatch(int partitions) {
        return GenerationDispatchSpecFactory.create(
                "empty-export-job", new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                        GenerationGraphPurpose.RELATIONAL_EXPORT,
                        List.of(GenerationStageGraph.StagePlan.of(0, partitions,
                                org.pangrova.generation.api.enums.GenerationPageKind.RELATIONAL_PARTITION,
                                "substrait-plan".getBytes(StandardCharsets.UTF_8), GenerationStageTemplate.of("operator-binding".getBytes(StandardCharsets.UTF_8), List.of(), List.of()),
                                List.of(), List.of(), true)), List.of(), List.of()),
                TENANT, "feature-owner",
                org.pangrova.security.api.authorization.DeferredAuthorizationScope.raw(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "raw", "customers"),
                        java.util.Set.of("customer_id"),
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName("lake", "features", "customer_features")),
                new GenerationDispatchSpec.ResourceGrant(
                        4096, 1, 4096, 60_000));
    }
}
