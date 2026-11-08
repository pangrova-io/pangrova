package org.pangrova.testing.integration.control.ai;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.control.feature.CatalogExportRequest;
import org.pangrova.ai.control.api.CatalogExportStorageException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.ai.control.feature.CatalogExportBuildStore;
import org.pangrova.persistence.ai.postgresql.assembly.AiInfrastructureStores;
import org.pangrova.generation.api.execution.GenerationPageKind;
import org.pangrova.generation.api.dependency.GenerationDependencySet;
import org.pangrova.generation.api.output.GenerationEmptyPartitionPolicy;
import org.pangrova.generation.api.execution.GenerationExecutionKind;
import org.pangrova.generation.api.output.GenerationOutputTypeIds;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.job.GenerationJob;
import org.pangrova.generation.api.job.GenerationJobState;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.assembly.generation.GenerationStores;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.security.api.error.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogExportTransactionIntegrationTest extends org.pangrova.testing.testkit.ContainerTestBase {
    private static final String TENANT = "export-tenant";
    private static final String PRINCIPAL = "exporter";
    private static final Instant NOW = Instant.parse("2026-10-01T00:00:00Z");
    private static final DeferredAuthorizationScope SCOPE = DeferredAuthorizationScope.raw(
            new QualifiedTableName("lake", "raw", "customers"), Set.of("id"),
            new QualifiedTableName("lake", "features", "customers"));
    private static final List<String> TABLES = List.of("artifact_build_jobs", "artifact_build_dispatch_specs",
            "artifact_build_pages", "artifact_execution_routes", "catalog_export_requests");
    private DataSource dataSource;
    private CatalogExportBuildStore store;
    private PostgresGenerationJobStore builds;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        GenerationAuthorityFixtures.provision(dataSource, TENANT, PRINCIPAL, SCOPE);
        builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        store = AiInfrastructureStores.jdbc(dataSource, builds).catalogExports();
    }

    @Test
    void commitsAndReplaysBothCatalogFamiliesWithTheSameDurableGraph() throws Exception {
        for (var kind : CatalogExportRequest.ObjectKind.values()) {
            var job = job(kind.name());
            var request = request(job, kind, PRINCIPAL);
            var spec = spec(job, TENANT, PRINCIPAL);
            store.createCatalogExportJob(request, job, spec, pages(job));
            store.createCatalogExportJob(request, job, spec, pages(job));
            assertThat(builds.findJob(TENANT, job.jobId())).contains(job);
            assertThat(builds.findDispatchSpec(TENANT, job.jobId())).contains(spec);
        }
        assertRows(2);
    }

    @Test
    void conflictingCatalogLinkRollsBackEveryNewBuildRow() throws Exception {
        var first = job("first");
        store.createCatalogExportJob(request(first, CatalogExportRequest.ObjectKind.FEATURE_VIEW, PRINCIPAL),
                first, spec(first, TENANT, PRINCIPAL), pages(first));
        var conflicting = job("second");
        assertThatThrownBy(() -> store.createCatalogExportJob(
                request(conflicting, CatalogExportRequest.ObjectKind.FEATURE_VIEW, PRINCIPAL),
                conflicting, spec(conflicting, TENANT, PRINCIPAL), pages(conflicting)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("another artifact job");
        assertThat(builds.findJob(TENANT, "first")).contains(first);
        assertThat(builds.findJob(TENANT, "second")).isEmpty();
        assertThat(builds.findDispatchSpec(TENANT, "second")).isEmpty();
        assertRows(1);
    }

    @Test
    void rejectsPrincipalTenantAndJobMismatchBeforeInsertion() throws Exception {
        var job = job("job");
        var request = request(job, CatalogExportRequest.ObjectKind.TRAINING_DATASET, PRINCIPAL);
        for (var invalid : List.of(spec(job, TENANT, "another-principal"),
                spec(job, "another-tenant", PRINCIPAL), spec(job("another-job"), TENANT, PRINCIPAL))) {
            assertThatThrownBy(() -> store.createCatalogExportJob(request, job, invalid, pages(job)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("identity");
        }
        assertRows(0);
    }

    @Test
    void rejectsNonExportArtifactOrExecutionKindBeforeInsertion() throws Exception {
        for (var job : List.of(job("index", GenerationOutputTypeIds.VECTOR_INDEX, GenerationExecutionKind.EXPORT),
                job("build", GenerationOutputTypeIds.TABULAR_EXPORT, GenerationExecutionKind.BUILD))) {
            assertThatThrownBy(() -> store.createCatalogExportJob(
                    request(job, CatalogExportRequest.ObjectKind.FEATURE_VIEW, PRINCIPAL),
                    job, spec(job, TENANT, PRINCIPAL), pages(job)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("identity");
        }
        assertRows(0);
    }

    @Test
    void revokedCurrentAuthorityCannotCreateTheCatalogLinkOrBuild() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM security_grants WHERE privilege='SELECT'");
        }
        var job = job("revoked");
        assertThatThrownBy(() -> store.createCatalogExportJob(
                request(job, CatalogExportRequest.ObjectKind.FEATURE_VIEW, PRINCIPAL),
                job, spec(job, TENANT, PRINCIPAL), pages(job)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertRows(0);
    }

    @Test
    void injectedAuthoritySharesExportTransactionAndItsDenialRollsBackAllChanges() throws Exception {
        var calls = new AtomicInteger();
        var authority = new GenerationTransactionAuthority() {
            @Override
            public GenerationDispatchSpec requireCurrent(Connection connection, String tenant, String job) {
                throw new AssertionError("Creation validates the supplied specification before persistence");
            }

            @Override
            public void requireCurrent(Connection connection, GenerationDispatchSpec spec) throws SQLException {
                assertThat(connection.getAutoCommit()).isFalse();
                calls.incrementAndGet();
                try (var statement = connection.prepareStatement(
                        "UPDATE tenants SET status='DELETED' WHERE tenant_id=?")) {
                    statement.setString(1, TENANT);
                    assertThat(statement.executeUpdate()).isEqualTo(1);
                }
                ModelAuthorityFixtures.artifacts().authority().requireCurrent(connection, spec);
            }
        };
        var guardedBuilds = new PostgresGenerationJobStore(dataSource, authority, GenerationStores.executionFence(),
                GenerationStores.currentRoles(), GenerationStores.publicationCandidates());
        var guarded = AiInfrastructureStores.jdbc(dataSource, guardedBuilds).catalogExports();
        var job = job("authority-transaction");
        var request = request(job, CatalogExportRequest.ObjectKind.FEATURE_VIEW, PRINCIPAL);
        var spec = spec(job, TENANT, PRINCIPAL);
        assertThatThrownBy(() -> guarded.createCatalogExportJob(request, job, spec, pages(job)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(calls).hasValue(1);
        assertRows(0);

        // The export owner rolled back the authority transaction as well as all build/link writes.
        store.createCatalogExportJob(request, job, spec, pages(job));
        assertRows(1);
    }

    @Test
    void sqlFailurePreservesClientCodeAndRollsBackTheBuildBeforeTheCatalogLink() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE catalog_export_requests ADD CONSTRAINT reject_export CHECK (false)");
        }
        var job = job("sql-failure");
        var request = request(job, CatalogExportRequest.ObjectKind.FEATURE_VIEW, PRINCIPAL);
        var spec = spec(job, TENANT, PRINCIPAL);
        assertThatThrownBy(() -> store.createCatalogExportJob(request, job, spec, pages(job)))
                .isInstanceOfSatisfying(CatalogExportStorageException.class, failure -> {
                    assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR);
                    assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                    assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("23514");
                });
        assertRows(0);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE catalog_export_requests DROP CONSTRAINT reject_export");
        }
        store.createCatalogExportJob(request, job, spec, pages(job));
        assertRows(1);
    }

    private void assertRows(int expected) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            for (String table : TABLES) {
                try (var result = statement.executeQuery("SELECT count(*) FROM " + table)) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).as(table).isEqualTo(expected);
                }
            }
        }
    }

    private static CatalogExportRequest request(GenerationJob job,
            CatalogExportRequest.ObjectKind kind, String principal) {
        return new CatalogExportRequest(TENANT, kind, "lake", "features", "customers", 1,
                job.stagingGeneration(), job.artifactId(), job.jobId(), job.dependencySet().sha256(), principal, NOW);
    }

    private static GenerationJob job(String id) {
        return job(id, GenerationOutputTypeIds.TABULAR_EXPORT, GenerationExecutionKind.EXPORT);
    }

    private static GenerationJob job(String id, String artifactType, GenerationExecutionKind executionKind) {
        return new GenerationJob(TENANT, id, "export-" + id, artifactType, "generation-1",
                "lake.raw.customers", "snapshot-1", executionKind,
                GenerationDependencySet.sourceSnapshot("lake.raw.customers", "snapshot-1"),
                GenerationEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://exports/" + id,
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "writer", "", id,
                1, 0, GenerationJobState.QUEUED, 0, NOW, NOW, NOW);
    }

    private static List<GenerationPage> pages(GenerationJob job) {
        return List.of(new GenerationPage(job.jobId(), "page", 0, 0, 0,
                GenerationPageKind.RELATIONAL_PARTITION, List.of(), List.of(), 3));
    }

    private static GenerationDispatchSpec spec(GenerationJob job, String tenant, String principal) {
        var stage = GenerationStageGraph.StagePlan.of(0, 1, GenerationPageKind.RELATIONAL_PARTITION,
                new byte[]{1}, GenerationStageTemplate.of(new byte[]{2}, List.of(), List.of()),
                List.of(), List.of(), true);
        var graph = new GenerationStageGraph(2, GenerationStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(stage), List.of(), List.of());
        return new GenerationDispatchSpec(job.jobId(), graph, tenant, principal, SCOPE, 4096, 1, 4096, 60_000);
    }
}
