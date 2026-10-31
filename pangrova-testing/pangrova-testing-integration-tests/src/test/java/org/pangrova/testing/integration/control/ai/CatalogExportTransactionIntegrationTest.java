package org.pangrova.testing.integration.control.ai;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.control.feature.CatalogExportRequest;
import org.pangrova.ai.control.api.CatalogExportStorageException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.ai.control.feature.CatalogExportBuildStore;
import org.pangrova.ai.control.assembly.AiInfrastructureStores;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.DataBuildDependencySet;
import org.pangrova.databuild.api.DataBuildEmptyPartitionPolicy;
import org.pangrova.databuild.api.DataBuildExecutionKind;
import org.pangrova.databuild.api.DataBuildOutputTypeIds;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildJob;
import org.pangrova.databuild.api.build.DataBuildJobState;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;
import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;
import org.pangrova.databuild.spi.jdbc.DataBuildTransactionAuthority;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore;
import org.pangrova.metadata.testinfra.DataBuildAuthorityFixtures;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.security.api.AuthorizationDeniedException;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

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
    private JdbcDataBuildStore builds;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = MigratedPostgres.create();
        DataBuildAuthorityFixtures.provision(dataSource, TENANT, PRINCIPAL, SCOPE);
        store = AiInfrastructureStores.jdbc(dataSource, ModelAuthorityFixtures.artifacts().authority()).catalogExports();
        builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
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
        for (var job : List.of(job("index", DataBuildOutputTypeIds.VECTOR_INDEX, DataBuildExecutionKind.EXPORT),
                job("build", DataBuildOutputTypeIds.TABULAR_EXPORT, DataBuildExecutionKind.BUILD))) {
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
        var authority = new DataBuildTransactionAuthority() {
            @Override
            public DataBuildDispatchSpec requireCurrent(Connection connection, String tenant, String job) {
                throw new AssertionError("Creation validates the supplied specification before persistence");
            }

            @Override
            public void requireCurrent(Connection connection, DataBuildDispatchSpec spec) throws SQLException {
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
        var guarded = AiInfrastructureStores.jdbc(dataSource, authority).catalogExports();
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

    private static CatalogExportRequest request(DataBuildJob job,
            CatalogExportRequest.ObjectKind kind, String principal) {
        return new CatalogExportRequest(TENANT, kind, "lake", "features", "customers", 1,
                job.stagingGeneration(), job.artifactId(), job.jobId(), job.dependencySet().sha256(), principal, NOW);
    }

    private static DataBuildJob job(String id) {
        return job(id, DataBuildOutputTypeIds.TABULAR_EXPORT, DataBuildExecutionKind.EXPORT);
    }

    private static DataBuildJob job(String id, String artifactType, DataBuildExecutionKind executionKind) {
        return new DataBuildJob(TENANT, id, "export-" + id, artifactType, "generation-1",
                "lake.raw.customers", "snapshot-1", executionKind,
                DataBuildDependencySet.sourceSnapshot("lake.raw.customers", "snapshot-1"),
                DataBuildEmptyPartitionPolicy.ALLOW_ZERO_ROWS, "s3://exports/" + id,
                "pangrova.tabular-export.arrow-stream", 1, 0, "a".repeat(64), "writer", "", id,
                1, 0, DataBuildJobState.QUEUED, 0, NOW, NOW, NOW);
    }

    private static List<DataBuildPage> pages(DataBuildJob job) {
        return List.of(new DataBuildPage(job.jobId(), "page", 0, 0, 0,
                DataBuildPageKind.RELATIONAL_PARTITION, List.of(), List.of(), 3));
    }

    private static DataBuildDispatchSpec spec(DataBuildJob job, String tenant, String principal) {
        var stage = DataBuildStageGraph.StagePlan.of(0, 1, DataBuildPageKind.RELATIONAL_PARTITION,
                new byte[]{1}, DataBuildStageTemplate.of(new byte[]{2}, List.of(), List.of()),
                List.of(), List.of(), true);
        var graph = new DataBuildStageGraph(2, DataBuildStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(stage), List.of(), List.of());
        return new DataBuildDispatchSpec(job.jobId(), graph, tenant, principal, SCOPE, 4096, 1, 4096, 60_000);
    }
}
