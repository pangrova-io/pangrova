package org.pangrova.coordinator.index.ddl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.CoordinatorArtifactAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.artifact.build.ArtifactBuildDispatchSpec;
import org.pangrova.metadata.artifact.contract.ArtifactTypeIds;
import org.pangrova.metadata.artifact.jdbc.build.JdbcArtifactBuildStore;
import org.pangrova.planner.artifact.VectorArtifactBuildPlanBuilder;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual persisted index producer and current PostgreSQL authority for background replacements. */
class DeferredIndexAuthorityResolverTest extends ContainerTestBase {
    private static final Instant NOW = Instant.parse("2026-09-30T00:00:00Z");
    private static final String TENANT = "index-authority-tenant";
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "raw", "items");
    private DataSource dataSource;
    private JdbcArtifactBuildStore builds;
    private IndexDescriptor previous;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("deferred_index_authority");
        CoordinatorArtifactAuthorityFixtures.provision(dataSource, TENANT, "alice",
                DeferredAuthorizationScope.raw(TABLE, Set.of("embedding"), TABLE));
        builds = new JdbcArtifactBuildStore(dataSource);
        previous = descriptor(TABLE, "embedding");
        var plan = VectorArtifactBuildPlanBuilder.build(List.of("lake", "raw", "items"), "embedding",
                BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef(), BuiltInCapabilityCatalog.hnswArtifactFormat());
        var submission = DurableIndexBuildSubmission.create(new DurableIndexBuildSubmission.Definition(
                TENANT, "alice", DurableIndexBuildService.jobId(TENANT, previous), previous, TABLE,
                plan, ArtifactTypeIds.VECTOR_INDEX, List.of(), List.of(), "0", "s3://warehouse/staging/index-1/0",
                "index-1-generation-0", new ArtifactBuildDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000), NOW));
        builds.createJob(submission.job(), submission.dispatch(), List.of());
    }

    @Test
    void replacementUsesPersistedRealSubjectAndResolvedScopeWithoutInventingGroups() {
        var replacement = previous.withGeneration(1, NOW.plusSeconds(1)).withBuildSnapshotId("42", NOW.plusSeconds(1));
        var resolved = DeferredIndexAuthorityResolver.resolve(builds, previous, replacement);
        assertThat(resolved.tenantId()).isEqualTo(TENANT);
        assertThat(resolved.table()).isEqualTo(TABLE);
        assertThat(resolved.session().getUser()).isEqualTo("alice");
        assertThat(resolved.session().getCatalog()).contains("lake");
        assertThat(resolved.session().getSchema()).contains("raw");
        assertThat(resolved.session().getProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "user", "alice", "tenant_id", TENANT, "catalog", "lake", "schema", "raw"));
    }

    @Test
    void revokedBuilderCannotAuthorizeBackgroundReplacementEvenThoughMetadataRemains() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM pangrova_user_roles WHERE user_id='alice'");
        }
        assertThat(builds.findDispatchSpec(TENANT, DurableIndexBuildService.jobId(TENANT, previous))).isPresent();
        assertThatThrownBy(() -> DeferredIndexAuthorityResolver.resolve(builds, previous,
                previous.withGeneration(1, NOW.plusSeconds(1))))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void replacementCannotExpandThePriorExactTableOrColumns() {
        var differentColumn = descriptor(TABLE, "other_embedding").withGeneration(1, NOW.plusSeconds(1));
        var differentTable = descriptor(new QualifiedTableName("lake", "raw", "other_items"), "embedding")
                .withGeneration(1, NOW.plusSeconds(1));
        assertThatThrownBy(() -> DeferredIndexAuthorityResolver.resolve(builds, previous, differentColumn))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> DeferredIndexAuthorityResolver.resolve(builds, previous, differentTable))
                .isInstanceOf(AccessDeniedException.class);
    }

    private static IndexDescriptor descriptor(QualifiedTableName table, String column) {
        return new IndexDescriptor("index-1", "vector_idx", table.catalog(), table.schema(), table.table(),
                List.of(column), IndexType.VECTOR, Map.of("algorithm", "HNSW"), IndexState.READY,
                null, 0, "41", null, NOW, NOW, TENANT);
    }
}
