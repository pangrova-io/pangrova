package org.pangrova.coordinator.indexing.ddl;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.generation.api.constants.GenerationOutputTypeIds;
import org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.planner.generation.VectorGenerationPlanBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

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
    private PostgresGenerationJobStore builds;
    private IndexMetadataStore indexes;
    private IndexDescriptor previous;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("deferred_index_authority");
        GenerationAuthorityFixtures.provision(dataSource, TENANT, "alice",
                DeferredAuthorizationScope.raw(TABLE, Set.of("embedding"), TABLE));
        builds = ModelAuthorityFixtures.artifacts().builds(dataSource);
        previous = descriptor(TABLE, "embedding");
        indexes = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
        indexes.registerIndex(previous);
        // Historical persisted submission identity, retained by the V125 migration.
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE index_descriptors SET native_submission_attempt_id='submission' "
                    + "WHERE index_id='index-1'");
        }
        var plan = VectorGenerationPlanBuilder.build(List.of("lake", "raw", "items"), "embedding",
                BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef(), BuiltInCapabilityCatalog.hnswArtifactFormat());
        var submission = DurableIndexBuildSubmission.create(new DurableIndexBuildSubmission.Definition(
                TENANT, "alice", DurableIndexBuildService.jobId(TENANT, previous), previous, TABLE,
                plan, GenerationOutputTypeIds.VECTOR_INDEX, List.of(), List.of(), "0", "s3://warehouse/staging/index-1/0",
                "index-1-generation-0", new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000), NOW));
        builds.createJob(submission.job(), submission.dispatch(), List.of());
    }

    @Test
    void replacementUsesPersistedRealSubjectAndResolvedScopeWithoutInventingGroups() {
        var replacement = previous.withGeneration(1, NOW.plusSeconds(1)).withBuildSnapshotId("42", NOW.plusSeconds(1));
        var resolved = DeferredIndexAuthorityResolver.resolve(builds, indexes, previous, replacement);
        assertThat(resolved.tenantId()).isEqualTo(TENANT);
        assertThat(resolved.table()).isEqualTo(TABLE);
        assertThat(resolved.session().getUser()).isEqualTo("alice");
        assertThat(resolved.session().getCatalog()).contains("lake");
        assertThat(resolved.session().getSchema()).contains("raw");
        assertThat(resolved.session().getProperties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "user", "alice", "tenant_id", TENANT, "catalog", "lake", "schema", "raw"));
    }

    @Test
    void publishedAttemptSupersedesTerminalSiblingAuthority() throws Exception {
        String legacy = DurableIndexBuildService.jobId(TENANT, previous);
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='FAILED' WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, TENANT);
            statement.setString(2, legacy);
            assertThat(statement.executeUpdate()).isOne();
        }
        new org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore(dataSource)
                .grantRoleToUser(org.pangrova.security.api.policy.UserRoleAssignment.of("bob", "artifact_builder"));
        new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(dataSource)
                .addUserToTenant(TENANT, "bob", org.pangrova.security.api.enums.TenantRole.MEMBER);
        String attempt = java.util.UUID.randomUUID().toString();
        String jobId = DurableIndexBuildService.jobId(TENANT, previous, attempt);
        var plan = VectorGenerationPlanBuilder.build(List.of("lake", "raw", "items"), "embedding",
                BuiltInCapabilityCatalog.hnswArtifactBuildPlanRef(), BuiltInCapabilityCatalog.hnswArtifactFormat());
        var submission = DurableIndexBuildSubmission.create(new DurableIndexBuildSubmission.Definition(
                TENANT, "bob", jobId, previous, TABLE, plan, GenerationOutputTypeIds.VECTOR_INDEX,
                List.of(), List.of(), "0", "s3://warehouse/staging/index-1/attempt-" + attempt,
                jobId, new GenerationDispatchSpec.ResourceGrant(4096, 1, 4096, 60_000), NOW));
        builds.createJob(submission.job(), submission.dispatch(), List.of());
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "UPDATE artifact_build_jobs SET state='PUBLISHED',manifest_sha256=?,published_at=?,"
                        + "published_object_count=0,published_row_count=0,published_total_bytes=0 "
                        + "WHERE tenant_id=? AND job_id=?")) {
            statement.setString(1, "a".repeat(64));
            statement.setObject(2, java.time.OffsetDateTime.ofInstant(NOW, java.time.ZoneOffset.UTC));
            statement.setString(3, TENANT);
            statement.setString(4, jobId);
            assertThat(statement.executeUpdate()).isOne();
        }
        var replacement = previous.withGeneration(1, NOW.plusSeconds(1)).withBuildSnapshotId("42", NOW.plusSeconds(1));
        assertThat(DeferredIndexAuthorityResolver.resolve(builds, indexes, previous, replacement).session().getUser())
                .isEqualTo("bob");
    }

    @Test
    void revokedBuilderCannotAuthorizeBackgroundReplacementEvenThoughMetadataRemains() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM pangrova_user_roles WHERE user_id='alice'");
        }
        assertThat(builds.findDispatchSpec(TENANT, DurableIndexBuildService.jobId(TENANT, previous))).isPresent();
        assertThatThrownBy(() -> DeferredIndexAuthorityResolver.resolve(builds, indexes, previous,
                previous.withGeneration(1, NOW.plusSeconds(1))))
                .isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void replacementCannotExpandThePriorExactTableOrColumns() {
        var differentColumn = descriptor(TABLE, "other_embedding").withGeneration(1, NOW.plusSeconds(1));
        var differentTable = descriptor(new QualifiedTableName("lake", "raw", "other_items"), "embedding")
                .withGeneration(1, NOW.plusSeconds(1));
        assertThatThrownBy(() -> DeferredIndexAuthorityResolver.resolve(builds, indexes, previous, differentColumn))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> DeferredIndexAuthorityResolver.resolve(builds, indexes, previous, differentTable))
                .isInstanceOf(AccessDeniedException.class);
    }

    private static IndexDescriptor descriptor(QualifiedTableName table, String column) {
        return new IndexDescriptor("index-1", "vector_idx", table.catalog(), table.schema(), table.table(),
                List.of(column), IndexType.VECTOR, Map.of("algorithm", "HNSW"), IndexState.READY,
                null, 0, "41", null, NOW, NOW, TENANT);
    }
}
