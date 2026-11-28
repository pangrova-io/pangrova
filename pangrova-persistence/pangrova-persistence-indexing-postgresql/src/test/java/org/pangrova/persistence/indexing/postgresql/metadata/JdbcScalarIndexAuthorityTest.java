package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;

import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.persistence.jdbc.migration.SchemaMigrationRunner;
import org.pangrova.indexing.api.exception.IndexAlreadyExistsException;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
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

class JdbcScalarIndexAuthorityTest extends ContainerTestBase {
    private static final String OWNER = "mapping-test-owner";
    private static final String TENANT = "tenant-scalar-index";
    private static final String PRINCIPAL = "alice";
    private static final QualifiedTableName TABLE = new QualifiedTableName("lake", "raw", "events");

    private DataSource dataSource;
    private JdbcIndexMetadataStore indexes;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("scalar_index_authority");
        SchemaMigrationRunner.migrate(dataSource);
        GenerationAuthorityFixtures.provision(dataSource, TENANT, PRINCIPAL,
                DeferredAuthorizationScope.raw(TABLE, Set.of("value"), TABLE));
        indexes = new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent);
    }

    @Test
    void exactPersistedSubjectCanReauthorizeAfterStoreReopen() {
        var descriptor = descriptor("index-1");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);

        assertThat(new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent).loadAuthorizedBuildPrincipal(descriptor))
                .isEqualTo(PRINCIPAL);
        assertThatThrownBy(() -> indexes.loadAuthorizedBuildPrincipal(
                new IndexDescriptor(descriptor.indexId(), descriptor.indexName(), "other", "raw", "events",
                        descriptor.columnNames(), descriptor.indexType(), descriptor.properties(),
                        descriptor.state(), null, descriptor.generation(), descriptor.buildSnapshotId(),
                        descriptor.lastIndexedSnapshotId(), descriptor.createdAt(), descriptor.updatedAt(), TENANT)))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void revokedGrantAndUnscopedHistoricalDescriptorCannotReplay() throws Exception {
        var scoped = descriptor("index-scoped");
        indexes.registerIndexWithBuildAuthority(scoped, PRINCIPAL);
        var historical = descriptor("index-historical");
        indexes.registerIndex(historical);
        assertThatThrownBy(() -> indexes.loadAuthorizedBuildPrincipal(historical))
                .isInstanceOf(AccessDeniedException.class);

        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DELETE FROM security_grants WHERE privilege='SELECT'");
        }
        assertThatThrownBy(() -> indexes.loadAuthorizedBuildPrincipal(scoped))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(indexes.findIndex(scoped.indexId())).isPresent();
    }

    @Test
    void droppedIndexCleanupRetainsExactScopeAfterReadGrantRevocation() throws Exception {
        var descriptor = descriptor("index-revoked-cleanup");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);
        assertThatThrownBy(() -> indexes.loadDroppedIndexCleanupPrincipal(descriptor))
                .isInstanceOf(AccessDeniedException.class);
        indexes.dropIndex(descriptor.indexId(), Instant.now());
        var dropped = indexes.findIndex(descriptor.indexId()).orElseThrow();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DELETE FROM security_grants WHERE privilege='SELECT'");
        }

        assertThatThrownBy(() -> indexes.loadAuthorizedBuildPrincipal(dropped))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(new JdbcIndexMetadataStore(dataSource, TableWriteAuthorityFixture.create()::requireCurrent)
                .loadDroppedIndexCleanupPrincipal(dropped)).isEqualTo(PRINCIPAL);
        assertThatThrownBy(() -> indexes.loadDroppedIndexCleanupPrincipal(
                new IndexDescriptor(dropped.indexId(), dropped.indexName(), "other", "raw", "events",
                        dropped.columnNames(), dropped.indexType(), dropped.properties(),
                        dropped.state(), null, dropped.generation(), dropped.buildSnapshotId(),
                        dropped.lastIndexedSnapshotId(), dropped.createdAt(), dropped.updatedAt(), TENANT)))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void publicationChecksCurrentGrantOnItsOwnTransaction() throws Exception {
        var descriptor = registerForPublication("index-publication");
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DELETE FROM security_grants WHERE privilege='SELECT'");
        }

        assertThatThrownBy(() -> indexes.publishIndexGeneration(publication(descriptor)))
                .isInstanceOf(AuthorizationDeniedException.class);
        assertThat(indexes.findIndex(descriptor.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.BUILDING);
        assertThat(indexes.findActiveIndexGeneration(descriptor.indexId())).isEmpty();
    }

    @Test
    void authorizedEmptyGenerationPublishesReady() {
        var descriptor = registerForPublication("index-empty");

        assertThat(indexes.publishIndexGeneration(publication(descriptor)).generation()).isZero();
        assertThat(indexes.findIndex(descriptor.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.READY);
    }

    @Test
    void publicationRejectsDifferentFenceEvenAfterSuccessfulAdmission() {
        var descriptor = registerForPublication("index-other-fence");
        var incorrectFence = descriptor.withBuildSnapshotId(
                descriptor.buildSnapshotId(), descriptor.updatedAt().plusMillis(1));

        assertThatThrownBy(() -> indexes.publishIndexGeneration(publication(incorrectFence)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("publication fence");
        assertThat(indexes.findIndex(descriptor.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.BUILDING);
        assertThat(indexes.findActiveIndexGeneration(descriptor.indexId())).isEmpty();
        assertThat(indexes.publishIndexGeneration(publication(descriptor)).generation()).isZero();
    }

    @Test
    void injectedAuthorityUsesPublicationTransactionAndFailureRollsBack() throws Exception {
        var descriptor = registerForPublication("index-authority-participant");
        var observed = new java.util.concurrent.atomic.AtomicReference<java.sql.Connection>();
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var participantStore = new JdbcIndexMetadataStore(dataSource, (connection, principal, scope) -> {
            assertThat(connection.getAutoCommit()).isFalse();
            assertThat(principal).isEqualTo(PRINCIPAL);
            observed.set(connection);
            calls.incrementAndGet();
            TableWriteAuthorityFixture.create().requireCurrent(connection, principal, scope);
            try (var statement = connection.prepareStatement(
                    "UPDATE index_descriptors SET stale_reason='authority-probe' WHERE index_id=?")) {
                statement.setString(1, descriptor.indexId());
                assertThat(statement.executeUpdate()).isEqualTo(1);
            }
            throw new java.sql.SQLException("participant failure");
        });
        assertThat(calls).hasValue(0);
        assertThatThrownBy(() -> participantStore.publishIndexGeneration(publication(descriptor)))
                .hasRootCauseMessage("participant failure");
        assertThat(calls).hasValue(1);
        assertThat(observed.get().isClosed()).isTrue();
        var retained = indexes.findIndex(descriptor.indexId()).orElseThrow();
        assertThat(retained.state()).isEqualTo(IndexState.BUILDING);
        assertThat(retained.staleReason()).isNull();
        assertThat(indexes.findActiveIndexGeneration(descriptor.indexId())).isEmpty();
    }

    @Test
    void authorityIsRequiredAtConstruction() {
        assertThatThrownBy(() -> new JdbcIndexMetadataStore(dataSource, null))
                .isInstanceOf(NullPointerException.class).hasMessage("authorization");
    }

    @Test
    void onlyExactBuildingSnapshotCanBePinned() {
        var descriptor = registerForPublication("index-generation");
        assertThat(indexes.isIndexSnapshotBuildable(descriptor.indexId(), "42")).isTrue();
        assertThat(indexes.isIndexSnapshotBuildable(descriptor.indexId(), "43")).isFalse();

        indexes.publishIndexGeneration(publication(descriptor));
        assertThat(indexes.isIndexSnapshotBuildable(descriptor.indexId(), "42")).isFalse();
        var ready = indexes.findIndex(descriptor.indexId()).orElseThrow();
        assertThat(indexes.beginIndexGeneration(ready.indexId(), 1, "43",
                ready.updatedAt().plusMillis(1), ready.updatedAt(), ready.state(), ready.generation()))
                .isTrue();
        assertThat(indexes.isIndexSnapshotBuildable(descriptor.indexId(), "43")).isTrue();
        assertThat(indexes.isIndexSnapshotBuildable(descriptor.indexId(), "42")).isFalse();
    }

    @Test
    void sameNameRecreationKeepsDroppedIndexAndFilesForGc() {
        var old = namedDescriptor("index-old", "shared");
        indexes.registerIndexWithBuildAuthority(old, PRINCIPAL);
        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim(old.indexId(), 0, OWNER, Instant.now())).isTrue();
        indexes.registerIndexFile(OWNER, new IndexFileMapping(old.indexId(), "/data/old.parquet",
                "/index/old.idx", 1, 16, "a".repeat(64), 0, "42", 0,
                Instant.now(), 0));
        indexes.dropIndex(old.indexId(), Instant.now());

        var replacement = namedDescriptor("index-new", "shared");
        indexes.registerIndexWithBuildAuthority(replacement, PRINCIPAL);

        assertThat(indexes.findIndex(old.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.DROPPED);
        assertThat(indexes.getIndexFileMappings(old.indexId())).hasSize(1);
        assertThat(indexes.findIndex(replacement.indexId())).isPresent();
        assertThatThrownBy(() -> indexes.registerIndexWithBuildAuthority(
                namedDescriptor("index-third", "shared"), PRINCIPAL))
                .isInstanceOf(IndexAlreadyExistsException.class);
    }

    @Test
    void purgeRequiresDroppedStateAndDurableMappingRetirement() {
        var descriptor = descriptor("index-purge");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);
        assertThat(new JdbcIndexBuildClaimRepository(dataSource).tryClaim(descriptor.indexId(), 0, OWNER, Instant.now())).isTrue();
        indexes.registerIndexFile(OWNER, new IndexFileMapping(descriptor.indexId(), "/data/purge.parquet",
                "/index/purge.idx", 1, 16, "a".repeat(64), 0, "42", 0,
                Instant.now(), 0));

        assertThatThrownBy(() -> indexes.purgeIndex(descriptor.indexId()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(indexes.findIndex(descriptor.indexId())).isPresent();
        assertThat(indexes.getIndexFileMappings(descriptor.indexId())).hasSize(1);

        indexes.dropIndex(descriptor.indexId(), Instant.now());
        assertThatThrownBy(() -> indexes.purgeIndex(descriptor.indexId()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("remaining mappings");
        var dropped = indexes.findIndex(descriptor.indexId()).orElseThrow();
        assertThat(indexes.retireDroppedIndexMappings(dropped.indexId(), dropped.updatedAt(), dropped.generation()))
                .isTrue();
        assertThat(indexes.getIndexFileMappings(descriptor.indexId())).isEmpty();
        var retirement = indexes.pollMappedArtifactRetirements(10).get(0);
        assertThatThrownBy(() -> indexes.purgeIndex(descriptor.indexId()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retirement");
        // Database-only fixture; no physical artifact was created.
        assertThat(indexes.beginMappedArtifactReclaim(retirement)).isTrue();
        indexes.completeMappedArtifactReclaim(retirement);
        indexes.purgeIndex(descriptor.indexId());
        assertThat(indexes.findIndex(descriptor.indexId())).isEmpty();
    }

    private IndexDescriptor registerForPublication(String id) {
        var descriptor = descriptor(id);
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);
        // Admission stores a microsecond timestamp. Keep this exact fence for the whole build.
        return indexes.findIndex(id).orElseThrow();
    }

    private static IndexPublicationRequest publication(IndexDescriptor descriptor) {
        return new IndexPublicationRequest(descriptor.indexId(), descriptor.indexId(),
                descriptor.generation(), descriptor.buildSnapshotId(), descriptor.indexType().name(),
                new IndexGenerationFormat("pangrova.index-file", 1, 0, "b".repeat(64), "test-writer"),
                0, descriptor.updatedAt(), descriptor.updatedAt().plusMillis(1));
    }

    private static IndexDescriptor descriptor(String id) {
        return namedDescriptor(id, id);
    }

    private static IndexDescriptor namedDescriptor(String id, String name) {
        Instant now = Instant.now();
        return new IndexDescriptor(id, name, TABLE.catalog(), TABLE.schema(), TABLE.table(),
                List.of("value"), IndexType.BLOOM_FILTER, Map.of(), IndexState.BUILDING,
                null, 0, "42", null, now, now, TENANT);
    }
}
