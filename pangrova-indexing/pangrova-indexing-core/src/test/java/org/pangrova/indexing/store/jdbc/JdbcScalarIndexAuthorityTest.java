package org.pangrova.indexing.store.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.store.jdbc.catalog.SchemaMigrationRunner;
import org.pangrova.metadata.exception.IndexAlreadyExistsException;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.metadata.testinfra.GenerationAuthorityFixtures;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.store.IndexGenerationFormat;
import org.pangrova.indexing.store.IndexPublicationRequest;
import org.pangrova.spi.security.AuthorizationDeniedException;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcScalarIndexAuthorityTest extends ContainerTestBase {
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
        indexes = new JdbcIndexMetadataStore(dataSource);
    }

    @Test
    void exactPersistedSubjectCanReauthorizeAfterStoreReopen() {
        var descriptor = descriptor("index-1");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);

        assertThat(new JdbcIndexMetadataStore(dataSource).loadAuthorizedBuildPrincipal(descriptor))
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
        assertThat(new JdbcIndexMetadataStore(dataSource)
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
        var descriptor = descriptor("index-publication");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);
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
        var descriptor = descriptor("index-empty");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);

        assertThat(indexes.publishIndexGeneration(publication(descriptor)).generation()).isZero();
        assertThat(indexes.findIndex(descriptor.indexId()).orElseThrow().state())
                .isEqualTo(IndexState.READY);
    }

    @Test
    void onlyExactBuildingSnapshotCanBePinned() {
        var descriptor = descriptor("index-generation");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);
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
        indexes.registerIndexFile(new IndexFileMapping(old.indexId(), "/data/old.parquet",
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
    void purgeRequiresDroppedStateAndDeletesMappingsAtomically() {
        var descriptor = descriptor("index-purge");
        indexes.registerIndexWithBuildAuthority(descriptor, PRINCIPAL);
        indexes.registerIndexFile(new IndexFileMapping(descriptor.indexId(), "/data/purge.parquet",
                "/index/purge.idx", 1, 16, "a".repeat(64), 0, "42", 0,
                Instant.now(), 0));

        assertThatThrownBy(() -> indexes.purgeIndex(descriptor.indexId()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(indexes.findIndex(descriptor.indexId())).isPresent();
        assertThat(indexes.getIndexFileMappings(descriptor.indexId())).hasSize(1);

        indexes.dropIndex(descriptor.indexId(), Instant.now());
        indexes.purgeIndex(descriptor.indexId());
        assertThat(indexes.findIndex(descriptor.indexId())).isEmpty();
        assertThat(indexes.getIndexFileMappings(descriptor.indexId())).isEmpty();
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
