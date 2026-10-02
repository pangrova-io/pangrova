package org.pangrova.metadata.store;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PostgreSQL integration test for {@link JdbcMetadataStore}.
 * Validates: R17.2, AC17
 */
class JdbcMetadataStoreIntegrationTest {

    private static JdbcMetadataStore store;
    private static DataSource dataSource;

    @BeforeAll
    static void initSchema() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("metadata_store");
        store = new JdbcMetadataStore(dataSource);
    }

    @Test
    void getCatalogDescriptorReturnsInsertedRow() throws SQLException {
        insertCatalog("test_catalog", CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, "{}", "test comment", "[]");

        Optional<CatalogDescriptor> result = store.getCatalogDescriptor("test_catalog");

        assertThat(result).isPresent();
        CatalogDescriptor d = result.get();
        assertThat(d.name()).isEqualTo("test_catalog");
        assertThat(d.type()).isEqualTo(CatalogType.INTERNAL);
        assertThat(d.provider()).isEqualTo("iceberg");
        assertThat(d.state()).isEqualTo(CatalogState.ACTIVE);
        assertThat(d.comment()).isEqualTo("test comment");
    }

    @Test
    void listAllCatalogDescriptorsReturnsAll() throws SQLException {
        insertCatalog("list_a", CatalogType.CONNECTOR, "hive",
                CatalogState.ACTIVE, "{}", "", "[]");
        insertCatalog("list_b", CatalogType.FEDERATION, "trino",
                CatalogState.CREATING, "{}", "", "[]");

        List<CatalogDescriptor> all = store.listAllCatalogDescriptors();

        assertThat(all.stream().map(CatalogDescriptor::name))
                .contains("list_a", "list_b");
    }

    @Test
    void deleteCatalogDescriptorRemovesIt() throws SQLException {
        insertCatalog("to_delete", CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, "{}", "", "[]");

        store.deleteCatalogDescriptor("to_delete");

        assertThat(store.getCatalogDescriptor("to_delete")).isEmpty();
    }

    @Test
    void saveCatalogDescriptorUpsertsExisting() {
        CatalogDescriptor original = new CatalogDescriptor(
                "upsert_test", CatalogType.INTERNAL, "iceberg",
                CatalogState.CREATING, Map.of(), "original", Set.of());
        store.saveCatalogDescriptor(original);

        CatalogDescriptor updated = new CatalogDescriptor(
                "upsert_test", CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, Map.of("key", "val"), "updated", Set.of(),
                original.incarnationId(), original.revision() + 1);
        store.saveCatalogDescriptor(updated);

        Optional<CatalogDescriptor> result = store.getCatalogDescriptor("upsert_test");
        assertThat(result).isPresent();
        assertThat(result.get().state()).isEqualTo(CatalogState.ACTIVE);
        assertThat(result.get().comment()).isEqualTo("updated");
        assertThat(result.get().incarnationId()).isEqualTo(original.incarnationId());
        assertThat(result.get().revision()).isEqualTo(2);
        assertThat(store.findCatalogDescriptorRevision(
                original.name(), original.incarnationId(), 1)).contains(original);
        assertThat(store.findCatalogDescriptorRevision(
                updated.name(), updated.incarnationId(), 2)).contains(updated);
        assertThat(store.findCatalogDescriptorRevision(
                updated.name(), updated.incarnationId(), 3)).isEmpty();
        assertThat(store.findCatalogDescriptorRevision(
                updated.name(), "another-incarnation", 1)).isEmpty();
    }

    @Test
    void historicalRevisionSurvivesDropAndNewIncarnation() {
        CatalogDescriptor old = new CatalogDescriptor(
                "recreated_catalog", CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, Map.of("endpoint", "old"), "", Set.of("tenant-a"));
        store.saveCatalogDescriptor(old);
        store.deleteCatalogDescriptor(old.name());

        CatalogDescriptor replacement = new CatalogDescriptor(
                old.name(), CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, Map.of("endpoint", "new"), "", Set.of("tenant-b"));
        store.saveCatalogDescriptor(replacement);

        assertThat(store.findCatalogDescriptorRevision(
                old.name(), old.incarnationId(), old.revision())).contains(old);
        assertThat(store.findCatalogDescriptorRevision(
                replacement.name(), replacement.incarnationId(), replacement.revision()))
                .contains(replacement);
        assertThat(store.findCatalogDescriptorRevision(old.name(), "", 1)).isEmpty();
        assertThat(store.findCatalogDescriptorRevision(old.name(), old.incarnationId(), 0)).isEmpty();
    }

    @Test
    void rejectedRevisionDoesNotAlterCurrentOrHistoricalSnapshot() throws SQLException {
        CatalogDescriptor first = new CatalogDescriptor(
                "fenced_history", CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, Map.of("endpoint", "one"), "", Set.of());
        store.saveCatalogDescriptor(first);
        CatalogDescriptor conflicting = new CatalogDescriptor(
                first.name(), first.type(), first.provider(), first.state(),
                Map.of("endpoint", "forged"), "", Set.of(), first.incarnationId(), 1);

        assertThatThrownBy(() -> store.saveCatalogDescriptor(conflicting))
                .isInstanceOf(org.pangrova.metadata.exception.MetastoreException.class)
                .hasMessageContaining("revision fence");
        assertThat(store.getCatalogDescriptor(first.name())).contains(first);
        assertThat(store.findCatalogDescriptorRevision(
                first.name(), first.incarnationId(), 1)).contains(first);

        try (Connection conn = dataSource.getConnection();
                var ps = conn.prepareStatement("UPDATE catalog_descriptor_revisions "
                        + "SET provider='forged' WHERE name=? AND incarnation_id=? AND revision=1")) {
            ps.setString(1, first.name());
            ps.setString(2, first.incarnationId());
            assertThatThrownBy(ps::executeUpdate)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("immutable");
        }
    }

    @Test
    void currentAndHistoryBecomeVisibleTogetherAfterBlockedHistoryInsert() throws Exception {
        DataSource isolated = SharedPostgresContainer.migratedDataSource("catalog_history_atomic");
        var isolatedStore = new JdbcMetadataStore(isolated);
        CatalogDescriptor first = new CatalogDescriptor(
                "atomic_catalog", CatalogType.INTERNAL, "iceberg", CatalogState.ACTIVE,
                Map.of("version", "one"), "", Set.of());
        isolatedStore.saveCatalogDescriptor(first);
        CatalogDescriptor second = new CatalogDescriptor(
                first.name(), first.type(), first.provider(), first.state(),
                Map.of("version", "two"), "", Set.of(), first.incarnationId(), 2);

        var executor = Executors.newSingleThreadExecutor();
        try (Connection blocker = isolated.getConnection()) {
            blocker.setAutoCommit(false);
            try (var lock = blocker.createStatement()) {
                lock.execute("LOCK TABLE catalog_descriptor_revisions IN SHARE MODE");
            }
            var writer = executor.submit(() -> isolatedStore.saveCatalogDescriptor(second));
            try {
                assertThat(waitForHistoryInsertLock(isolated)).isTrue();
                assertThat(isolatedStore.getCatalogDescriptor(first.name())).contains(first);
                assertThat(isolatedStore.findCatalogDescriptorRevision(
                        first.name(), first.incarnationId(), 1)).contains(first);
                assertThat(isolatedStore.findCatalogDescriptorRevision(
                        first.name(), first.incarnationId(), 2)).isEmpty();
            } finally {
                blocker.commit();
            }
            writer.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(isolatedStore.getCatalogDescriptor(first.name())).contains(second);
        assertThat(isolatedStore.findCatalogDescriptorRevision(
                first.name(), first.incarnationId(), 2)).contains(second);
    }

    private static boolean waitForHistoryInsertLock(DataSource dataSource) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        try (Connection reader = dataSource.getConnection();
                var ps = reader.prepareStatement("SELECT count(*) FROM pg_locks "
                        + "WHERE relation='catalog_descriptor_revisions'::regclass AND NOT granted")) {
            while (System.nanoTime() < deadline) {
                try (var rows = ps.executeQuery()) {
                    rows.next();
                    if (rows.getLong(1) > 0) {
                        return true;
                    }
                }
                Thread.yield();
            }
        }
        return false;
    }

    // --- Helpers ---

    private static void insertCatalog(
            String name, CatalogType type, String provider,
            CatalogState state, String propertiesJson,
            String comment, String allowedTenantsJson) throws SQLException {
        String sql = "INSERT INTO catalog_definitions (name, type, provider, state, "
                + "properties, comment, allowed_tenants) VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (name) DO UPDATE SET type=EXCLUDED.type, "
                + "provider=EXCLUDED.provider, state=EXCLUDED.state, "
                + "properties=EXCLUDED.properties, comment=EXCLUDED.comment, "
                + "allowed_tenants=EXCLUDED.allowed_tenants";
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, type.name());
            ps.setString(3, provider);
            ps.setString(4, state.name());
            ps.setObject(5, propertiesJson, Types.OTHER);
            ps.setString(6, comment);
            ps.setObject(7, allowedTenantsJson, Types.OTHER);
            ps.executeUpdate();
        }
    }
}
