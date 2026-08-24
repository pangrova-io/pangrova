package org.pangrova.metadata.store;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for JdbcMetadataStore using H2 in-memory database.
 * Validates: R17.2, AC17
 *
 * <p>Uses H2 with PostgreSQL compatibility mode to support ON CONFLICT upsert syntax.
 * No Docker dependency — runs entirely in-process.
 */
class JdbcMetadataStoreIntegrationTest {

    private static JdbcMetadataStore store;
    private static DataSource dataSource;

    @BeforeAll
    static void initSchema() throws SQLException {
        dataSource = createH2DataSource();
        createH2Schema(dataSource);
        store = new JdbcMetadataStore(dataSource);
    }

    @AfterAll
    static void shutdown() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SHUTDOWN");
        }
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
                CatalogState.ACTIVE, Map.of("key", "val"), "updated", Set.of());
        store.saveCatalogDescriptor(updated);

        Optional<CatalogDescriptor> result = store.getCatalogDescriptor("upsert_test");
        assertThat(result).isPresent();
        assertThat(result.get().state()).isEqualTo(CatalogState.ACTIVE);
        assertThat(result.get().comment()).isEqualTo("updated");
    }

    // --- Helpers ---

    private static void insertCatalog(
            String name, CatalogType type, String provider,
            CatalogState state, String propertiesJson,
            String comment, String allowedTenantsJson) throws SQLException {
        String sql = "MERGE INTO catalog_definitions (name, type, provider, state, "
                + "properties, comment, allowed_tenants) KEY (name) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             var ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, type.name());
            ps.setString(3, provider);
            ps.setString(4, state.name());
            ps.setString(5, propertiesJson);
            ps.setString(6, comment);
            ps.setString(7, allowedTenantsJson);
            ps.executeUpdate();
        }
    }

    private static DataSource createH2DataSource() {
        return new EmbeddedDataSource(
                "jdbc:h2:mem:pangrova_meta;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "");
    }

    private static void createH2Schema(DataSource ds) throws SQLException {
        String ddl = """
                CREATE TABLE catalog_definitions (
                    name VARCHAR(255) PRIMARY KEY,
                    type VARCHAR(50) NOT NULL,
                    provider VARCHAR(50) NOT NULL,
                    state VARCHAR(20) NOT NULL DEFAULT 'CREATING',
                    properties TEXT NOT NULL,
                    comment TEXT,
                    allowed_tenants TEXT,
                    created_by VARCHAR(255),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE schema_metadata (
                    catalog_name VARCHAR(255) NOT NULL,
                    schema_name VARCHAR(255) NOT NULL,
                    properties TEXT,
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (catalog_name, schema_name),
                    FOREIGN KEY (catalog_name) REFERENCES catalog_definitions(name) ON DELETE CASCADE
                );
                CREATE TABLE table_metadata (
                    catalog_name VARCHAR(255) NOT NULL,
                    schema_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    columns TEXT NOT NULL,
                    partition_spec TEXT,
                    sort_order TEXT,
                    properties TEXT,
                    table_created_at TIMESTAMP,
                    table_updated_at TIMESTAMP,
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (catalog_name, schema_name, table_name),
                    FOREIGN KEY (catalog_name, schema_name)
                        REFERENCES schema_metadata(catalog_name, schema_name) ON DELETE CASCADE
                );
                CREATE TABLE sync_history (
                    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    catalog_name VARCHAR(255) NOT NULL,
                    sync_type VARCHAR(20) NOT NULL,
                    sync_time TIMESTAMP NOT NULL,
                    changes_applied INT NOT NULL DEFAULT 0,
                    errors INT NOT NULL DEFAULT 0,
                    error_messages TEXT,
                    duration_ms BIGINT NOT NULL
                );
                CREATE INDEX idx_catalog_time ON sync_history (catalog_name, sync_time);
                """;
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    /**
     * Minimal DataSource backed by DriverManager.
     * Avoids external driver-specific DataSource dependency.
     */
    private static final class EmbeddedDataSource implements DataSource {

        private final String url;
        private final String user;
        private final String password;

        EmbeddedDataSource(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return DriverManager.getConnection(url, user, password);
        }

        @Override
        public Connection getConnection(String u, String p) throws SQLException {
            return DriverManager.getConnection(url, u, p);
        }

        @Override public PrintWriter getLogWriter() { return null; }
        @Override public void setLogWriter(PrintWriter out) { }
        @Override public void setLoginTimeout(int seconds) { }
        @Override public int getLoginTimeout() { return 0; }
        @Override public Logger getParentLogger() { return Logger.getLogger("EmbeddedDataSource"); }
        @Override public <T> T unwrap(Class<T> iface) { throw new UnsupportedOperationException(); }
        @Override public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}
