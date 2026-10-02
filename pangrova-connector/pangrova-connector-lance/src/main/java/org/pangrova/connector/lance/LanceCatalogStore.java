package org.pangrova.connector.lance;

import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** PostgreSQL directory: table incarnation and visible version publish in one fenced transaction. */
final class LanceCatalogStore {
    private final String url;
    private final Properties credentials;

    LanceCatalogStore(String url, String user, String password) {
        if (url == null || !url.startsWith("jdbc:postgresql:")) {
            throw new IllegalArgumentException("Lance catalog requires a PostgreSQL JDBC URL");
        }
        this.url = url;
        credentials = new Properties();
        if (user != null) credentials.setProperty("user", user);
        if (password != null) credentials.setProperty("password", password);
        initialize();
    }

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(url, (Properties) credentials.clone());
    }

    private void initialize() {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("SELECT pg_advisory_xact_lock(hashtext('pangrova_lance_directory'))");
            statement.execute("CREATE TABLE IF NOT EXISTS pangrova_lance_schemas ("
                    + "catalog_name TEXT NOT NULL, tenant_id TEXT NOT NULL, schema_name TEXT NOT NULL, "
                    + "PRIMARY KEY(catalog_name,tenant_id,schema_name))");
            statement.execute("CREATE TABLE IF NOT EXISTS pangrova_lance_tables ("
                    + "catalog_name TEXT NOT NULL, tenant_id TEXT NOT NULL, schema_name TEXT NOT NULL, "
                    + "table_name TEXT NOT NULL, table_id TEXT NOT NULL, visible_version BIGINT NOT NULL, "
                    + "revision BIGINT NOT NULL, payload BYTEA NOT NULL, "
                    + "PRIMARY KEY(catalog_name,tenant_id,schema_name,table_name), "
                    + "FOREIGN KEY(catalog_name,tenant_id,schema_name) REFERENCES pangrova_lance_schemas "
                    + "(catalog_name,tenant_id,schema_name))");
            statement.execute("CREATE TABLE IF NOT EXISTS pangrova_lance_commits ("
                    + "catalog_name TEXT NOT NULL, tenant_id TEXT NOT NULL, table_id TEXT NOT NULL, "
                    + "fingerprint TEXT NOT NULL, published_version BIGINT NOT NULL, manifest_digest BYTEA NOT NULL, "
                    + "PRIMARY KEY(catalog_name,tenant_id,table_id,fingerprint))");
            statement.execute("CREATE TABLE IF NOT EXISTS pangrova_lance_versions ("
                    + "catalog_name TEXT NOT NULL, tenant_id TEXT NOT NULL, table_id TEXT NOT NULL, "
                    + "published_version BIGINT NOT NULL CHECK(published_version>0), payload BYTEA NOT NULL, "
                    + "manifest_digest BYTEA NOT NULL CHECK(octet_length(manifest_digest)=32), "
                    + "PRIMARY KEY(catalog_name,tenant_id,table_id,published_version))");
            connection.commit();
        } catch (SQLException failure) {
            throw failure("initialize", failure);
        }
    }

    void createSchema(String catalog, String tenant, String schema) {
        requireSchema(schema);
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO pangrova_lance_schemas VALUES (?,?,?) ON CONFLICT DO NOTHING")) {
            statement.setString(1, catalog);
            statement.setString(2, tenant);
            statement.setString(3, schema);
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw failure("create schema " + schema, failure);
        }
    }

    List<String> listSchemas(String catalog, String tenant) {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "SELECT schema_name FROM pangrova_lance_schemas WHERE catalog_name=? AND tenant_id=? "
                        + "ORDER BY schema_name")) {
            statement.setString(1, catalog);
            statement.setString(2, tenant);
            List<String> schemas = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) schemas.add(result.getString(1));
            }
            return List.copyOf(schemas);
        } catch (SQLException failure) {
            throw failure("list schemas", failure);
        }
    }

    static void requireSchema(String schema) {
        if (schema == null || schema.isBlank()) throw new IllegalArgumentException("Schema name must not be blank");
    }

    void create(LanceTableHandle table) {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO pangrova_lance_tables VALUES (?,?,?,?,?,?,?,?)")) {
            bindIdentity(statement, table.catalog(), table.tenantId(), table.table());
            statement.setString(5, table.tableId());
            statement.setLong(6, table.version());
            statement.setLong(7, table.revision());
            statement.setBytes(8, encode(table));
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw failure("create " + table.table(), failure);
        }
    }

    LanceTableHandle load(String catalog, String tenant, SchemaTableName table) {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "SELECT payload FROM pangrova_lance_tables WHERE catalog_name=? AND tenant_id=? "
                        + "AND schema_name=? AND table_name=?")) {
            bindIdentity(statement, catalog, tenant, table);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? decode(result.getBytes(1)) : null;
            }
        } catch (SQLException failure) {
            throw failure("load " + table, failure);
        }
    }

    List<SchemaTableName> list(String catalog, String tenant) {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "SELECT schema_name,table_name FROM pangrova_lance_tables WHERE catalog_name=? AND tenant_id=? "
                        + "ORDER BY schema_name,table_name")) {
            statement.setString(1, catalog);
            statement.setString(2, tenant);
            List<SchemaTableName> tables = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) tables.add(new SchemaTableName(result.getString(1), result.getString(2)));
            }
            return List.copyOf(tables);
        } catch (SQLException failure) {
            throw failure("list", failure);
        }
    }

    void publish(LanceOutputHandle output, String fingerprint, long version, byte[] manifestDigest, long rows) {
        LanceTableHandle base = output.table();
        validatePublication(base, version, manifestDigest, rows);
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            try {
                LanceTableHandle current = lockTable(connection, base);
                if (committed(connection, base, fingerprint, version, manifestDigest)) {
                    connection.commit();
                    return;
                }
                requireCurrentBase(current, base);
                long totalRows = output.allocation().operation()
                        == org.pangrova.spi.connector.metadata.NativeWriteAllocation.Operation.APPEND
                        ? Math.addExact(base.rowCount(), rows) : rows;
                LanceTableHandle published = new LanceTableHandle(base.catalog(), base.tenantId(), base.table(),
                        base.tableId(), base.datasetUri(), base.columns(), base.properties(), base.schemaFence(),
                        version, Math.addExact(base.revision(), 1), totalRows);
                updateTable(connection, published);
                insertCommit(connection, base, fingerprint, version, manifestDigest);
                insertVersion(connection, published, manifestDigest);
                connection.commit();
            } catch (RuntimeException | SQLException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw failure("publish " + base.table(), failure);
        }
    }

    private static void validatePublication(LanceTableHandle base, long version, byte[] digest, long rows) {
        if (version != Math.addExact(base.version(), 1) || digest.length != 32 || rows < 0) {
            throw new IllegalArgumentException("Invalid Native published identity or usage");
        }
    }

    Snapshot loadSnapshot(LanceTableHandle selector) {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "SELECT payload,manifest_digest FROM pangrova_lance_versions WHERE catalog_name=? "
                        + "AND tenant_id=? AND table_id=? AND published_version=?")) {
            statement.setString(1, selector.catalog());
            statement.setString(2, selector.tenantId());
            statement.setString(3, selector.tableId());
            statement.setLong(4, selector.version());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("Lance exact snapshot publication is absent");
                return new Snapshot(decode(result.getBytes(1)), result.getBytes(2));
            }
        } catch (SQLException failure) {
            throw failure("load exact snapshot " + selector.version(), failure);
        }
    }

    record Snapshot(LanceTableHandle table, byte[] manifestDigest) {
        Snapshot {
            manifestDigest = manifestDigest.clone();
            if (manifestDigest.length != 32) throw new IllegalStateException("Invalid snapshot manifest evidence");
        }
        @Override public byte[] manifestDigest() { return manifestDigest.clone(); }
    }

    private static void insertVersion(Connection connection, LanceTableHandle table, byte[] digest)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO pangrova_lance_versions VALUES (?,?,?,?,?,?)")) {
            statement.setString(1, table.catalog());
            statement.setString(2, table.tenantId());
            statement.setString(3, table.tableId());
            statement.setLong(4, table.version());
            statement.setBytes(5, encode(table));
            statement.setBytes(6, digest);
            statement.executeUpdate();
        }
    }

    private static void requireCurrentBase(LanceTableHandle current, LanceTableHandle base) {
        if (current == null || !current.tableId().equals(base.tableId())
                || current.revision() != base.revision() || current.version() != base.version()) {
            throw new IllegalStateException("Lance catalog CAS conflict");
        }
    }

    private static LanceTableHandle lockTable(Connection connection, LanceTableHandle base) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT payload FROM pangrova_lance_tables WHERE catalog_name=? AND tenant_id=? "
                        + "AND schema_name=? AND table_name=? FOR UPDATE")) {
            bindIdentity(statement, base.catalog(), base.tenantId(), base.table());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? decode(result.getBytes(1)) : null;
            }
        }
    }

    private static boolean committed(Connection connection, LanceTableHandle base, String fingerprint,
            long version, byte[] digest) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT published_version,manifest_digest FROM pangrova_lance_commits WHERE catalog_name=? "
                        + "AND tenant_id=? AND table_id=? AND fingerprint=?")) {
            statement.setString(1, base.catalog());
            statement.setString(2, base.tenantId());
            statement.setString(3, base.tableId());
            statement.setString(4, fingerprint);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) return false;
                if (result.getLong(1) != version || !java.security.MessageDigest.isEqual(result.getBytes(2), digest)) {
                    throw new IllegalStateException("Lance commit identity replay conflict");
                }
                return true;
            }
        }
    }

    private static void updateTable(Connection connection, LanceTableHandle table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE pangrova_lance_tables SET visible_version=?,revision=?,payload=? WHERE catalog_name=? "
                        + "AND tenant_id=? AND schema_name=? AND table_name=?")) {
            statement.setLong(1, table.version());
            statement.setLong(2, table.revision());
            statement.setBytes(3, encode(table));
            statement.setString(4, table.catalog());
            statement.setString(5, table.tenantId());
            statement.setString(6, table.table().schema());
            statement.setString(7, table.table().table());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Lance catalog publication lost table lock");
            }
        }
    }

    private static void insertCommit(Connection connection, LanceTableHandle table, String fingerprint,
            long version, byte[] digest) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO pangrova_lance_commits VALUES (?,?,?,?,?,?)")) {
            statement.setString(1, table.catalog());
            statement.setString(2, table.tenantId());
            statement.setString(3, table.tableId());
            statement.setString(4, fingerprint);
            statement.setLong(5, version);
            statement.setBytes(6, digest);
            statement.executeUpdate();
        }
    }

    void drop(LanceTableHandle table) {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM pangrova_lance_tables WHERE catalog_name=? AND tenant_id=? "
                        + "AND schema_name=? AND table_name=? AND table_id=? AND revision=?")) {
            bindIdentity(statement, table.catalog(), table.tenantId(), table.table());
            statement.setString(5, table.tableId());
            statement.setLong(6, table.revision());
            if (statement.executeUpdate() != 1) throw new IllegalStateException("Lance DROP catalog CAS conflict");
        } catch (SQLException failure) {
            throw failure("drop " + table.table(), failure);
        }
    }

    private static void bindIdentity(PreparedStatement statement, String catalog, String tenant,
            SchemaTableName table) throws SQLException {
        statement.setString(1, catalog);
        statement.setString(2, tenant);
        statement.setString(3, table.schema());
        statement.setString(4, table.table());
    }
    private static byte[] encode(LanceTableHandle table) {
        try {
            byte[] bytes = LanceOutputHandleCodec.MAPPER.writeValueAsBytes(table);
            if (bytes.length > LanceOutputHandleCodec.MAX_PAYLOAD) {
                throw new IllegalArgumentException("Lance catalog schema exceeds metadata limit");
            }
            return bytes;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot encode Lance catalog state", failure);
        }
    }
    private static LanceTableHandle decode(byte[] bytes) {
        if (bytes.length > LanceOutputHandleCodec.MAX_PAYLOAD) {
            throw new IllegalStateException("Lance catalog schema exceeds metadata limit");
        }
        try {
            return LanceOutputHandleCodec.MAPPER.readValue(bytes, LanceTableHandle.class);
        } catch (IOException failure) {
            throw new IllegalStateException("Invalid Lance catalog state", failure);
        }
    }
    private static IllegalStateException failure(String operation, SQLException cause) {
        return new IllegalStateException("Lance catalog " + operation + " failed", cause);
    }
}
