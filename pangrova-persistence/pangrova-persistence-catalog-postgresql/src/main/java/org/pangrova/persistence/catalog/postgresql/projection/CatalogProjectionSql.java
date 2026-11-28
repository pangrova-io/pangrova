package org.pangrova.persistence.catalog.postgresql.projection;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.metadata.codec.table.TableMetadataSerializer;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import static org.pangrova.persistence.jdbc.operation.JdbcOperations.setJson;

/** SQL participants borrow the workflow connection and never commit, roll back or close it. */
@Internal
public final class CatalogProjectionSql {
    private static final TableMetadataSerializer TABLES = new TableMetadataSerializer();

    private CatalogProjectionSql() {}

    /** The caller already holds the deployment row lock. Zero is the unissued epoch. */
    public static boolean isCurrentObservation(Connection connection, String catalog, long epoch) throws SQLException {
        if (epoch <= 0) return false;
        try (var check = connection.prepareStatement(
                "SELECT 1 FROM catalog_definitions WHERE name=? AND observation_epoch=?")) {
            check.setString(1, catalog);
            check.setLong(2, epoch);
            try (var rows = check.executeQuery()) {
                return rows.next();
            }
        }
    }

    /** Loads the projected metadata of a table on the caller connection. */
    public static Optional<TableMetadata> load(Connection connection, QualifiedTableRef ref) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT columns "
                        + "FROM table_metadata WHERE catalog_name=? AND schema_name=? AND table_name=?")) {
            bindRef(statement, ref);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(readTable(rows, ref)) : Optional.empty();
            }
        }
    }

    private static TableMetadata readTable(ResultSet rows, QualifiedTableRef ref) throws SQLException {
        TableMetadata full = TABLES.deserialize(rows.getString("columns"));
        return new TableMetadata(ref.catalog(), ref.schema(), ref.table(), full.columns(),
                full.partitionSpec(), full.sortOrder(), full.properties(),
                full.createdAt(), full.updatedAt());
    }

    /** Inserts or updates the projected metadata of a table and reports whether a row changed. */
    public static boolean upsert(Connection connection, TableMetadata table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO table_metadata (catalog_name, schema_name, table_name, "
                        + "columns, partition_spec, sort_order, properties, table_created_at, "
                        + "table_updated_at, synced_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
                        + "ON CONFLICT (catalog_name, schema_name, table_name) DO UPDATE SET "
                        + "columns=EXCLUDED.columns, partition_spec=EXCLUDED.partition_spec, "
                        + "sort_order=EXCLUDED.sort_order, properties=EXCLUDED.properties, "
                        + "table_created_at=EXCLUDED.table_created_at, table_updated_at=EXCLUDED.table_updated_at, "
                        + "synced_at=EXCLUDED.synced_at")) {
            statement.setString(1, table.catalog());
            statement.setString(2, table.schema());
            statement.setString(3, table.table());
            setJson(statement, 4, TABLES.serialize(table));
            setJson(statement, 5, json(table.partitionSpec()));
            setJson(statement, 6, json(table.sortOrder()));
            setJson(statement, 7, json(table.properties()));
            statement.setTimestamp(8, timestamp(table.createdAt()));
            statement.setTimestamp(9, timestamp(table.updatedAt()));
            statement.setTimestamp(10, Timestamp.from(Instant.now()));
            return statement.executeUpdate() > 0;
        }
    }

    /** Deletes the projected metadata of a table and reports whether a row existed. */
    public static boolean delete(Connection connection, QualifiedTableRef ref) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM table_metadata WHERE catalog_name=? AND schema_name=? AND table_name=?")) {
            bindRef(statement, ref);
            return statement.executeUpdate() > 0;
        }
    }

    /** Records the observed schemas of a catalog and reports whether any row changed. */
    public static boolean observeSchemas(Connection connection, String catalog,
            List<String> schemas) throws SQLException {
        Timestamp now = Timestamp.from(Instant.now());
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO schema_metadata (catalog_name, schema_name, synced_at) "
                        + "VALUES (?,?,?) ON CONFLICT (catalog_name, schema_name) "
                        + "DO UPDATE SET synced_at=EXCLUDED.synced_at")) {
            for (String schema : schemas) {
                statement.setString(1, catalog);
                statement.setString(2, schema);
                statement.setTimestamp(3, now);
                statement.addBatch();
            }
            statement.executeBatch();
            return !schemas.isEmpty();
        }
    }

    private static void bindRef(PreparedStatement statement, QualifiedTableRef ref) throws SQLException {
        statement.setString(1, ref.catalog());
        statement.setString(2, ref.schema());
        statement.setString(3, ref.table());
    }

    private static String json(Object value) {
        try {
            return CanonicalJsonMapper.writeValueAsString(value);
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new MetastoreException("Cannot encode projection value", failure);
        }
    }

    private static Timestamp timestamp(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }

}
