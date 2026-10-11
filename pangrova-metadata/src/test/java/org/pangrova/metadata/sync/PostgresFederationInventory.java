package org.pangrova.metadata.sync;

import org.pangrova.metadata.adapter.FederatedCatalog;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.serde.TableMetadataSerializer;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Component-test source that reads actual PostgreSQL inventory through the production serializer.
 * It is not a production federation provider, a CDC implementation, or a Native execution fixture.
 */
final class PostgresFederationInventory implements FederatedCatalog {
    private final DataSource dataSource;
    private final String catalog;
    private final JdbcMetadataStore store;
    private final TableMetadataSerializer serializer = new TableMetadataSerializer();
    private boolean closed;

    PostgresFederationInventory(DataSource dataSource, String catalog) {
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.store = new JdbcMetadataStore(dataSource);
    }

    @Override
    public List<String> listSchemas() {
        requireOpen();
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement(
                        "SELECT schema_name FROM schema_metadata WHERE catalog_name=? ORDER BY schema_name")) {
            statement.setString(1, catalog);
            try (var rows = statement.executeQuery()) {
                List<String> schemas = new ArrayList<>();
                while (rows.next()) {
                    schemas.add(rows.getString(1));
                }
                return List.copyOf(schemas);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Actual PostgreSQL schema inventory read failed", failure);
        }
    }

    @Override
    public List<String> listTables(String schema) {
        requireOpen();
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("SELECT table_name FROM table_metadata "
                        + "WHERE catalog_name=? AND schema_name=? ORDER BY table_name")) {
            statement.setString(1, catalog);
            statement.setString(2, schema);
            try (var rows = statement.executeQuery()) {
                List<String> tables = new ArrayList<>();
                while (rows.next()) {
                    tables.add(rows.getString(1));
                }
                return List.copyOf(tables);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Actual PostgreSQL table inventory read failed", failure);
        }
    }

    @Override
    public TableMetadata getTable(String schema, String table) {
        requireOpen();
        return store.loadTable(catalog, schema, table)
                .orElseThrow(() -> new MetastoreException("Missing actual source table: " + schema + "." + table));
    }

    @Override
    public TableStatistics getTableStatistics(String schema, String table) {
        getTable(schema, table);
        throw new UnsupportedOperationException("The PostgreSQL inventory does not store data-plane statistics");
    }

    @Override
    public List<MetadataChange> getChangesSince(Instant since) {
        requireOpen();
        try (var connection = dataSource.getConnection();
                var statement = connection.prepareStatement("SELECT columns FROM table_metadata "
                        + "WHERE catalog_name=? AND synced_at>? ORDER BY schema_name, table_name")) {
            statement.setString(1, catalog);
            statement.setTimestamp(2, Timestamp.from(since));
            try (var rows = statement.executeQuery()) {
                List<MetadataChange> changes = new ArrayList<>();
                while (rows.next()) {
                    TableMetadata metadata = serializer.deserialize(rows.getString(1));
                    MetadataChange.ChangeType type = metadata.createdAt() != null
                            && metadata.createdAt().isAfter(since)
                            ? MetadataChange.ChangeType.ADD : MetadataChange.ChangeType.UPDATE;
                    changes.add(new MetadataChange(type,
                            new QualifiedTableRef(metadata.catalog(), metadata.schema(), metadata.table()), metadata));
                }
                return List.copyOf(changes);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Actual PostgreSQL changed inventory read failed", failure);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("PostgreSQL inventory source has closed");
        }
    }

    @Override
    public void close() {
        // All real JDBC reads use try-with-resources; the shared DataSource remains externally owned.
        closed = true;
    }
}
