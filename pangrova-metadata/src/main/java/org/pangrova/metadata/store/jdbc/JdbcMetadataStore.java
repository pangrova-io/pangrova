package org.pangrova.metadata.store.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.metadata.core.cache.InvalidationEntry.InvalidationType;
import org.pangrova.metadata.core.store.MetadataStore;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.api.error.MetastoreException;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.core.store.SyncResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.metadata.store.jdbc.JdbcSupport.setJson;

/**
 * JDBC-backed MetadataStore for PostgreSQL persistence.
 */
public class JdbcMetadataStore implements MetadataStore {

    private static final int SEARCH_LIMIT = 100;
    private static final String CACHE_KEY_SEPARATOR = "\u001F";
    private static final String INSERT_INVALIDATION =
            "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                    + "VALUES (?, ?, NOW(), NOW())";

    private final DataSource dataSource;
    private final TableMetadataSerializer serializer;

    public JdbcMetadataStore(DataSource dataSource) {
        this.dataSource = dataSource;
        this.serializer = new TableMetadataSerializer();
    }

    @Override
    public void saveTable(TableMetadata metadata) {
        String sql = "INSERT INTO table_metadata (catalog_name, schema_name, table_name, "
                + "columns, partition_spec, sort_order, properties, table_created_at, "
                + "table_updated_at, synced_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (catalog_name, schema_name, table_name) DO UPDATE SET "
                + "columns=EXCLUDED.columns, partition_spec=EXCLUDED.partition_spec, "
                + "sort_order=EXCLUDED.sort_order, properties=EXCLUDED.properties, "
                + "table_updated_at=EXCLUDED.table_updated_at, synced_at=EXCLUDED.synced_at";
        executeMutation("save table", tableCacheKey(metadata.catalog(), metadata.schema(), metadata.table()),
                InvalidationType.TABLE, conn -> executeUpdate(conn, sql, ps -> bindSaveTable(ps, metadata)) > 0);
    }

    @Override
    public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
        String sql = "SELECT columns, partition_spec, sort_order, properties, "
                + "table_created_at, table_updated_at FROM table_metadata "
                + "WHERE catalog_name=? AND schema_name=? AND table_name=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        }, rs -> rs.next() ? Optional.of(mapTableMetadata(rs, catalog, schema, table)) : Optional.empty());
    }

    @Override
    public void deleteTable(String catalog, String schema, String table) {
        String sql = "DELETE FROM table_metadata WHERE catalog_name=? AND schema_name=? AND table_name=?";
        executeMutation("delete table", tableCacheKey(catalog, schema, table), InvalidationType.TABLE,
                conn -> executeUpdate(conn, sql, ps -> {
                    ps.setString(1, catalog);
                    ps.setString(2, schema);
                    ps.setString(3, table);
                }) > 0);
    }

    @Override
    public List<QualifiedTableRef> searchTables(String pattern) {
        String sql = "SELECT catalog_name, schema_name, table_name FROM table_metadata "
                + "WHERE table_name LIKE ? ESCAPE '\\' LIMIT ?";
        return executeQuery(sql, ps -> {
            ps.setString(1, "%" + escapeLikePattern(pattern) + "%");
            ps.setInt(2, SEARCH_LIMIT);
        }, this::mapTableRefs);
    }

    private static String escapeLikePattern(String input) {
        return input.replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_");
    }

    @Override
    public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
        String sql = "INSERT INTO catalog_definitions (name, type, provider, state, properties, "
                + "comment, allowed_tenants, incarnation_id, revision) VALUES (?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (name) DO UPDATE SET type=EXCLUDED.type, provider=EXCLUDED.provider, "
                + "state=EXCLUDED.state, properties=EXCLUDED.properties, "
                + "comment=EXCLUDED.comment, allowed_tenants=EXCLUDED.allowed_tenants, "
                + "revision=EXCLUDED.revision "
                + "WHERE catalog_definitions.incarnation_id=EXCLUDED.incarnation_id "
                + "AND EXCLUDED.revision=catalog_definitions.revision+1";
        String historySql = "INSERT INTO catalog_descriptor_revisions "
                + "(name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision) VALUES (?,?,?,?,?,?,?,?,?)";
        executeMutation("save catalog descriptor", descriptor.name(), InvalidationType.CATALOG, conn -> {
            try (PreparedStatement current = conn.prepareStatement(sql);
                    PreparedStatement history = conn.prepareStatement(historySql)) {
                bindSaveCatalog(current, descriptor);
                if (current.executeUpdate() != 1) {
                    throw new MetastoreException(
                            "Catalog revision fence rejected save: " + descriptor.name());
                }
                bindSaveCatalog(history, descriptor);
                history.executeUpdate();
                return true;
            }
        });
    }

    @Override
    public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision "
                + "FROM catalog_definitions WHERE name=?";
        return executeQuery(sql, ps -> ps.setString(1, name),
                rs -> rs.next() ? Optional.of(mapCatalogDescriptor(rs)) : Optional.empty());
    }

    @Override
    public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String name, String incarnationId, long revision) {
        if (name == null || name.isBlank() || incarnationId == null
                || incarnationId.isBlank() || revision <= 0) {
            return Optional.empty();
        }
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision FROM catalog_descriptor_revisions "
                + "WHERE name=? AND incarnation_id=? AND revision=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, name);
            ps.setString(2, incarnationId);
            ps.setLong(3, revision);
        }, rs -> rs.next() ? Optional.of(mapCatalogDescriptor(rs)) : Optional.empty());
    }

    @Override
    public void deleteCatalogDescriptor(String name) {
        executeMutation("delete catalog descriptor", name, InvalidationType.CATALOG,
                conn -> executeUpdate(conn, "DELETE FROM catalog_definitions WHERE name=?",
                        ps -> ps.setString(1, name)) > 0);
    }

    @Override
    public List<CatalogDescriptor> listAllCatalogDescriptors() {
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision "
                + "FROM catalog_definitions";
        return executeQuery(sql, ps -> { }, this::mapCatalogDescriptors);
    }

    @Override
    public Optional<Instant> getLastSyncTime(String catalogName) {
        String sql = "SELECT MAX(sync_time) FROM sync_history WHERE catalog_name=?";
        return executeQuery(sql, ps -> ps.setString(1, catalogName), rs -> {
            if (!rs.next()) return Optional.empty();
            Timestamp ts = rs.getTimestamp(1);
            return ts == null ? Optional.empty() : Optional.of(ts.toInstant());
        });
    }

    @Override
    public void updateSyncTime(String catalogName, Instant syncTime) {
        // No-op: sync time is tracked via recordSyncHistory
    }

    @Override
    public void recordSyncHistory(SyncResult result) {
        String sql = "INSERT INTO sync_history (catalog_name, sync_type, sync_time, "
                + "changes_applied, errors, error_messages, duration_ms) VALUES (?,?,?,?,?,?,?)";
        executeUpdate(sql, ps -> bindSyncHistory(ps, result));
    }

    @Override
    public void saveSchemas(String catalogName, List<String> schemas) {
        String sql = "INSERT INTO schema_metadata (catalog_name, schema_name, synced_at) "
                + "VALUES (?,?,?) ON CONFLICT (catalog_name, schema_name) DO UPDATE SET synced_at=EXCLUDED.synced_at";
        Timestamp now = Timestamp.from(Instant.now());
        executeMutation("save schemas", catalogName, InvalidationType.CATALOG, conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (String schema : schemas) {
                    ps.setString(1, catalogName);
                    ps.setString(2, schema);
                    ps.setTimestamp(3, now);
                    ps.addBatch();
                }
                ps.executeBatch();
                return !schemas.isEmpty();
            }
        });
    }

    // --- SQL execution helpers ---

    /** Commits a local projection mutation and its durable cache invalidation together. */
    private void executeMutation(String operation, String cacheKey, InvalidationType type,
            JdbcSupport.SqlFunction<Connection, Boolean> mutation) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                if (mutation.apply(conn)) {
                    executeUpdate(conn, INSERT_INVALIDATION, ps -> {
                        ps.setString(1, cacheKey);
                        ps.setString(2, type.name());
                    });
                }
                commitMutation(conn, operation, cacheKey);
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackMutation(conn, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException(
                    "Metadata mutation failed: operation=" + operation + " cacheKey=" + cacheKey
                            + " sqlState=" + failure.getSQLState(), failure);
        }
    }

    private static void commitMutation(Connection conn, String operation, String cacheKey) {
        try {
            conn.commit();
        } catch (SQLException failure) {
            // A failed COMMIT acknowledgment cannot establish whether the database committed.
            throw new MetastoreException(
                    "Metadata mutation commit outcome unconfirmed: operation=" + operation
                            + " cacheKey=" + cacheKey + " sqlState=" + failure.getSQLState(), failure);
        }
    }

    private static void rollbackMutation(Connection conn, Throwable failure) {
        try {
            conn.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private static int executeUpdate(Connection conn, String sql, JdbcSupport.SqlConsumer binder)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            return ps.executeUpdate();
        }
    }

    private static String tableCacheKey(String catalog, String schema, String table) {
        return catalog + CACHE_KEY_SEPARATOR + schema + CACHE_KEY_SEPARATOR + table;
    }

    private <T> T executeQuery(String sql, JdbcSupport.SqlConsumer binder,
            JdbcSupport.SqlFunction<ResultSet, T> extractor) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return extractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new MetastoreException("Query failed: " + sql, e);
        }
    }

    private void executeUpdate(String sql, JdbcSupport.SqlConsumer binder) {
        try (Connection conn = dataSource.getConnection()) {
            executeUpdate(conn, sql, binder);
        } catch (SQLException e) {
            throw new MetastoreException("Update failed: " + sql, e);
        }
    }

    // --- Bind helpers ---

    private void bindSaveTable(PreparedStatement ps, TableMetadata m) throws SQLException {
        ps.setString(1, m.catalog());
        ps.setString(2, m.schema());
        ps.setString(3, m.table());
        setJson(ps, 4, serializer.serialize(m));
        setJson(ps, 5, toJson(m.partitionSpec()));
        setJson(ps, 6, toJson(m.sortOrder()));
        setJson(ps, 7, toJson(m.properties()));
        ps.setTimestamp(8, toTimestamp(m.createdAt()));
        ps.setTimestamp(9, toTimestamp(m.updatedAt()));
        ps.setTimestamp(10, Timestamp.from(Instant.now()));
    }

    private void bindSaveCatalog(PreparedStatement ps, CatalogDescriptor d) throws SQLException {
        ps.setString(1, d.name());
        ps.setString(2, d.type().name());
        ps.setString(3, d.provider());
        ps.setString(4, d.state().name());
        setJson(ps, 5, toJson(d.properties()));
        ps.setString(6, d.comment());
        setJson(ps, 7, toJson(d.allowedTenants()));
        ps.setString(8, d.incarnationId());
        ps.setLong(9, d.revision());
    }

    private void bindSyncHistory(PreparedStatement ps, SyncResult r) throws SQLException {
        ps.setString(1, r.catalogName());
        ps.setString(2, r.syncType());
        ps.setTimestamp(3, Timestamp.from(r.syncTime()));
        ps.setInt(4, r.changesApplied());
        ps.setInt(5, r.errors());
        setJson(ps, 6, toJson(r.errorMessages()));
        ps.setLong(7, r.durationMs());
    }

    // --- Row mapping helpers ---

    private TableMetadata mapTableMetadata(ResultSet rs, String catalog, String schema, String table)
            throws SQLException {
        String columnsJson = rs.getString("columns");
        TableMetadata full = serializer.deserialize(columnsJson);
        return new TableMetadata(catalog, schema, table, full.columns(),
                full.partitionSpec(), full.sortOrder(), full.properties(),
                toInstant(rs.getTimestamp("table_created_at")),
                toInstant(rs.getTimestamp("table_updated_at")));
    }

    private List<QualifiedTableRef> mapTableRefs(ResultSet rs) throws SQLException {
        List<QualifiedTableRef> refs = new ArrayList<>();
        while (rs.next()) {
            refs.add(new QualifiedTableRef(rs.getString(1), rs.getString(2), rs.getString(3)));
        }
        return refs;
    }

    private CatalogDescriptor mapCatalogDescriptor(ResultSet rs) throws SQLException {
        return new CatalogDescriptor(
                rs.getString("name"),
                CatalogType.valueOf(rs.getString("type")),
                rs.getString("provider"),
                CatalogState.valueOf(rs.getString("state")),
                fromJsonMap(rs.getString("properties")),
                rs.getString("comment"),
                fromJsonSet(rs.getString("allowed_tenants")),
                rs.getString("incarnation_id"),
                rs.getLong("revision"));
    }

    private List<CatalogDescriptor> mapCatalogDescriptors(ResultSet rs) throws SQLException {
        List<CatalogDescriptor> list = new ArrayList<>();
        while (rs.next()) {
            list.add(mapCatalogDescriptor(rs));
        }
        return list;
    }

    // --- JSON conversion helpers ---

    private String toJson(Map<String, String> value) {
        return serializeToJson(value);
    }

    private String toJson(Set<String> value) {
        return serializeToJson(value);
    }

    private String toJson(List<String> value) {
        return serializeToJson(value);
    }

    /**
     * Internal JSON serialization via Jackson.
     * Accepts Iterable/Map — the only types we serialize to JDBC columns.
     */
    private <T> String serializeToJson(T value) {
        try {
            return MetadataJsonMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON serialization failed", e);
        }
    }

    private Map<String, String> fromJsonMap(String json) {
        if (json == null) return Map.of();
        try {
            return MetadataJsonMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }

    private Set<String> fromJsonSet(String json) {
        if (json == null) return Set.of();
        try {
            return MetadataJsonMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }

    // --- Timestamp helpers ---

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }


}
