package org.pangrova.metadata.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.serde.TableMetadataSerializer;
import org.pangrova.metadata.sync.SyncResult;

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

/**
 * JDBC-backed MetadataStore for PostgreSQL persistence.
 */
public class JdbcMetadataStore implements MetadataStore {

    private static final int SEARCH_LIMIT = 100;

    private final DataSource dataSource;
    private final TableMetadataSerializer serializer;
    private final ObjectMapper mapper;

    public JdbcMetadataStore(DataSource dataSource) {
        this.dataSource = dataSource;
        this.serializer = new TableMetadataSerializer();
        this.mapper = MetadataJsonMapper.instance();
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
        executeUpdate(sql, ps -> bindSaveTable(ps, metadata));
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
        executeUpdate(sql, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        });
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
                + "comment, allowed_tenants) VALUES (?,?,?,?,?,?,?) "
                + "ON CONFLICT (name) DO UPDATE SET type=EXCLUDED.type, provider=EXCLUDED.provider, "
                + "state=EXCLUDED.state, properties=EXCLUDED.properties, "
                + "comment=EXCLUDED.comment, allowed_tenants=EXCLUDED.allowed_tenants";
        executeUpdate(sql, ps -> bindSaveCatalog(ps, descriptor));
    }

    @Override
    public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants "
                + "FROM catalog_definitions WHERE name=?";
        return executeQuery(sql, ps -> ps.setString(1, name),
                rs -> rs.next() ? Optional.of(mapCatalogDescriptor(rs)) : Optional.empty());
    }

    @Override
    public void deleteCatalogDescriptor(String name) {
        executeUpdate("DELETE FROM catalog_definitions WHERE name=?", ps -> ps.setString(1, name));
    }

    @Override
    public List<CatalogDescriptor> listAllCatalogDescriptors() {
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants "
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
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (String schema : schemas) {
                ps.setString(1, catalogName);
                ps.setString(2, schema);
                ps.setTimestamp(3, now);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new MetastoreException("Batch saveSchemas failed", e);
        }
    }

    // --- SQL execution helpers ---

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
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException("Update failed: " + sql, e);
        }
    }

    // --- Bind helpers ---

    private void bindSaveTable(PreparedStatement ps, TableMetadata m) throws SQLException {
        ps.setString(1, m.catalog());
        ps.setString(2, m.schema());
        ps.setString(3, m.table());
        ps.setString(4, serializer.serialize(m));
        ps.setString(5, toJson(m.partitionSpec()));
        ps.setString(6, toJson(m.sortOrder()));
        ps.setString(7, toJson(m.properties()));
        ps.setTimestamp(8, toTimestamp(m.createdAt()));
        ps.setTimestamp(9, toTimestamp(m.updatedAt()));
        ps.setTimestamp(10, Timestamp.from(Instant.now()));
    }

    private void bindSaveCatalog(PreparedStatement ps, CatalogDescriptor d) throws SQLException {
        ps.setString(1, d.name());
        ps.setString(2, d.type().name());
        ps.setString(3, d.provider());
        ps.setString(4, d.state().name());
        ps.setString(5, toJson(d.properties()));
        ps.setString(6, d.comment());
        ps.setString(7, toJson(d.allowedTenants()));
    }

    private void bindSyncHistory(PreparedStatement ps, SyncResult r) throws SQLException {
        ps.setString(1, r.catalogName());
        ps.setString(2, r.syncType());
        ps.setTimestamp(3, Timestamp.from(r.syncTime()));
        ps.setInt(4, r.changesApplied());
        ps.setInt(5, r.errors());
        ps.setString(6, toJson(r.errorMessages()));
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
                fromJsonSet(rs.getString("allowed_tenants")));
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
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON serialization failed", e);
        }
    }

    private Map<String, String> fromJsonMap(String json) {
        if (json == null) return Map.of();
        try {
            return mapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }

    private Set<String> fromJsonSet(String json) {
        if (json == null) return Set.of();
        try {
            return mapper.readValue(json, new TypeReference<>() { });
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
