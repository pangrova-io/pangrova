package org.pangrova.persistence.indexing.postgresql.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/**
 * Handles JDBC ↔ domain object mapping for index metadata.
 *
 * <p>Encapsulates PreparedStatement binding, ResultSet extraction,
 * and JSON serialization for {@link IndexDescriptor} and {@link IndexFileMapping}.</p>
 */
public final class IndexMetadataMapper {

    private static final TypeReference<List<String>> LIST_STRING_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, String>> MAP_STRING_TYPE = new TypeReference<>() {};

    // PreparedStatement parameter indices beyond the single-digit range.
    private static final int DESCRIPTOR_GENERATION_PARAM = 11;
    private static final int DESCRIPTOR_BUILD_SNAPSHOT_ID_PARAM = 12;
    private static final int DESCRIPTOR_LAST_INDEXED_SNAPSHOT_ID_PARAM = 13;
    private static final int DESCRIPTOR_CREATED_AT_PARAM = 14;
    private static final int DESCRIPTOR_UPDATED_AT_PARAM = 15;
    private static final int DESCRIPTOR_TENANT_ID_PARAM = 16;
    private static final int MAPPING_CREATED_AT_PARAM = 11;

    private IndexMetadataMapper() {}

    // --- Bind helpers ---

    /** Binds an {@link IndexDescriptor} to statement parameters 1–16. */
    public static void bindDescriptor(PreparedStatement ps, IndexDescriptor d) throws SQLException {
        ps.setString(1, d.indexId());
        ps.setString(2, d.indexName());
        ps.setString(3, d.catalogName());
        ps.setString(4, d.schemaName());
        ps.setString(5, d.tableName());
        setJson(ps, 6, toJson(d.columnNames()));
        ps.setString(7, d.indexType().name());
        setJson(ps, 8, toJsonMap(d.properties()));
        ps.setString(9, d.state().name());
        ps.setString(10, d.staleReason());
        ps.setInt(DESCRIPTOR_GENERATION_PARAM, d.generation());
        ps.setString(DESCRIPTOR_BUILD_SNAPSHOT_ID_PARAM, d.buildSnapshotId());
        ps.setString(DESCRIPTOR_LAST_INDEXED_SNAPSHOT_ID_PARAM, d.lastIndexedSnapshotId());
        setInstant(ps, DESCRIPTOR_CREATED_AT_PARAM, d.createdAt());
        setInstant(ps, DESCRIPTOR_UPDATED_AT_PARAM, d.updatedAt());
        ps.setString(DESCRIPTOR_TENANT_ID_PARAM, d.tenantId());
    }

    /** Binds an {@link IndexFileMapping} to statement parameters 1–11. */
    public static void bindMapping(PreparedStatement ps, IndexFileMapping m) throws SQLException {
        ps.setString(1, m.indexId());
        ps.setString(2, m.dataFilePath());
        ps.setString(3, m.indexFilePath());
        ps.setLong(4, m.rowCount());
        ps.setLong(5, m.indexSizeBytes());
        ps.setString(6, m.checksum());
        ps.setInt(7, m.generation());
        String token = m.snapshotToken();
        if (token == null || token.isEmpty()) {
            ps.setString(8, "");
        } else {
            ps.setString(8, token);
        }
        ps.setLong(9, m.sequenceNum());
        ps.setLong(10, m.lastVerifiedAt());
        ps.setTimestamp(MAPPING_CREATED_AT_PARAM, Timestamp.from(m.createdAt()));
    }

    // --- Map helpers ---

    /** Maps the current row of the result set to an {@link IndexDescriptor}. */
    public static IndexDescriptor mapDescriptor(ResultSet rs) throws SQLException {
        String buildSnapshotId = readOptionalColumn(rs, "build_snapshot_id");
        Map<String, String> properties = new HashMap<>(fromJsonMap(rs.getString("properties")));
        String schemaFingerprint = readOptionalColumn(rs, "source_schema_fingerprint");
        if (schemaFingerprint != null && !schemaFingerprint.isBlank()) {
            properties.put(IndexDescriptor.SOURCE_SCHEMA_FINGERPRINT_PROPERTY, schemaFingerprint);
        }
        return new IndexDescriptor(
                rs.getString("index_id"),
                rs.getString("index_name"),
                rs.getString("catalog_name"),
                rs.getString("schema_name"),
                rs.getString("table_name"),
                fromJsonList(rs.getString("column_names")),
                IndexType.valueOf(rs.getString("index_type")),
                properties,
                IndexState.valueOf(rs.getString("state")),
                rs.getString("stale_reason"),
                rs.getInt("generation"),
                buildSnapshotId,
                rs.getString("last_indexed_snapshot_id"),
                getInstant(rs, "created_at"),
                getInstant(rs, "updated_at"),
                rs.getString("tenant_id"));
    }

    private static String readOptionalColumn(ResultSet rs, String columnName) throws SQLException {
        java.sql.ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (columnName.equalsIgnoreCase(meta.getColumnName(i))) {
                return rs.getString(i);
            }
        }
        return null;
    }

    /** Maps all remaining rows of the result set to a list of {@link IndexDescriptor}s. */
    public static List<IndexDescriptor> mapDescriptorList(ResultSet rs) throws SQLException {
        List<IndexDescriptor> list = new ArrayList<>();
        while (rs.next()) {
            list.add(mapDescriptor(rs));
        }
        return list;
    }

    /** Maps the current row of the result set to an {@link IndexFileMapping}. */
    public static IndexFileMapping mapMapping(ResultSet rs) throws SQLException {
        return new IndexFileMapping(
                rs.getString("index_id"),
                rs.getString("data_file_path"),
                rs.getString("index_file_path"),
                rs.getLong("row_count"),
                rs.getLong("index_size_bytes"),
                rs.getString("checksum"),
                rs.getInt("generation"),
                rs.getString("snapshot_token"),
                rs.getLong("sequence_num"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getLong("last_verified_at"));
    }

    /** Maps all remaining rows of the result set to a list of {@link IndexFileMapping}s. */
    public static List<IndexFileMapping> mapMappingList(ResultSet rs) throws SQLException {
        List<IndexFileMapping> list = new ArrayList<>();
        while (rs.next()) {
            list.add(mapMapping(rs));
        }
        return list;
    }

    // --- JSON serialization ---

    /** Serializes a string list to JSON, returning an empty array for null/blank input. */
    public static String toJson(List<String> value) {
        try {
            return CanonicalJsonMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON serialization failed", e);
        }
    }

    /** Serializes a string map to JSON, returning an empty object for null/blank input. */
    public static String toJsonMap(Map<String, String> value) {
        try {
            return CanonicalJsonMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON serialization failed", e);
        }
    }

    /** Binds canonical PostgreSQL JSON/JSONB text without relying on implicit VARCHAR casts. */
    public static void setJson(PreparedStatement statement, int index, String json)
            throws SQLException {
        statement.setObject(index, json, Types.OTHER);
    }

    /** Deserializes a JSON array into a string list; null or blank input yields an empty list. */
    public static List<String> fromJsonList(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return CanonicalJsonMapper.readValue(json, LIST_STRING_TYPE);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }

    /** Deserializes a JSON object into a string map; null or blank input yields an empty map. */
    public static Map<String, String> fromJsonMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return CanonicalJsonMapper.readValue(json, MAP_STRING_TYPE);
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }

}
