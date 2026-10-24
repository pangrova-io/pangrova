package org.pangrova.metadata.control;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.store.JdbcSupport;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.metadata.store.JdbcSupport.setJson;

/**
 * JDBC-backed control store. Ownership decisions and maintenance admission must
 * agree across coordinators, so any deployment that enables enforcement uses
 * this store rather than {@link InMemoryTableControlStore}.
 */
@ThreadSafe
public final class JdbcTableControlStore implements TableControlStore {

    private static final String COLUMNS = "table_id, catalog_name, schema_name, table_name, "
            + "format, format_table_uuid, format_location, ownership_mode, state, provenance, "
            + "catalog_incarnation_id, catalog_revision, metadata_revision, version_token, "
            + "version_observed_at, owner_principal, properties, source_signature, "
            + "created_at, updated_at, last_synced_at, last_maintained_at";

    private static final String UPSERT = "INSERT INTO table_control_registry (" + COLUMNS + ") "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            + "ON CONFLICT (table_id) DO UPDATE SET "
            + "catalog_name=EXCLUDED.catalog_name, schema_name=EXCLUDED.schema_name, "
            + "table_name=EXCLUDED.table_name, format=EXCLUDED.format, "
            + "format_table_uuid=EXCLUDED.format_table_uuid, "
            + "format_location=EXCLUDED.format_location, "
            + "ownership_mode=EXCLUDED.ownership_mode, state=EXCLUDED.state, "
            + "provenance=EXCLUDED.provenance, "
            + "catalog_incarnation_id=EXCLUDED.catalog_incarnation_id, "
            + "catalog_revision=EXCLUDED.catalog_revision, "
            + "metadata_revision=EXCLUDED.metadata_revision, "
            + "version_token=EXCLUDED.version_token, "
            + "version_observed_at=EXCLUDED.version_observed_at, "
            + "owner_principal=EXCLUDED.owner_principal, properties=EXCLUDED.properties, "
            + "source_signature=EXCLUDED.source_signature, updated_at=EXCLUDED.updated_at, "
            + "last_synced_at=EXCLUDED.last_synced_at, "
            + "last_maintained_at=EXCLUDED.last_maintained_at "
            // Monotonic guard: a writer that read an older revision must not be
            // able to undo a newer state (for example revert QUARANTINED to ACTIVE).
            + "WHERE table_control_registry.metadata_revision < EXCLUDED.metadata_revision";

    private static final String FIND_LIVE = "SELECT " + COLUMNS + " FROM table_control_registry "
            + "WHERE catalog_name=? AND schema_name=? AND table_name=? "
            + "AND state <> 'DROPPED'";

    private static final String FIND_BY_ID = "SELECT " + COLUMNS + " FROM table_control_registry "
            + "WHERE table_id=?";

    private static final String LIST_BY_CATALOG = "SELECT " + COLUMNS
            + " FROM table_control_registry WHERE catalog_name=? "
            + "ORDER BY schema_name, table_name";

    private static final String LIST_ALL = "SELECT " + COLUMNS
            + " FROM table_control_registry ORDER BY catalog_name, schema_name, table_name";

    private static final String INSERT_INVALIDATION =
            "INSERT INTO invalidation_log (cache_key, invalidation_type, created_at, updated_at) "
                    + "VALUES (?, ?, NOW(), NOW())";

    private static final String INSERT_OBSERVATION =
            "INSERT INTO table_control_version_observation "
                    + "(table_id, version_token, previous_version_token, source, observed_at) "
                    + "VALUES (?,?,?,?,?)";

    private static final String LIST_OBSERVATIONS =
            "SELECT table_id, version_token, previous_version_token, source, observed_at "
                    + "FROM table_control_version_observation WHERE table_id=? "
                    + "ORDER BY observation_id DESC LIMIT ?";

    private static final String CACHE_KEY_SEPARATOR = "\u001F";

    private final DataSource dataSource;

    public JdbcTableControlStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Optional<TableControlRecord> find(String catalog, String schema, String table) {
        return query(FIND_LIVE, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        }, rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty());
    }

    @Override
    public Optional<TableControlRecord> findById(UUID tableId) {
        return query(FIND_BY_ID, ps -> ps.setObject(1, tableId),
                rs -> rs.next() ? Optional.of(map(rs)) : Optional.empty());
    }

    @Override
    public List<TableControlRecord> listByCatalog(String catalog) {
        return query(LIST_BY_CATALOG, ps -> ps.setString(1, catalog), this::mapAll);
    }

    @Override
    public List<TableControlRecord> listAll() {
        return query(LIST_ALL, ps -> { }, this::mapAll);
    }

    /**
     * Writes the control record and its cache invalidation in one transaction.
     * Peers only learn about an ownership or version change after both commit, so
     * a reader can never observe a new revision with a stale cached resolution.
     */
    @Override
    public void save(TableControlRecord record) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                int written = executeUpdate(conn, UPSERT, ps -> bind(ps, record));
                if (written == 0) {
                    throw new MetastoreException("Stale table control write for "
                            + record.qualifiedName() + " at revision "
                            + record.metadataRevision()
                            + ": another coordinator advanced the record");
                }
                executeUpdate(conn, INSERT_INVALIDATION, ps -> {
                    ps.setString(1, cacheKey(record));
                    ps.setString(2, "TABLE");
                });
                conn.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollback(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to save table control record: " + record.qualifiedName(), e);
        }
    }

    @Override
    public void recordVersionObservation(TableVersionObservation observation) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_OBSERVATION)) {
            ps.setObject(1, observation.tableId());
            ps.setString(2, observation.versionToken());
            ps.setString(3, observation.previousVersionToken());
            ps.setString(4, observation.source());
            JdbcSupport.setInstant(ps, 5, observation.observedAt());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new MetastoreException(
                    "Failed to record version observation for tableId="
                            + observation.tableId(), e);
        }
    }

    @Override
    public List<TableVersionObservation> listVersionObservations(UUID tableId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        return query(LIST_OBSERVATIONS, ps -> {
            ps.setObject(1, tableId);
            ps.setInt(2, limit);
        }, rs -> {
            List<TableVersionObservation> observations = new ArrayList<>();
            while (rs.next()) {
                observations.add(new TableVersionObservation(
                        rs.getObject("table_id", UUID.class),
                        rs.getString("version_token"),
                        rs.getString("previous_version_token"),
                        rs.getString("source"),
                        JdbcSupport.getInstant(rs, "observed_at")));
            }
            return observations;
        });
    }

    private static String cacheKey(TableControlRecord record) {
        return record.catalog() + CACHE_KEY_SEPARATOR + record.schema()
                + CACHE_KEY_SEPARATOR + record.table();
    }

    private static int executeUpdate(Connection conn, String sql,
            JdbcSupport.SqlConsumer binder) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            return ps.executeUpdate();
        }
    }

    private static void rollback(Connection conn, Throwable failure) {
        try {
            conn.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void bind(PreparedStatement ps, TableControlRecord record) throws SQLException {
        ps.setObject(1, record.tableId());
        ps.setString(2, record.catalog());
        ps.setString(3, record.schema());
        ps.setString(4, record.table());
        ps.setString(5, record.format());
        ps.setString(6, record.formatTableUuid());
        ps.setString(7, record.formatLocation());
        ps.setString(8, record.ownershipMode().name());
        ps.setString(9, record.state().name());
        ps.setString(10, record.provenance().name());
        ps.setString(11, record.catalogIncarnationId());
        if (record.catalogRevision() == null) {
            ps.setNull(12, Types.BIGINT);
        } else {
            ps.setLong(12, record.catalogRevision());
        }
        ps.setLong(13, record.metadataRevision());
        ps.setString(14, record.versionToken());
        JdbcSupport.setNullableInstant(ps, 15, record.versionObservedAt());
        ps.setString(16, record.ownerPrincipal());
        setJson(ps, 17, toJson(record.properties()));
        ps.setString(18, record.sourceSignature());
        JdbcSupport.setInstant(ps, 19, record.createdAt());
        JdbcSupport.setInstant(ps, 20, record.updatedAt());
        JdbcSupport.setNullableInstant(ps, 21, record.lastSyncedAt());
        JdbcSupport.setNullableInstant(ps, 22, record.lastMaintainedAt());
    }

    private static String toJson(Map<String, String> properties) {
        try {
            return org.pangrova.metadata.store.MetadataJsonMapper
                    .writeValueAsString(properties);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new MetastoreException("Failed to serialize table control properties", e);
        }
    }

    private TableControlRecord map(ResultSet rs) throws SQLException {
        return TableControlRecord.builder()
                .tableId(rs.getObject("table_id", UUID.class))
                .name(rs.getString("catalog_name"), rs.getString("schema_name"),
                        rs.getString("table_name"))
                .format(rs.getString("format"))
                .formatTableUuid(rs.getString("format_table_uuid"))
                .formatLocation(rs.getString("format_location"))
                .ownershipMode(OwnershipMode.valueOf(rs.getString("ownership_mode")))
                .state(TableControlState.valueOf(rs.getString("state")))
                .provenance(TableControlProvenance.valueOf(rs.getString("provenance")))
                .catalogIncarnationId(rs.getString("catalog_incarnation_id"))
                .catalogRevision(nullableLong(rs, "catalog_revision"))
                .metadataRevision(rs.getLong("metadata_revision"))
                .version(rs.getString("version_token"), JdbcSupport.getInstant(rs, "version_observed_at"))
                .ownerPrincipal(rs.getString("owner_principal"))
                .properties(fromJson(rs.getString("properties")))
                .sourceSignature(rs.getString("source_signature"))
                .createdAt(JdbcSupport.getInstant(rs, "created_at"))
                .updatedAt(JdbcSupport.getInstant(rs, "updated_at"))
                .lastSyncedAt(JdbcSupport.getInstant(rs, "last_synced_at"))
                .lastMaintainedAt(JdbcSupport.getInstant(rs, "last_maintained_at"))
                .build();
    }

    private static Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private static Map<String, String> fromJson(String json) {
        if (json == null) {
            return Map.of();
        }
        try {
            return org.pangrova.metadata.store.MetadataJsonMapper.readValue(
                    json, new com.fasterxml.jackson.core.type.TypeReference<>() { });
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new MetastoreException("Failed to deserialize table control properties", e);
        }
    }

    private List<TableControlRecord> mapAll(ResultSet rs) throws SQLException {
        List<TableControlRecord> records = new ArrayList<>();
        while (rs.next()) {
            records.add(map(rs));
        }
        return records;
    }

    private <T> T query(String sql, JdbcSupport.SqlConsumer binder,
            JdbcSupport.SqlFunction<ResultSet, T> extractor) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return extractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new MetastoreException("Table control query failed: " + sql, e);
        }
    }
}
