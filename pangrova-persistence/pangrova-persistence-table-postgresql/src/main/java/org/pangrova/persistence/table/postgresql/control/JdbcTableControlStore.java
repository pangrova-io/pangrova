package org.pangrova.persistence.table.postgresql.control;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.rollbackAfterFailure;

import org.pangrova.common.json.CanonicalJsonMapper;
import org.pangrova.table.api.identity.TableRef;
import org.pangrova.table.api.exception.TableControlException;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlProvenance;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.rename.TableControlRenameIntent;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.ownership.TableControlStore;
import org.pangrova.table.api.ownership.TableReadOwnerStore;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.observation.TableVersionObservation;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.persistence.jdbc.operation.JdbcOperations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.setJson;

/**
 * JDBC-backed control store. Ownership decisions and maintenance admission must
 * agree across coordinators, so any deployment that enables enforcement uses
 * this store rather than
 * the Persistence Memory table control provider.
 */
@ThreadSafe
public final class JdbcTableControlStore implements TableControlStore, TableReadOwnerStore {

    private static final String COLUMNS = "table_id, catalog_name, schema_name, table_name, "
            + "format, format_table_uuid, format_location, ownership_mode, state, provenance, "
            + "catalog_incarnation_id, catalog_revision, metadata_revision, version_token, "
            + "version_observed_at, owner_principal, properties, source_signature, "
            + "created_at, updated_at, last_synced_at, last_maintained_at";

    private static final String INSERT = "INSERT INTO table_control_registry (" + COLUMNS + ") "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String UPSERT = INSERT
            + " ON CONFLICT (table_id) DO UPDATE SET "
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
            + "WHERE state <> 'DROPPED' AND table_id IN (SELECT table_id "
            + "FROM table_control_name_reservation WHERE catalog_name=? AND schema_name=? AND table_name=?)";

    private static final String FIND_BY_ID = "SELECT " + COLUMNS + " FROM table_control_registry "
            + "WHERE table_id=?";

    private static final String OBSERVE_READ_OWNER = """
            SELECT control.*, catalog.incarnation_id AS observed_catalog_incarnation,
                catalog.revision AS observed_catalog_revision
            FROM (
            """ + FIND_LIVE + """
            ) control
            LEFT JOIN catalog_definitions catalog ON catalog.name=control.catalog_name
            """;

    private static final String MATCH_READ_OWNER = """
            SELECT 1 FROM table_control_name_reservation reservation
            JOIN table_control_registry control ON control.table_id=reservation.table_id
                AND control.catalog_name=reservation.catalog_name
                AND control.schema_name=reservation.schema_name
                AND control.table_name=reservation.table_name
            JOIN catalog_definitions catalog ON catalog.name=control.catalog_name
                AND catalog.incarnation_id=control.catalog_incarnation_id
                AND catalog.revision=control.catalog_revision
            WHERE reservation.catalog_name=? AND reservation.schema_name=? AND reservation.table_name=?
                AND control.table_id=? AND control.state='ACTIVE'
                AND control.catalog_incarnation_id=? AND control.catalog_revision=?
                AND control.format=? AND control.format_table_uuid IS NOT DISTINCT FROM ?
                AND control.format_location IS NOT DISTINCT FROM ?
            """;

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
    public Optional<ReadOwnerObservation> observeReadOwner(String catalog, String schema, String table) {
        return query(OBSERVE_READ_OWNER, ps -> {
            ps.setString(1, catalog);
            ps.setString(2, schema);
            ps.setString(3, table);
        }, rs -> {
            if (!rs.next()) return Optional.empty();
            String incarnation = rs.getString("observed_catalog_incarnation");
            Optional<CatalogDeploymentIdentity> deployment = incarnation == null ? Optional.empty()
                    : Optional.of(new CatalogDeploymentIdentity(
                            incarnation, rs.getLong("observed_catalog_revision")));
            return Optional.of(new ReadOwnerObservation(map(rs), deployment));
        });
    }

    @Override
    public boolean matchesCurrentReadOwner(TableControlRecord expected) {
        return query(MATCH_READ_OWNER, ps -> {
            ps.setString(1, expected.catalog());
            ps.setString(2, expected.schema());
            ps.setString(3, expected.table());
            ps.setObject(4, expected.tableId());
            ps.setString(5, expected.catalogIncarnationId());
            ps.setObject(6, expected.catalogRevision(), Types.BIGINT);
            ps.setString(7, expected.format());
            ps.setString(8, expected.formatTableUuid());
            ps.setString(9, expected.formatLocation());
        }, ResultSet::next);
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
        saveTransaction(record, Optional.empty());
    }

    @Override
    public void saveWithObservation(TableControlRecord record, TableVersionObservation observation) {
        if (!record.tableId().equals(observation.tableId())
                || !java.util.Objects.equals(record.versionToken(), observation.versionToken())) {
            throw new IllegalArgumentException("Observation must describe the saved table and version");
        }
        saveTransaction(record, Optional.of(observation));
    }

    private void saveTransaction(TableControlRecord record,
            Optional<TableVersionObservation> observation) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                writeRevision(conn, UPSERT, record, observation);
                conn.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(conn, failure);
                throw failure;
            }
        } catch (SQLException e) {
            throw new TableControlException(
                    "Failed to save table control record: " + record.qualifiedName(), e);
        }
    }

    private TableControlRecord lockRegistration(Connection connection, TableControlRecord expected)
            throws SQLException {
        try (var statement = connection.prepareStatement(FIND_BY_ID + " FOR UPDATE")) {
            statement.setObject(1, expected.tableId());
            try (var rows = statement.executeQuery()) {
                TableControlRecord current = rows.next() ? map(rows) : null;
                if (!expected.matchesRegistration(current)) {
                    throw TableControlException.staleWrite("Table registration changed for "
                            + expected.qualifiedName());
                }
                return current;
            }
        }
    }

    @Override
    public void replaceRegistration(TableControlRecord expected, TableControlRecord replacement, Instant retiredAt) {
        expected.requireReplacement(replacement);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                TableControlRecord current = lockRegistration(connection, expected);
                writeRevision(connection, UPSERT, current.retiredAt(retiredAt), Optional.empty());
                writeRevision(connection, INSERT, replacement, Optional.empty());
                connection.commit();
            } catch (SQLException | RuntimeException | Error failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new TableControlException("Failed to replace table registration: " + expected.qualifiedName(),
                    failure);
        }
    }

    private void writeRevision(Connection conn, String mutation, TableControlRecord record,
            Optional<TableVersionObservation> observation) throws SQLException {
        int written = executeUpdate(conn, mutation, ps -> bind(ps, record));
        if (written == 0) {
            throw TableControlException.staleWrite("Stale table control write for "
                    + record.qualifiedName() + " at revision "
                    + record.metadataRevision()
                    + ": another coordinator advanced the record");
        }
        var names = reservedNames(conn, record.tableId());
        var reserved = new java.util.LinkedHashSet<TableRef>();
        if (record.state() != TableControlState.DROPPED) {
            reserved.add(record.tableRef());
            TableControlRenameIntent.source(record).ifPresent(reserved::add);
        }
        if (!names.equals(reserved)) {
            replaceReservations(conn, record.tableId(), reserved);
        }
        names.addAll(reserved);
        names.add(record.tableRef());
        for (var name : names) {
            executeUpdate(conn, INSERT_INVALIDATION, ps -> {
                ps.setString(1, name.catalog() + CACHE_KEY_SEPARATOR + name.schema()
                        + CACHE_KEY_SEPARATOR + name.table());
                ps.setString(2, "TABLE");
            });
        }
        if (observation.isPresent()) {
            executeUpdate(conn, INSERT_OBSERVATION, ps -> bindObservation(ps, observation.get()));
        }
    }

    private static void replaceReservations(Connection connection, UUID tableId,
            java.util.Set<TableRef> names) throws SQLException {
        executeUpdate(connection, "DELETE FROM table_control_name_reservation WHERE table_id=?",
                ps -> ps.setObject(1, tableId));
        for (var name : names) {
            executeUpdate(connection, "INSERT INTO table_control_name_reservation "
                    + "(catalog_name,schema_name,table_name,table_id) VALUES (?,?,?,?)", ps -> {
                        ps.setString(1, name.catalog());
                        ps.setString(2, name.schema());
                        ps.setString(3, name.table());
                        ps.setObject(4, tableId);
                    });
        }
    }

    private static java.util.Set<TableRef> reservedNames(
            Connection connection, UUID tableId) throws SQLException {
        var names = new java.util.LinkedHashSet<TableRef>();
        try (var query = connection.prepareStatement("SELECT catalog_name, schema_name, table_name "
                + "FROM table_control_name_reservation WHERE table_id=?")) {
            query.setObject(1, tableId);
            try (var rows = query.executeQuery()) {
                while (rows.next()) names.add(new TableRef(
                        rows.getString(1), rows.getString(2), rows.getString(3)));
            }
        }
        return names;
    }

    private static void bindObservation(PreparedStatement ps, TableVersionObservation observation)
            throws SQLException {
        ps.setObject(1, observation.tableId());
        ps.setString(2, observation.versionToken());
        ps.setString(3, observation.previousVersionToken());
        ps.setString(4, observation.source());
        JdbcOperations.setInstant(ps, 5, observation.observedAt());
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
                        JdbcOperations.getInstant(rs, "observed_at")));
            }
            return observations;
        });
    }

    private static int executeUpdate(Connection conn, String sql,
            JdbcOperations.SqlConsumer binder) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            return ps.executeUpdate();
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
        JdbcOperations.setNullableInstant(ps, 15, record.versionObservedAt());
        ps.setString(16, record.ownerPrincipal());
        setJson(ps, 17, toJson(record.properties()));
        ps.setString(18, record.sourceSignature());
        JdbcOperations.setInstant(ps, 19, record.createdAt());
        JdbcOperations.setInstant(ps, 20, record.updatedAt());
        JdbcOperations.setNullableInstant(ps, 21, record.lastSyncedAt());
        JdbcOperations.setNullableInstant(ps, 22, record.lastMaintainedAt());
    }

    private static String toJson(Map<String, String> properties) {
        try {
            return CanonicalJsonMapper
                    .writeValueAsString(properties);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to serialize table control properties", e);
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
                .version(rs.getString("version_token"), JdbcOperations.getInstant(rs, "version_observed_at"))
                .ownerPrincipal(rs.getString("owner_principal"))
                .properties(fromJson(rs.getString("properties")))
                .sourceSignature(rs.getString("source_signature"))
                .createdAt(JdbcOperations.getInstant(rs, "created_at"))
                .updatedAt(JdbcOperations.getInstant(rs, "updated_at"))
                .lastSyncedAt(JdbcOperations.getInstant(rs, "last_synced_at"))
                .lastMaintainedAt(JdbcOperations.getInstant(rs, "last_maintained_at"))
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
            return CanonicalJsonMapper.readValue(
                    json, new com.fasterxml.jackson.core.type.TypeReference<>() { });
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new TableControlException("Failed to deserialize table control properties", e);
        }
    }

    private List<TableControlRecord> mapAll(ResultSet rs) throws SQLException {
        List<TableControlRecord> records = new ArrayList<>();
        while (rs.next()) {
            records.add(map(rs));
        }
        return records;
    }

    private <T> T query(String sql, JdbcOperations.SqlConsumer binder,
            JdbcOperations.SqlFunction<ResultSet, T> extractor) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return extractor.apply(rs);
            }
        } catch (SQLException e) {
            throw new TableControlException("Table control query failed: " + sql, e);
        }
    }
}
