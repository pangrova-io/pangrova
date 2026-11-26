package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.indexing.api.enums.SchemaChangeType;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord;
import org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaTransition;
import org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.setJson;
import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.toJson;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setNullableInstant;

/** PostgreSQL write-ahead journal and atomic descriptor fencing for schema evolution. */
final class JdbcIndexSchemaEvolutionStore {
    private static final int OLD_COLUMN_TYPE_PARAM = 11;
    private static final int NEW_COLUMN_TYPE_PARAM = 12;
    private static final int STATE_PARAM = 13;
    private static final int CREATED_AT_PARAM = 14;
    private static final int UPDATED_AT_PARAM = 15;
    private static final int APPLIED_AT_PARAM = 16;
    private static final int NEW_SCHEMA_NAME_PARAM = 17;

    private static final String RECORD_COLUMNS = "operation_id,index_id,tenant_id,catalog_name,schema_name,table_name,"
            + "event_type,old_table_name,old_column_name,new_column_name,old_column_type,new_column_type,"
            + "new_schema_name,state,created_at,updated_at,source_identity_format,source_identity_uuid,"
            + "source_identity_location,source_identity_schema,target_identity_format,target_identity_uuid,"
            + "target_identity_location,target_identity_schema";
    private final DataSource dataSource;

    JdbcIndexSchemaEvolutionStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    UUID prepare(SchemaChangeEvent event, Instant now) {
        return prepare(event, Optional.empty(), now);
    }

    UUID prepare(SchemaChangeEvent event, Optional<TableIdentity> sourceIdentity, Instant now) {
        UUID operationId = UUID.randomUUID();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<TableScope> scopes = scopes(event);
                for (TableScope scope : scopes) {
                    lockScope(connection, scope);
                    requireUnclaimed(connection, scope, null);
                }
                insert(connection, operationId, null, event, IndexSchemaEvolutionState.INTENDED, now, null);
                updateIdentity(connection, operationId, sourceIdentity, true);
                for (TableScope scope : scopes) {
                    claimScope(connection, scope, operationId);
                    fenceTable(connection, event, scope, operationId, now);
                }
                connection.commit();
                return operationId;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            // A lost commit response can leave a durable INTENDED operation,
            // but this method has not returned its ID and the connector call
            // cannot have started. Resolve only that exact pre-call operation.
            try {
                markAborted(operationId, Instant.now());
            } catch (RuntimeException recoveryFailure) {
                failure.addSuppressed(recoveryFailure);
            }
            throw new IllegalStateException("Failed to prepare index schema evolution " + operationId, failure);
        }
    }

    void markCommitted(UUID operationId, Instant now) {
        markCommitted(operationId, Optional.empty(), now);
    }

    void markCommitted(UUID operationId, Optional<TableIdentity> targetIdentity, Instant now) {
        updateState(operationId, IndexSchemaEvolutionState.COMMITTED, targetIdentity, now);
    }

    void markUnknown(UUID operationId, Instant now) {
        updateState(operationId, IndexSchemaEvolutionState.UNKNOWN, Optional.empty(), now);
    }

    void markAborted(UUID operationId, Instant now) {
        updateState(operationId, IndexSchemaEvolutionState.ABORTED, Optional.empty(), now);
    }

    Optional<IndexSchemaEvolutionRecord> find(UUID operationId) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT " + RECORD_COLUMNS + " FROM index_schema_evolution_outbox WHERE operation_id=?")) {
            statement.setObject(1, operationId);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(map(rows)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to inspect index schema evolution", failure);
        }
    }

    List<IndexDescriptor> targets(UUID operationId) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT d.* FROM index_schema_operation_targets t JOIN index_descriptors d "
                                + "ON d.index_id=t.index_id AND d.updated_at=t.fenced_at "
                                + "WHERE t.operation_id=? AND d.state='STALE' ORDER BY d.index_id")) {
            statement.setObject(1, operationId);
            try (ResultSet rows = statement.executeQuery()) {
                List<IndexDescriptor> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(IndexMetadataMapper.mapDescriptor(rows));
                }
                return List.copyOf(result);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to inspect index schema operation targets", failure);
        }
    }

    List<IndexSchemaEvolutionRecord> listPending(int limit) {
        return listByState(limit, true);
    }

    List<IndexSchemaEvolutionRecord> listUnresolved(int limit) {
        return listByState(limit, false);
    }

    void deferRetry(UUID operationId, Instant now) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "UPDATE index_schema_evolution_outbox SET updated_at=? "
                                + "WHERE operation_id=? AND state='COMMITTED'")) {
            setInstant(statement, 1, now);
            statement.setObject(2, operationId);
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to defer schema operation retry", failure);
        }
    }

    private List<IndexSchemaEvolutionRecord> listByState(int limit, boolean runnable) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT " + RECORD_COLUMNS
                                + " FROM index_schema_evolution_outbox WHERE "
                                + (runnable ? "state='COMMITTED' ORDER BY updated_at,operation_id"
                                        : "state IN ('INTENDED','UNKNOWN') ORDER BY created_at,operation_id")
                                + " LIMIT ?")) {
            statement.setInt(1, limit);
            try (ResultSet rows = statement.executeQuery()) {
                List<IndexSchemaEvolutionRecord> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(map(rows));
                }
                return List.copyOf(result);
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to list pending index schema evolution", failure);
        }
    }

    void apply(UUID operationId, List<IndexSchemaTransition> transitions, Instant now) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                List<TableScope> claims = lockOperationScopes(connection, operationId);
                IndexSchemaEvolutionState state = lockOperation(connection, operationId);
                if (state != IndexSchemaEvolutionState.COMMITTED) {
                    connection.rollback();
                    return;
                }
                for (TableScope scope : claims) {
                    requireUnclaimed(connection, scope, operationId);
                }
                for (IndexSchemaTransition transition : transitions) {
                    applyTransition(connection, operationId, transition, now);
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE index_schema_evolution_outbox SET state='APPLIED', applied_at=?, updated_at=? "
                                + "WHERE operation_id=? AND state='COMMITTED'")) {
                    setInstant(statement, 1, now);
                    setInstant(statement, 2, now);
                    statement.setObject(3, operationId);
                    statement.executeUpdate();
                }
                releaseClaims(connection, operationId);
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to apply index schema evolution", failure);
        }
    }

    Optional<UUID> fenceMismatch(IndexDescriptor descriptor, String reason, Instant now) {
        UUID operationId = UUID.randomUUID();
        SchemaChangeEvent event = new SchemaChangeEvent(
                SchemaChangeType.SCHEMA_MISMATCH, descriptor.tenantId(), descriptor.catalogName(),
                descriptor.schemaName(), descriptor.tableName(), null, null, null, null, null, null);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                TableScope scope = sourceScope(event);
                lockScope(connection, scope);
                if (otherClaim(connection, scope, null).isPresent()) {
                    connection.rollback();
                    return Optional.empty();
                }
                // Observed drift is not a connector mutation. Record only the
                // terminal STALE fence; there is no READY restoration to replay.
                insert(connection, operationId, descriptor.indexId(), event,
                        IndexSchemaEvolutionState.APPLIED, now, now);
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE index_descriptors SET state='STALE', stale_reason=?, updated_at=? "
                                + "WHERE index_id=? AND state=? AND generation=? AND updated_at=?")) {
                    statement.setString(1, reason);
                    setInstant(statement, 2, now);
                    statement.setString(3, descriptor.indexId());
                    statement.setString(4, descriptor.state().name());
                    statement.setInt(5, descriptor.generation());
                    setInstant(statement, 6, descriptor.updatedAt());
                    if (statement.executeUpdate() != 1) {
                        connection.rollback();
                        return Optional.empty();
                    }
                }
                fenceStagingBuild(connection, descriptor.indexId(), now);
                insertCacheEvent(connection, descriptor, now);
                connection.commit();
                return Optional.of(operationId);
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to fence index schema mismatch", failure);
        }
    }

    private void updateState(UUID operationId, IndexSchemaEvolutionState state,
                             Optional<TableIdentity> identity, Instant now) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (state == IndexSchemaEvolutionState.ABORTED) {
                    lockOperationScopes(connection, operationId);
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE index_schema_evolution_outbox SET state=?, updated_at=? "
                                + "WHERE operation_id=? AND state='INTENDED'")) {
                    statement.setString(1, state.name());
                    setInstant(statement, 2, now);
                    statement.setObject(3, operationId);
                    if (statement.executeUpdate() == 1) {
                        if (state == IndexSchemaEvolutionState.COMMITTED) {
                            updateIdentity(connection, operationId, identity, false);
                        } else if (state == IndexSchemaEvolutionState.ABORTED) {
                            releaseClaims(connection, operationId);
                        }
                    }
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to update index schema evolution state", failure);
        }
    }

    private record TableScope(String catalog, String schema, String table) {
        private static final Comparator<TableScope> ORDER = Comparator.comparing(TableScope::catalog)
                .thenComparing(TableScope::schema).thenComparing(TableScope::table);
    }

    private static TableScope sourceScope(SchemaChangeEvent event) {
        return new TableScope(event.catalogName(), event.schemaName(),
                event.type() == SchemaChangeType.RENAME_TABLE ? event.oldTableName() : event.tableName());
    }

    private static List<TableScope> scopes(SchemaChangeEvent event) {
        TableScope source = sourceScope(event);
        if (event.type() != SchemaChangeType.RENAME_TABLE) {
            return List.of(source);
        }
        var target = new TableScope(event.catalogName(),
                event.newSchemaName() == null ? event.schemaName() : event.newSchemaName(), event.tableName());
        return java.util.stream.Stream.of(source, target).distinct().sorted(TableScope.ORDER).toList();
    }

    private static void bindScope(PreparedStatement statement, TableScope scope) throws SQLException {
        statement.setString(1, scope.catalog());
        statement.setString(2, scope.schema());
        statement.setString(3, scope.table());
    }

    private static void lockScope(Connection connection, TableScope scope) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pangrova_lock_index_schema_scope(?,?,?)")) {
            bindScope(statement, scope);
            statement.executeQuery().close();
        }
    }

    private static Optional<UUID> otherClaim(Connection connection, TableScope scope, UUID own) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT operation_id FROM index_schema_operation_claims WHERE catalog_name=? AND schema_name=? "
                        + "AND table_name=? AND (?::uuid IS NULL OR operation_id<>?::uuid) "
                        + "ORDER BY operation_id LIMIT 1")) {
            bindScope(statement, scope);
            statement.setObject(4, own);
            statement.setObject(5, own);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? Optional.of(row.getObject(1, UUID.class)) : Optional.empty();
            }
        }
    }

    private static void requireUnclaimed(Connection connection, TableScope scope, UUID own) throws SQLException {
        Optional<UUID> blocker = otherClaim(connection, scope, own);
        if (blocker.isPresent()) {
            throw new IllegalStateException("Unresolved index schema operation " + blocker.orElseThrow()
                    + " owns " + scope);
        }
    }

    private static void claimScope(Connection connection, TableScope scope, UUID operationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO index_schema_operation_claims (catalog_name,schema_name,table_name,operation_id) "
                        + "VALUES (?,?,?,?)")) {
            bindScope(statement, scope);
            statement.setObject(4, operationId);
            statement.executeUpdate();
        }
    }

    private static List<TableScope> lockOperationScopes(Connection connection, UUID operationId) throws SQLException {
        List<TableScope> claims = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT catalog_name,schema_name,table_name FROM index_schema_operation_claims WHERE operation_id=?")) {
            statement.setObject(1, operationId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    claims.add(new TableScope(rows.getString(1), rows.getString(2), rows.getString(3)));
                }
            }
        }
        claims.sort(TableScope.ORDER);
        for (TableScope scope : claims) {
            lockScope(connection, scope);
        }
        return List.copyOf(claims);
    }

    private static void releaseClaims(Connection connection, UUID operationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM index_schema_operation_claims WHERE operation_id=?")) {
            statement.setObject(1, operationId);
            statement.executeUpdate();
        }
    }

    private record FenceTarget(String indexId, String tenantId) {}

    private static void fenceTable(Connection connection, SchemaChangeEvent event, TableScope scope,
                                   UUID operationId, Instant now) throws SQLException {
        String sql = "SELECT index_id,tenant_id FROM index_descriptors WHERE catalog_name=? "
                + "AND schema_name=? AND table_name=? AND state<>'DROPPED' ORDER BY index_id FOR UPDATE";
        List<FenceTarget> targets = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindScope(statement, scope);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    targets.add(new FenceTarget(rows.getString("index_id"), rows.getString("tenant_id")));
                }
            }
        }
        for (FenceTarget target : targets) {
            boolean captured = scope.equals(sourceScope(event)) && target.tenantId().equals(event.tenantId());
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE index_descriptors SET state='STALE', stale_reason=?, updated_at=? WHERE index_id=?")) {
                statement.setString(1, captured ? "schema_change_pending" : "schema_change_peer_scope");
                setInstant(statement, 2, now);
                statement.setString(3, target.indexId());
                statement.executeUpdate();
            }
            fenceStagingBuild(connection, target.indexId(), now);
            if (captured) {
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO index_schema_operation_targets (operation_id,index_id,fenced_at) "
                                + "VALUES (?,?,?)")) {
                    statement.setObject(1, operationId);
                    statement.setString(2, target.indexId());
                    setInstant(statement, 3, now);
                    statement.executeUpdate();
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO index_cache_events (event_type,index_id,catalog_name,schema_name,"
                            + "table_name,created_at) VALUES ('SCHEMA_CHANGE',?,?,?,?,?)")) {
                statement.setString(1, target.indexId());
                statement.setString(2, scope.catalog());
                statement.setString(3, scope.schema());
                statement.setString(4, scope.table());
                setInstant(statement, 5, now);
                statement.executeUpdate();
            }
        }
    }

    private static void fenceStagingBuild(Connection connection, String indexId, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_generation_builds SET state='FAILED',failure_reason='schema_change_pending',updated_at=? "
                        + "WHERE index_id=? AND state='BUILDING'")) {
            setInstant(statement, 1, now);
            statement.setString(2, indexId);
            statement.executeUpdate();
        }
    }

    private static void updateIdentity(Connection connection, UUID operationId,
                                       Optional<TableIdentity> identity, boolean source) throws SQLException {
        if (identity.isEmpty()) {
            return;
        }
        String prefix = source ? "source_identity_" : "target_identity_";
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_schema_evolution_outbox SET " + prefix + "format=?," + prefix + "uuid=?,"
                        + prefix + "location=?," + prefix + "schema=? WHERE operation_id=?")) {
            TableIdentity value = identity.orElseThrow();
            statement.setString(1, value.format());
            statement.setString(2, value.formatTableUuid());
            statement.setString(3, value.location());
            statement.setString(4, value.schemaFingerprint());
            statement.setObject(5, operationId);
            statement.executeUpdate();
        }
    }

    private static IndexSchemaEvolutionState lockOperation(
            Connection connection, UUID operationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state FROM index_schema_evolution_outbox WHERE operation_id=? FOR UPDATE")) {
            statement.setObject(1, operationId);
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()) {
                    throw new IllegalStateException("Index schema evolution operation is absent");
                }
                return IndexSchemaEvolutionState.valueOf(row.getString("state"));
            }
        }
    }

    private static void applyTransition(
            Connection connection, UUID operationId, IndexSchemaTransition transition, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT d.state,d.generation,d.updated_at FROM index_descriptors d "
                        + "JOIN index_schema_operation_targets t ON t.index_id=d.index_id "
                        + "AND t.fenced_at=d.updated_at "
                        + "WHERE t.operation_id=? AND d.index_id=? FOR UPDATE OF d")) {
            statement.setObject(1, operationId);
            statement.setString(2, transition.indexId());
            try (ResultSet row = statement.executeQuery()) {
                if (!row.next()
                        || !transition.expectedState().name().equals(row.getString("state"))
                        || row.getInt("generation") != transition.expectedGeneration()
                        || !transition.expectedUpdatedAt().equals(getInstant(row, "updated_at"))) {
                    return;
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_descriptors SET state=?, stale_reason=?, "
                        + "table_name=COALESCE(?,table_name), column_names=COALESCE(?,column_names), "
                        + "source_schema_fingerprint=COALESCE(?,source_schema_fingerprint), "
                        + "schema_name=COALESCE(?,schema_name), updated_at=? WHERE index_id=?")) {
            statement.setString(1, transition.newState().name());
            statement.setString(2, transition.staleReason());
            statement.setString(3, transition.newTableName());
            if (transition.newColumnNames() == null) {
                statement.setNull(4, java.sql.Types.OTHER);
            } else {
                setJson(statement, 4, toJson(transition.newColumnNames()));
            }
            statement.setString(5, transition.sourceSchemaFingerprint());
            statement.setString(6, transition.newSchemaName());
            setInstant(statement, 7, now);
            statement.setString(8, transition.indexId());
            statement.executeUpdate();
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO index_cache_events (event_type,index_id,catalog_name,schema_name,table_name,created_at) "
                        + "VALUES ('SCHEMA_CHANGE',?,?,?,?,?)")) {
            statement.setString(1, transition.indexId());
            statement.setString(2, transition.catalogName());
            statement.setString(3, transition.newSchemaName());
            statement.setString(4, transition.newTableName());
            setInstant(statement, 5, now);
            statement.executeUpdate();
        }
    }

    private static void insertCacheEvent(Connection connection, IndexDescriptor descriptor, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO index_cache_events (event_type,index_id,catalog_name,schema_name,table_name,created_at) "
                        + "VALUES ('SCHEMA_CHANGE',?,?,?,?,?)")) {
            statement.setString(1, descriptor.indexId());
            statement.setString(2, descriptor.catalogName());
            statement.setString(3, descriptor.schemaName());
            statement.setString(4, descriptor.tableName());
            setInstant(statement, 5, now);
            statement.executeUpdate();
        }
    }

    private static void insert(
            Connection connection, UUID operationId, String indexId, SchemaChangeEvent event,
            IndexSchemaEvolutionState state, Instant now, Instant appliedAt) throws SQLException {
        String sql = "INSERT INTO index_schema_evolution_outbox (operation_id,index_id,tenant_id,"
                + "catalog_name,schema_name,table_name,event_type,old_table_name,old_column_name,"
                + "new_column_name,old_column_type,new_column_type,state,created_at,updated_at,applied_at,"
                + "new_schema_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, operationId);
            statement.setString(2, indexId);
            statement.setString(3, event.tenantId());
            statement.setString(4, event.catalogName());
            statement.setString(5, event.schemaName());
            statement.setString(6, event.tableName());
            statement.setString(7, event.type().name());
            statement.setString(8, event.oldTableName());
            statement.setString(9, event.oldColumnName());
            statement.setString(10, event.newColumnName());
            statement.setString(OLD_COLUMN_TYPE_PARAM, event.oldColumnType());
            statement.setString(NEW_COLUMN_TYPE_PARAM, event.newColumnType());
            statement.setString(STATE_PARAM, state.name());
            setInstant(statement, CREATED_AT_PARAM, now);
            setInstant(statement, UPDATED_AT_PARAM, now);
            setNullableInstant(statement, APPLIED_AT_PARAM, appliedAt);
            statement.setString(NEW_SCHEMA_NAME_PARAM, event.newSchemaName());
            statement.executeUpdate();
        }
    }

    private static IndexSchemaEvolutionRecord map(ResultSet row) throws SQLException {
        SchemaChangeEvent event = new SchemaChangeEvent(
                SchemaChangeType.valueOf(row.getString("event_type")),
                row.getString("tenant_id"), row.getString("catalog_name"),
                row.getString("schema_name"), row.getString("table_name"),
                row.getString("old_table_name"), row.getString("old_column_name"),
                row.getString("new_column_name"), row.getString("old_column_type"),
                row.getString("new_column_type"), row.getString("new_schema_name"));
        return new IndexSchemaEvolutionRecord(
                row.getObject("operation_id", UUID.class), row.getString("index_id"), event,
                IndexSchemaEvolutionState.valueOf(row.getString("state")), getInstant(row, "created_at"),
                getInstant(row, "updated_at"), readIdentity(row, "source_identity_"),
                readIdentity(row, "target_identity_"));
    }
    private static Optional<TableIdentity> readIdentity(ResultSet row, String prefix) throws SQLException {
        String format = row.getString(prefix + "format");
        return format == null ? Optional.empty() : Optional.of(new TableIdentity(format,
                row.getString(prefix + "uuid"), row.getString(prefix + "location"), row.getString(prefix + "schema")));
    }
}
