package org.pangrova.persistence.indexing.postgresql.metadata;

import org.pangrova.indexing.api.descriptor.IndexDescriptor;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.setJson;
import static org.pangrova.persistence.indexing.postgresql.metadata.IndexMetadataMapper.toJson;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
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

    private final DataSource dataSource;

    JdbcIndexSchemaEvolutionStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    UUID prepare(SchemaChangeEvent event, Instant now) {
        UUID operationId = UUID.randomUUID();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                insert(connection, operationId, null, event, IndexSchemaEvolutionState.INTENDED, now, null);
                fenceTable(connection, event, now);
                connection.commit();
                return operationId;
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to prepare index schema evolution", failure);
        }
    }

    void markCommitted(UUID operationId, Instant now) {
        updateState(operationId, IndexSchemaEvolutionState.COMMITTED, now);
    }

    void markAborted(UUID operationId, Instant now) {
        updateState(operationId, IndexSchemaEvolutionState.ABORTED, now);
    }

    List<IndexSchemaEvolutionRecord> listPending(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT operation_id,index_id,tenant_id,catalog_name,schema_name,table_name,"
                                + "event_type,old_table_name,old_column_name,new_column_name,"
                                + "old_column_type,new_column_type,new_schema_name,state,created_at,updated_at "
                                + "FROM index_schema_evolution_outbox WHERE state IN ('INTENDED','COMMITTED') "
                                + "ORDER BY created_at,operation_id LIMIT ?")) {
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
                IndexSchemaEvolutionState state = lockOperation(connection, operationId);
                if (state != IndexSchemaEvolutionState.COMMITTED) {
                    connection.rollback();
                    return;
                }
                for (IndexSchemaTransition transition : transitions) {
                    applyTransition(connection, transition, now);
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE index_schema_evolution_outbox SET state='APPLIED', applied_at=?, updated_at=? "
                                + "WHERE operation_id=? AND state='COMMITTED'")) {
                    setInstant(statement, 1, now);
                    setInstant(statement, 2, now);
                    statement.setObject(3, operationId);
                    statement.executeUpdate();
                }
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
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
                insert(connection, operationId, descriptor.indexId(), event,
                        IndexSchemaEvolutionState.COMMITTED, now, null);
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
                insertCacheEvent(connection, descriptor, now);
                connection.commit();
                return Optional.of(operationId);
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to fence index schema mismatch", failure);
        }
    }

    private void updateState(UUID operationId, IndexSchemaEvolutionState state, Instant now) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE index_schema_evolution_outbox SET state=?, updated_at=? "
                            + "WHERE operation_id=? AND state='INTENDED'")) {
                statement.setString(1, state.name());
                setInstant(statement, 2, now);
                statement.setObject(3, operationId);
                statement.executeUpdate();
                connection.commit();
            } catch (SQLException | RuntimeException failure) {
                connection.rollback();
                throw failure;
            }
        } catch (SQLException failure) {
            throw new IllegalStateException("Failed to update index schema evolution state", failure);
        }
    }

    private static void fenceTable(Connection connection, SchemaChangeEvent event, Instant now)
            throws SQLException {
        String table = SchemaChangeType.RENAME_TABLE.equals(event.type())
                ? event.oldTableName() : event.tableName();
        String sql = "SELECT index_id FROM index_descriptors WHERE tenant_id=? AND catalog_name=? "
                + "AND schema_name=? AND table_name=? AND state<>'DROPPED' FOR UPDATE";
        List<String> indexIds = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, event.tenantId());
            statement.setString(2, event.catalogName());
            statement.setString(3, event.schemaName());
            statement.setString(4, table);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    indexIds.add(rows.getString("index_id"));
                }
            }
        }
        for (String indexId : indexIds) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE index_descriptors SET state='STALE', stale_reason='schema_change_pending', "
                            + "updated_at=? WHERE index_id=?")) {
                setInstant(statement, 1, now);
                statement.setString(2, indexId);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO index_cache_events (event_type,index_id,catalog_name,schema_name,"
                            + "table_name,created_at) VALUES ('SCHEMA_CHANGE',?,?,?,?,?)")) {
                statement.setString(1, indexId);
                statement.setString(2, event.catalogName());
                statement.setString(3, event.schemaName());
                statement.setString(4, table);
                setInstant(statement, 5, now);
                statement.executeUpdate();
            }
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
            Connection connection, IndexSchemaTransition transition, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state,generation,updated_at FROM index_descriptors WHERE index_id=? FOR UPDATE")) {
            statement.setString(1, transition.indexId());
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
                getInstant(row, "updated_at"));
    }
}
