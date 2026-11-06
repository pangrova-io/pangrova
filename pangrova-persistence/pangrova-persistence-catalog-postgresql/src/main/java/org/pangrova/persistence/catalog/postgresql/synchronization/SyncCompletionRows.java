package org.pangrova.persistence.catalog.postgresql.synchronization;

import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.metadata.codec.json.MetadataJsonMapper;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Immutable completion request encoding shared by the intent and receipt tables; borrowed connection. */
final class SyncCompletionRows {
    private static final int ERROR_MESSAGES_PARAMETER = 11;
    private static final int DURATION_PARAMETER = 12;
    private static final String INSERT_COLUMNS = "(run_id,catalog_name,incarnation_id,revision,expected_checkpoint,"
            + "next_checkpoint,sync_type,sync_time,changes_applied,errors,error_messages,duration_ms) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?::jsonb,?)";
    private static final String INSERT_INTENT = "INSERT INTO metadata_sync_intents" + INSERT_COLUMNS;
    private static final String INSERT_RECEIPT = "INSERT INTO metadata_sync_runs" + INSERT_COLUMNS;
    private static final String SELECT_INTENT = "SELECT * FROM metadata_sync_intents WHERE run_id=?";
    private static final String SELECT_RECEIPT = "SELECT * FROM metadata_sync_runs WHERE run_id=?";

    enum Relation { INTENT, RECEIPT }

    private SyncCompletionRows() {}

    static void insert(Connection connection, MetadataSyncCompletion completion, Relation relation)
            throws SQLException {
        String sql = switch (relation) {
            case INTENT -> INSERT_INTENT;
            case RECEIPT -> INSERT_RECEIPT;
        };
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, completion.runId());
            statement.setString(2, completion.scope().catalogName());
            statement.setString(3, completion.scope().incarnationId());
            statement.setLong(4, completion.scope().revision());
            statement.setString(5, completion.expectedCheckpoint().map(Instant::toString).orElse(null));
            statement.setString(6, completion.nextCheckpoint().map(Instant::toString).orElse(null));
            SyncResult result = completion.result();
            statement.setString(7, result.syncType());
            statement.setString(8, result.syncTime().toString());
            statement.setInt(9, result.changesApplied());
            statement.setInt(10, result.errors());
            try {
                statement.setString(ERROR_MESSAGES_PARAMETER,
                        MetadataJsonMapper.writeValueAsString(result.errorMessages()));
            } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
                throw new MetastoreException("Cannot encode metadata sync diagnostics", failure);
            }
            statement.setLong(DURATION_PARAMETER, result.durationMs());
            statement.executeUpdate();
        }
    }

    static Optional<MetadataSyncCompletion> read(Connection connection, UUID runId, Relation relation)
            throws SQLException {
        String sql = switch (relation) {
            case INTENT -> SELECT_INTENT;
            case RECEIPT -> SELECT_RECEIPT;
        };
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, runId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(decode(rows));
            }
        }
    }

    static MetadataSyncCompletion decode(ResultSet rows) throws SQLException {
        var scope = new CatalogDeploymentIdentity(rows.getString("catalog_name"),
                rows.getString("incarnation_id"), rows.getLong("revision"));
        List<String> errors;
        try {
            errors = MetadataJsonMapper.readValue(rows.getString("error_messages"), new TypeReference<>() {});
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new MetastoreException("Cannot decode metadata sync diagnostics", failure);
        }
        var result = new SyncResult(scope.catalogName(), rows.getString("sync_type"),
                Instant.parse(rows.getString("sync_time")), rows.getInt("changes_applied"),
                rows.getInt("errors"), errors, rows.getLong("duration_ms"));
        return new MetadataSyncCompletion(rows.getObject("run_id", UUID.class), scope,
                Optional.ofNullable(rows.getString("expected_checkpoint")).map(Instant::parse),
                Optional.ofNullable(rows.getString("next_checkpoint")).map(Instant::parse), result);
    }
}
