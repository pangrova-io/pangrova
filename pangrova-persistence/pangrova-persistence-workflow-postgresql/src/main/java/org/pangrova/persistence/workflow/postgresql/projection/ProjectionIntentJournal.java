package org.pangrova.persistence.workflow.postgresql.projection;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionConflictException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

/** Borrowed-connection SQL only. The projection transaction owner controls locking and commit. */
final class ProjectionIntentJournal {
    private ProjectionIntentJournal() {}

    record Intent(MetadataProjectionMutation mutation, String state) {
        void requirePending(MetadataProjectionMutation requested) {
            if (!mutation.equals(requested) || !state.equals("PENDING")) {
                throw new MetadataProjectionConflictException("Projection intent content or terminal state differs");
            }
        }
    }

    static Optional<Intent> find(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT operation_id,catalog_name,incarnation_id,revision,request_json,state "
                        + "FROM metadata_projection_intents WHERE operation_id=?")) {
            statement.setObject(1, id);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(decode(rows)) : Optional.empty();
            }
        }
    }

    static Optional<Intent> pending(Connection connection, String catalog) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT operation_id,catalog_name,incarnation_id,revision,request_json,state "
                        + "FROM metadata_projection_intents WHERE catalog_name=? AND state='PENDING'")) {
            statement.setString(1, catalog);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(decode(rows)) : Optional.empty();
            }
        }
    }

    static void insert(Connection connection, MetadataProjectionMutation mutation) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO metadata_projection_intents"
                        + "(operation_id,catalog_name,incarnation_id,revision,request_json,state) "
                        + "VALUES (?,?,?,?,?::jsonb,'PENDING')")) {
            statement.setObject(1, mutation.operationId());
            statement.setString(2, mutation.deployment().catalogName());
            statement.setString(3, mutation.deployment().incarnationId());
            statement.setLong(4, mutation.deployment().revision());
            statement.setString(5, ProjectionReceiptCodec.request(mutation));
            statement.executeUpdate();
        }
    }

    static void finish(Connection connection, UUID id, String state) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE metadata_projection_intents SET state=? WHERE operation_id=? AND state='PENDING'")) {
            statement.setString(1, state);
            statement.setObject(2, id);
            if (statement.executeUpdate() != 1) {
                throw new MetadataProjectionConflictException("Projection intent is not pending");
            }
        }
    }

    private static Intent decode(ResultSet rows) throws SQLException {
        UUID id = rows.getObject(1, UUID.class);
        var scope = new CatalogDeploymentIdentity(rows.getString(2), rows.getString(3), rows.getLong(4));
        return new Intent(ProjectionReceiptCodec.decodeRequest(id, scope, rows.getString(5)), rows.getString(6));
    }
}
