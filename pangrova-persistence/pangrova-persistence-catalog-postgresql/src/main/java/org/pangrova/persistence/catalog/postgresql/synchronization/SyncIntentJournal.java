package org.pangrova.persistence.catalog.postgresql.synchronization;

import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncCompletion;
import org.pangrova.persistence.catalog.api.synchronization.MetadataSyncConflictException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

/** Intent state transitions under the deployment lock; never commits or closes the borrowed connection. */
final class SyncIntentJournal {
    private SyncIntentJournal() {}

    static Optional<MetadataSyncCompletion> pending(Connection connection, String catalog) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM metadata_sync_intents WHERE catalog_name=? AND state='PENDING'")) {
            statement.setString(1, catalog);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(SyncCompletionRows.decode(rows)) : Optional.empty();
            }
        }
    }

    static void requirePending(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state FROM metadata_sync_intents WHERE run_id=?")) {
            statement.setObject(1, id);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !rows.getString(1).equals("PENDING")) {
                    throw new MetadataSyncConflictException("Metadata sync intent is not pending");
                }
            }
        }
    }

    static void finish(Connection connection, UUID id, String state) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE metadata_sync_intents SET state=? WHERE run_id=? AND state='PENDING'")) {
            statement.setString(1, state);
            statement.setObject(2, id);
            if (statement.executeUpdate() != 1) {
                throw new MetadataSyncConflictException("Metadata sync intent is not pending");
            }
        }
    }
}
