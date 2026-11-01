package org.pangrova.metadata.source.jdbc;

import org.pangrova.spi.connector.metadata.SourceCleanupBatch;
import org.pangrova.spi.connector.metadata.SourceObjectPage;
import org.pangrova.spi.connector.metadata.SourcePublicationReservation;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Fenced bounded cleanup of SDK objects, retaining every uncertain invocation and referenced object. */
final class JdbcSourceObjectCleanup {
    private JdbcSourceObjectCleanup() {}

    static Optional<SourceCleanupBatch> reserve(Connection connection, SourcePublicationReservation reservation,
        boolean accepted) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT state FROM managed_source_prepare_invocations "
            + "WHERE fingerprint=? ORDER BY invocation_id FOR UPDATE")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    if (!result.getString(1).equals("DRAINED")) return Optional.empty();
                }
            }
        }
        // An aborted partial/unknown inventory cannot prove ownership of every object; retain it.
        try (var statement = connection
            .prepareStatement("SELECT object_count,registered_count FROM managed_source_publications WHERE "
            + "fingerprint=?")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                if (!result.next() || result.getObject("object_count") == null
                    || result.getLong(1) != result.getLong(2)) return Optional.empty();
            }
        }
        try (var statement = connection.prepareStatement("SELECT cleanup_id FROM managed_source_cleanup_batches "
            + "WHERE fingerprint=? AND state='DELETING' ORDER BY cleanup_id LIMIT 1 FOR UPDATE")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                if (result.next()) return Optional.of(batch(connection, reservation, result.getString(1)));
            }
        }
        List<SourceObjectPage.ObjectReference> objects = new ArrayList<>();
        try (var statement = connection
            .prepareStatement("SELECT o.relative_path,o.size_bytes,o.purpose FROM managed_source_objects o "
            + "WHERE o.fingerprint=? AND o.cleanup_id IS NULL AND o.deleted_at IS NULL "
            + (accepted ? "AND o.purpose='WINNER_SIDECAR' " : "")
            + "AND NOT EXISTS (SELECT 1 FROM managed_source_objects other JOIN managed_source_publications p "
            + "ON p.fingerprint=other.fingerprint WHERE other.relative_path=o.relative_path AND p.dataset_uri=? "
            + "AND p.fingerprint<>o.fingerprint AND p.state<>'ABORTED' AND other.deleted_at IS NULL) "
            + "ORDER BY o.relative_path COLLATE \"C\" LIMIT 512 FOR UPDATE OF o")) {
            statement.setString(1, reservation.fingerprint());
            statement.setString(2, reservation.base().datasetUri());
            try (var result = statement.executeQuery()) {
                while (result.next()) objects.add(new SourceObjectPage.ObjectReference(result.getString(1), result
                    .getLong(2),
                    SourceObjectPage.Purpose.valueOf(result.getString(3))));
            }
        }
        if (objects.isEmpty()) return Optional.empty();
        var batch = new SourceCleanupBatch(reservation.executionId(), reservation.fingerprint(),
            UUID.randomUUID().toString().replace("-", ""), reservation.base().datasetUri(), objects,
            SourceObjectPage.digest(objects));
        try (var statement = connection.prepareStatement("INSERT INTO managed_source_cleanup_batches "
            + "VALUES(?,?,?,?,'DELETING',clock_timestamp(),NULL)")) {
            statement.setString(1, batch.cleanupId());
            statement.setString(2, batch.fingerprint());
            statement.setBytes(3, batch.sha256());
            statement.setInt(4, batch.objects().size());
            statement.executeUpdate();
        }
        try (var statement = connection.prepareStatement("UPDATE managed_source_objects SET cleanup_id=? "
            + "WHERE fingerprint=? AND relative_path=? AND cleanup_id IS NULL AND deleted_at IS NULL")) {
            for (var object : batch.objects()) {
                statement.setString(1, batch.cleanupId());
                statement.setString(2, batch.fingerprint());
                statement.setString(3, object.relativePath());
                if (statement.executeUpdate() != 1) {
                    throw new IllegalStateException("Object cleanup lost its exact ownership");
                }
            }
        }
        return Optional.of(batch);
    }

    static void complete(Connection connection, SourcePublicationReservation reservation,
        SourceCleanupBatch expected) throws SQLException {
        var actual = batch(connection, reservation, expected.cleanupId());
        if (!actual.executionId().equals(expected.executionId()) || !actual.fingerprint().equals(expected.fingerprint())
            || !actual.datasetUri().equals(expected.datasetUri()) || !actual.objects().equals(expected.objects())
            || !Arrays.equals(actual.sha256(), expected.sha256())) {
            throw new IllegalArgumentException("Object deletion receipt differs from its authoritative reservation");
        }
        try (var statement = connection
            .prepareStatement("UPDATE managed_source_objects SET deleted_at=COALESCE(deleted_at,clock_timestamp()) "
            + "WHERE fingerprint=? AND cleanup_id=?")) {
            statement.setString(1, reservation.fingerprint());
            statement.setString(2, expected.cleanupId());
            if (statement.executeUpdate() != expected.objects().size()) {
                throw new IllegalStateException("Object deletion receipt lost its membership");
            }
        }
        try (var statement = connection.prepareStatement("UPDATE managed_source_cleanup_batches "
            + "SET state='DELETED',deleted_at=COALESCE(deleted_at,clock_timestamp()) WHERE cleanup_id=? "
            + "AND fingerprint=?")) {
            statement.setString(1, expected.cleanupId());
            statement.setString(2, reservation.fingerprint());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Object deletion receipt lost its reserved batch");
            }
        }
    }

    private static SourceCleanupBatch batch(Connection connection, SourcePublicationReservation reservation,
        String id) throws SQLException {
        List<SourceObjectPage.ObjectReference> objects = new ArrayList<>();
        byte[] digest;
        int count;
        try (var statement = connection
            .prepareStatement("SELECT objects_sha256,object_count FROM managed_source_cleanup_batches "
            + "WHERE cleanup_id=? AND fingerprint=? FOR UPDATE")) {
            statement.setString(1, id);
            statement.setString(2, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalArgumentException("Unknown authoritative cleanup batch");
                digest = result.getBytes(1);
                count = result.getInt(2);
            }
        }
        try (var statement = connection
            .prepareStatement("SELECT relative_path,size_bytes,purpose FROM managed_source_objects "
            + "WHERE fingerprint=? AND cleanup_id=? ORDER BY relative_path COLLATE \"C\" FOR UPDATE")) {
            statement.setString(1, reservation.fingerprint());
            statement.setString(2, id);
            try (var result = statement.executeQuery()) {
                while (result.next()) objects.add(new SourceObjectPage.ObjectReference(result.getString(1), result
                    .getLong(2),
                    SourceObjectPage.Purpose.valueOf(result.getString(3))));
            }
        }
        if (objects.size() != count) throw new IllegalStateException("Cleanup batch membership changed");
        return new SourceCleanupBatch(reservation.executionId(), reservation.fingerprint(), id, reservation.base()
            .datasetUri(), objects, digest);
    }
}
