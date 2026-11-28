package org.pangrova.persistence.materialization.postgresql;

import org.pangrova.materialization.api.enums.GcState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.GcLedgerEntry;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.ReaderId;
import org.pangrova.materialization.api.domain.ReaderPin;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.setInstant;

/**
 * Lease and reader-pin statements of the materialization repository.
 *
 * <p>Pins are the read-side fence of generation GC: a generation is collectable
 * only when no unexpired pin protects it, so these writes stay small and
 * independent of the lifecycle transactions.
 */
final class JdbcMaterializationReaderPins {

    private final DataSource dataSource;
    private final JdbcMaterializationSql sql;

    JdbcMaterializationReaderPins(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
        this.sql = new JdbcMaterializationSql(dataSource);
    }

    public void releaseSourcePins(AttemptId attempt, java.time.Instant now) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE materialization.source_pins SET state='RELEASED' "
                             + "WHERE attempt_id=? AND state='ACQUIRED'")) {
            statement.setObject(1, attempt.value());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("releaseSourcePins failed", failure);
        }
    }
    public void pinReader(ReaderPin pin) {
        pinReader(pin, null);
    }

    void pinActiveQueryReader(ReaderPin pin) {
        if (!"QUERY".equals(pin.reader().kind().name())) {
            throw new IllegalArgumentException("Query reader authority requires a QUERY pin");
        }
        pinReader(pin, pin.reader().id());
    }

    private void pinReader(ReaderPin pin, String queryId) {
        sql.inTransaction("pinReader", connection -> {
            String state = lockGenerationState(connection, pin.generation(), queryId);
            if (!"PUBLISHED".equals(state) && !"RETIRED".equals(state)
                    && !"COMMITTED".equals(state)) {
                throw MaterializationException.requestRejected(
                        "generation " + pin.generation().value() + " in state " + state
                                + " is no longer readable");
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.reader_pins (generation_id, reader_kind, reader_id, owner, "
                            + "expires_at, heartbeat_at) VALUES (?,?,?,?,?,?) "
                            + "ON CONFLICT (generation_id, reader_kind, reader_id) DO UPDATE SET "
                            + "heartbeat_at=GREATEST(materialization.reader_pins.heartbeat_at, "
                            + "EXCLUDED.heartbeat_at), "
                            + "expires_at=GREATEST(materialization.reader_pins.expires_at, "
                            + "EXCLUDED.expires_at)")) {
                statement.setObject(1, pin.generation().value());
                statement.setString(2, pin.reader().kind().name());
                statement.setString(3, pin.reader().id());
                statement.setString(4, pin.owner());
                setInstant(statement, 5, pin.expiresAt());
                setInstant(statement, 6, pin.heartbeatAt());
                statement.executeUpdate();
            }
            return null;
        });
    }

    private static String lockGenerationState(Connection connection, GenerationId generation, String queryId)
            throws SQLException {
        String sql = queryId == null
                ? "SELECT state FROM materialization.generations WHERE generation_id=? FOR SHARE"
                : """
                    WITH owner AS MATERIALIZED (
                        SELECT expires_at, recovery_drain_state FROM execution_anchor
                        WHERE execution_id=? FOR SHARE
                    )
                    SELECT g.state FROM materialization.generations g CROSS JOIN owner
                    WHERE g.generation_id=? AND owner.expires_at > clock_timestamp() AND owner.recovery_drain_state=0
                    FOR SHARE OF g
                    """;
        try (PreparedStatement lock = connection.prepareStatement(sql)) {
            if (queryId == null) {
                lock.setObject(1, generation.value());
            } else {
                lock.setString(1, queryId);
                lock.setObject(2, generation.value());
            }
            try (java.sql.ResultSet result = lock.executeQuery()) {
                if (!result.next()) {
                    throw MaterializationException.requestRejected(
                            "generation " + generation.value()
                                    + " no longer exists and cannot be pinned");
                }
                return result.getString("state");
            }
        }
    }

    public void releaseReaderPin(GenerationId generation, ReaderId reader) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM materialization.reader_pins "
                             + "WHERE generation_id=? AND reader_kind=? AND reader_id=?")) {
            statement.setObject(1, generation.value());
            statement.setString(2, reader.kind().name());
            statement.setString(3, reader.id());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("releaseReaderPin failed", failure);
        }
    }

    public void releaseReaderPins(ReaderId reader) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM materialization.reader_pins WHERE reader_kind=? AND reader_id=?")) {
            statement.setString(1, reader.kind().name());
            statement.setString(2, reader.id());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("releaseReaderPins failed", failure);
        }
    }

    public void renewReaderPins(ReaderId reader, Instant expiresAt, Instant heartbeatAt) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE materialization.reader_pins SET "
                             + "heartbeat_at=GREATEST(heartbeat_at, ?), "
                             + "expires_at=GREATEST(expires_at, ?) WHERE reader_kind=? AND reader_id=?")) {
            setInstant(statement, 1, heartbeatAt);
            setInstant(statement, 2, expiresAt);
            statement.setString(3, reader.kind().name());
            statement.setString(4, reader.id());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("renewReaderPins failed", failure);
        }
    }

    public int purgeExpiredReaderPins(Instant now) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM materialization.reader_pins WHERE expires_at<=?")) {
            setInstant(statement, 1, now);
            return statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("purgeExpiredReaderPins failed", failure);
        }
    }

    public void recordGcDecision(GcLedgerEntry entry) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO materialization.gc_ledger (entry_id, generation_id, decision, "
                             + "evidence, state, created_at, updated_at) VALUES (?,?,?,?,?,?,?)")) {
            statement.setObject(1, entry.entryId());
            statement.setObject(2, entry.generation().value());
            statement.setString(3, entry.decision().name());
            statement.setString(4, entry.evidence());
            statement.setString(5, entry.state().name());
            setInstant(statement, 6, entry.createdAt());
            setInstant(statement, 7, entry.updatedAt());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("recordGcDecision failed", failure);
        }
    }

    public void updateGcDecision(UUID entryId, GcState state, Instant now) {
        if (state == GcState.PENDING) {
            throw new IllegalArgumentException("a GC decision cannot return to pending");
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE materialization.gc_ledger SET state=?, updated_at=? "
                             + "WHERE entry_id=? AND state<>'APPLIED'")) {
            statement.setString(1, state.name());
            setInstant(statement, 2, now);
            statement.setObject(3, entryId);
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("updateGcDecision failed", failure);
        }
    }
}
