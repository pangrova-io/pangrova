package org.pangrova.materialization.store.jdbc;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.port.MaterializationEvent;

import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.setInstant;
import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.setJson;
import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.setNullableInstant;

/**
 * Attempt, generation, and garbage-collection transactions of the JDBC authority.
 *
 * <p>Attempt reservation CASes the unique active-attempt index, settles
 * conditional on the observed attempt state, and generation collection is
 * re-validated under a row lock immediately before the physical drop, which
 * runs outside the transaction and marks the ledger failed on a miss.
 */
final class JdbcMaterializationAttemptLifecycle {

    private final JdbcMaterializationSql sql;
    private final JdbcMaterializationQueries queries;
    private final JdbcMaterializationReaderPins readerPins;

    /**
     * Creates the lifecycle over the shared SQL plumbing, read queries, and
     * reader-pin adapter.
     *
     * @param sql transaction plumbing shared with the owning repository
     * @param queries read queries of the owning repository
     * @param readerPins reader-pin adapter used for GC ledger outcomes
     */
    JdbcMaterializationAttemptLifecycle(JdbcMaterializationSql sql,
            JdbcMaterializationQueries queries, JdbcMaterializationReaderPins readerPins) {
        if (sql == null || queries == null || readerPins == null) {
            throw new IllegalArgumentException(
                    "sql, queries, and readerPins must not be null");
        }
        this.sql = sql;
        this.queries = queries;
        this.readerPins = readerPins;
    }

    void reserveAttempt(RefreshAttempt attempt, Generation candidate, List<SourcePin> sourcePins,
            MaterializationWriteCapability capability, long expectedObjectRevision,
            MaterializationEvent event) {
        if (attempt.baseObjectRevision() != expectedObjectRevision) {
            throw MaterializationException.concurrentPublication(
                    "attempt base revision " + attempt.baseObjectRevision()
                            + " does not match admission revision " + expectedObjectRevision);
        }
        Optional<RefreshAttempt> active = queries.findActiveAttempt(attempt.object());
        if (active.isPresent()) {
            throw MaterializationException.concurrentPublication(
                    "materialization " + attempt.object().canonicalName()
                            + " already has an active attempt " + active.orElseThrow().id().value()
                            + " in state " + active.orElseThrow().state());
        }
        JdbcMaterializationRows.requireCapability(attempt, candidate, capability);
        String sourceVectorHash = attempt.sourceVector().canonicalHash();
        if (!MessageDigest.isEqual(
                sourceVectorHash.getBytes(StandardCharsets.UTF_8),
                candidate.sourceVector().canonicalHash().getBytes(StandardCharsets.UTF_8))) {
            throw MaterializationException.evidenceMismatch(
                    "attempt and candidate carry different source vectors");
        }
        sql.inTransaction("reserveAttempt", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.attempts (attempt_id, tenant_id, object_name, "
                            + "incarnation_id, definition_revision, base_object_revision, "
                            + "base_generation_id, refresh_kind, source_vector, source_vector_hash, "
                            + "trigger_kind, requesting_principal, definer_principal, "
                            + "execution_anchor, control_epoch, lease_expires_at, candidate_catalog, "
                            + "candidate_schema, candidate_table, state, error_code, created_at, "
                            + "updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                statement.setObject(1, attempt.id().value());
                JdbcMaterializationRows.bindObjectId(statement, 2, attempt.object());
                statement.setObject(4, attempt.incarnation().value());
                statement.setLong(5, attempt.definitionRevision());
                statement.setLong(6, attempt.baseObjectRevision());
                JdbcMaterializationRows.bindNullableUuid(statement, 7,
                        attempt.baseGeneration().map(GenerationId::value).orElse(null));
                statement.setString(8, JdbcMaterializationStateCodec.refreshKindName(attempt.refreshKind()));
                setJson(statement, 9, MaterializationJson.sourceVector(attempt.sourceVector()));
                statement.setString(10, sourceVectorHash);
                statement.setString(11, attempt.trigger().name());
                statement.setString(12, attempt.requesting().name());
                statement.setString(13, attempt.definer().name());
                statement.setString(14, attempt.fence().executionAnchor());
                statement.setLong(15, attempt.fence().controlEpoch());
                setInstant(statement, 16, attempt.fence().leaseExpiresAt());
                statement.setString(17, attempt.candidateTable().catalog());
                statement.setString(18, attempt.candidateTable().schema());
                statement.setString(19, attempt.candidateTable().table());
                statement.setString(20, JdbcMaterializationStateCodec.attemptStateName(attempt.state()));
                statement.setNull(21, java.sql.Types.VARCHAR);
                setInstant(statement, 22, attempt.createdAt());
                setInstant(statement, 23, attempt.updatedAt());
                try {
                    statement.executeUpdate();
                } catch (SQLException conflict) {
                    if ("23505".equals(conflict.getSQLState())) {
                        throw MaterializationException.concurrentPublication(
                                "another attempt for " + attempt.object().canonicalName()
                                        + " became active concurrently");
                    }
                    throw conflict;
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.generations (generation_id, tenant_id, object_name, "
                            + "incarnation_id, definition_revision, producing_attempt_id, "
                            + "base_generation_id, physical_catalog, physical_schema, physical_table, "
                            + "refresh_kind, source_vector, source_vector_hash, state, created_at) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                statement.setObject(1, candidate.id().value());
                JdbcMaterializationRows.bindObjectId(statement, 2, candidate.object());
                statement.setObject(4, candidate.incarnation().value());
                statement.setLong(5, candidate.definitionRevision());
                statement.setObject(6, candidate.producingAttempt().value());
                JdbcMaterializationRows.bindNullableUuid(statement, 7,
                        candidate.baseGeneration().map(GenerationId::value).orElse(null));
                statement.setString(8, candidate.physicalTable().catalog());
                statement.setString(9, candidate.physicalTable().schema());
                statement.setString(10, candidate.physicalTable().table());
                statement.setString(11, JdbcMaterializationStateCodec.refreshKindName(candidate.refreshKind()));
                setJson(statement, 12, MaterializationJson.sourceVector(candidate.sourceVector()));
                statement.setString(13, candidate.sourceVector().canonicalHash());
                statement.setString(14, JdbcMaterializationStateCodec.generationStateName(candidate.state()));
                setInstant(statement, 15, candidate.createdAt());
                statement.executeUpdate();
            }
            for (SourcePin pin : sourcePins) {
                JdbcMaterializationWrites.insertSourcePin(connection, pin);
            }
            JdbcMaterializationWrites.insertCapability(connection, capability);
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }

    void settleAttempt(RefreshAttempt previous, RefreshAttempt attempt, Generation generation,
            MaterializationEvent event) {
        if (attempt.updatedAt().isBefore(previous.updatedAt())) {
            throw MaterializationException.requestRejected(
                    "attempt update instant must not move backwards");
        }
        sql.inTransaction("settleAttempt", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.attempts SET state=?, error_code=?, updated_at=? "
                            + "WHERE attempt_id=? AND state=?")) {
                statement.setString(1, JdbcMaterializationStateCodec.attemptStateName(attempt.state()));
                if (attempt.errorCode().isPresent()) {
                    statement.setString(2, attempt.errorCode().orElseThrow());
                } else {
                    statement.setNull(2, java.sql.Types.VARCHAR);
                }
                setInstant(statement, 3, attempt.updatedAt());
                statement.setObject(4, attempt.id().value());
                statement.setString(5, JdbcMaterializationStateCodec.attemptStateName(previous.state()));
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "attempt " + attempt.id().value() + " is no longer in state "
                                    + JdbcMaterializationStateCodec.attemptStateName(previous.state()));
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state=?, published_at=? "
                            + "WHERE generation_id=?")) {
                statement.setString(1, JdbcMaterializationStateCodec.generationStateName(generation.state()));
                setNullableInstant(statement, 2, generation.publishedAt().orElse(null));
                statement.setObject(3, generation.id().value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.requestRejected(
                            "generation " + generation.id().value() + " does not exist");
                }
            }
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }

    boolean collectGeneration(Generation generation, GcLedgerEntry entry,
            MaterializationEvent event, Runnable physicalDrop) {
        String expectedState = JdbcMaterializationStateCodec.generationStateName(generation.state());
        boolean collected = sql.inTransaction("collectGeneration", connection -> {
            String state;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT state FROM materialization.generations WHERE generation_id=? FOR UPDATE")) {
                statement.setObject(1, generation.id().value());
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return false;
                    }
                    state = result.getString("state");
                }
            }
            if (!expectedState.equals(state) || !collectableUnderLock(connection, generation)) {
                return false;
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state='COLLECTED' "
                            + "WHERE generation_id=? AND state=?")) {
                statement.setObject(1, generation.id().value());
                statement.setString(2, expectedState);
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "generation " + generation.id().value()
                                    + " changed while it was being collected");
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.gc_ledger SET state='APPLIED', updated_at=? "
                            + "WHERE entry_id=? AND state<>'APPLIED'")) {
                setInstant(statement, 1, event.occurredAt());
                statement.setObject(2, entry.entryId());
                statement.executeUpdate();
            }
            JdbcMaterializationWrites.insertEvent(connection, event);
            return true;
        });
        if (!collected) {
            return false;
        }
        // Physical deletion is an external side effect and must not run inside the
        // database transaction. A failed deletion leaves the generation COLLECTED and
        // marks the ledger FAILED so operators can retry without holding row locks.
        try {
            physicalDrop.run();
        } catch (RuntimeException failure) {
            readerPins.updateGcDecision(entry.entryId(), GcLedgerEntry.GcState.FAILED, event.occurredAt());
            throw failure;
        }
        return true;
    }

    private static boolean collectableUnderLock(Connection connection, Generation generation)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM materialization.reader_pins p "
                        + "WHERE p.generation_id=? AND p.expires_at > clock_timestamp() "
                        + "UNION ALL SELECT 1 FROM materialization.generations d "
                        + "WHERE d.base_generation_id=? "
                        + "AND d.state IN ('CANDIDATE','COMMITTED','PUBLISHED','QUARANTINED') "
                        + "UNION ALL SELECT 1 FROM materialization.objects o "
                        + "WHERE o.tenant_id=? AND o.object_name=? "
                        + "AND o.published_generation_id=? LIMIT 1")) {
            statement.setObject(1, generation.id().value());
            statement.setObject(2, generation.id().value());
            JdbcMaterializationRows.bindObjectId(statement, 3, generation.object());
            statement.setObject(5, generation.id().value());
            try (ResultSet result = statement.executeQuery()) {
                return !result.next();
            }
        }
    }

    void saveGeneration(Generation generation, MaterializationEvent event) {
        sql.inTransaction("saveGeneration", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state=?, published_at=? "
                            + "WHERE generation_id=?")) {
                statement.setString(1, JdbcMaterializationStateCodec.generationStateName(generation.state()));
                setNullableInstant(statement, 2, generation.publishedAt().orElse(null));
                statement.setObject(3, generation.id().value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.requestRejected(
                            "generation " + generation.id().value() + " does not exist");
                }
            }
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }
}
