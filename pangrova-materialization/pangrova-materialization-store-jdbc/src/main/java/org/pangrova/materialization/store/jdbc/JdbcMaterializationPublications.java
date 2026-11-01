package org.pangrova.materialization.store.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationRepository;
import org.pangrova.materialization.port.PublicationResult;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.spi.connector.materialization.CommitEvidence;

import static org.pangrova.common.jdbc.JdbcSupport.canonicalInstant;
import static org.pangrova.common.jdbc.JdbcSupport.getInstant;
import static org.pangrova.common.jdbc.JdbcSupport.setInstant;

/**
 * Commit-evidence and publication authority of the materialization kernel.
 *
 * <p>Publication is one transaction: head CAS, previous-generation retirement,
 * generation and attempt transition, command receipt, dependent schedule advance,
 * and the audit event either all commit or none do.
 */
final class JdbcMaterializationPublications {

    private final JdbcMaterializationSql sql;
    private final MaterializationRepository repository;

    JdbcMaterializationPublications(JdbcMaterializationSql sql,
            MaterializationRepository repository) {
        if (sql == null || repository == null) {
            throw new IllegalArgumentException("sql and repository must not be null");
        }
        this.sql = sql;
        this.repository = repository;
    }

    public void recordCommitEvidence(AttemptId attemptId, CommitEvidence evidence, Instant now) {
        Instant recordedAt = canonicalInstant(now);
        sql.inTransaction("recordCommitEvidence", connection -> {
            String state;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT state FROM materialization.attempts WHERE attempt_id=? FOR UPDATE")) {
                statement.setObject(1, attemptId.value());
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        throw MaterializationException.requestRejected(
                                "attempt " + attemptId.value() + " does not exist");
                    }
                    state = result.getString("state");
                }
            }
            Optional<CommitEvidence> existing = findEvidence(connection, attemptId);
            if (existing.isPresent()) {
                if (!existing.orElseThrow().equals(evidence)) {
                    throw MaterializationException.evidenceMismatch(
                            "attempt " + attemptId.value() + " already carries different evidence");
                }
                if (state.equals("QUARANTINED")) {
                    moveToCommitted(connection, attemptId, recordedAt);
                }
                return null;
            }
            if (!List.of("PREPARING", "BUILDING", "QUARANTINED").contains(state)) {
                throw MaterializationException.evidenceMismatch(
                        "attempt " + attemptId.value() + " in state " + state
                                + " cannot accept commit evidence");
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.attempts SET state='BUILDING', updated_at=? "
                            + "WHERE attempt_id=? AND state='PREPARING'")) {
                setInstant(statement, 1, recordedAt);
                statement.setObject(2, attemptId.value());
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.commit_evidence (attempt_id, tenant_id, "
                            + "connector_id, commit_fingerprint, manifest_hash, format_version, "
                            + "committed_at) SELECT attempt_id, tenant_id, ?, ?, ?, ?, ? "
                            + "FROM materialization.attempts WHERE attempt_id=?")) {
                statement.setString(1, evidence.connectorId());
                statement.setString(2, evidence.commitFingerprint());
                statement.setString(3, evidence.manifestHash());
                statement.setString(4, evidence.formatVersion());
                setInstant(statement, 5, evidence.committedAt());
                statement.setObject(6, attemptId.value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.evidenceMismatch(
                            "commit evidence could not be attached to attempt " + attemptId.value());
                }
            }
            moveToCommitted(connection, attemptId, recordedAt);
            return null;
        });
    }

    public PublicationResult publish(PublishCommand command, MaterializationEvent event) {
        Optional<CommandRecord> receipt = repository.findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent() && JdbcMaterializationWrites.isPublicationReceipt(
                receipt.orElseThrow())) {
            return replayPublication(command, receipt.orElseThrow());
        }
        Instant now = canonicalInstant(event.occurredAt());
        return sql.inTransaction("publish", connection ->
                publishInTransaction(connection, command, event, now));
    }

    private PublicationResult publishInTransaction(Connection connection, PublishCommand command,
            MaterializationEvent event, Instant now) throws SQLException {
        Head head = lockHead(connection, command);
        requireLiveFence(connection, command.fence());
        requireCommittedAttempt(connection, command);
        requireCommittedGeneration(connection, command);
        MaterializationObject desired = head.current().publish(
                command.generation().id(), head.current().revision(), now);
        moveHead(connection, command, head.current().revision(), now);
        retirePreviousGeneration(connection, head.publishedGeneration(), command);
        publishGeneration(connection, command, now);
        publishAttempt(connection, command, now);
        recordPublication(connection, command, desired, event, now);
        return new PublicationResult(desired,
                JdbcMaterializationRows.publishedView(
                        command.generation(), command.evidence(), now), true);
    }

    private Head lockHead(Connection connection, PublishCommand command) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT " + JdbcMaterializationRows.OBJECT_COLUMNS + " FROM materialization.objects "
                        + "WHERE tenant_id=? AND object_name=? FOR UPDATE")) {
            JdbcMaterializationRows.bindObjectId(statement, 1, command.object());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw MaterializationException.notFound(command.object());
                }
                MaterializationObject current = JdbcMaterializationRows.mapObject(result);
                UUID publishedGeneration =
                        result.getObject("published_generation_id", UUID.class);
                requirePublicationHead(current, command);
                return new Head(current, publishedGeneration);
            }
        }
    }

    private static void requirePublicationHead(MaterializationObject current,
            PublishCommand command) {
        if (current.revision() != command.expectedObjectRevision()) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + command.expectedObjectRevision()
                            + " but found " + current.revision() + " for "
                            + command.object().canonicalName());
        }
        boolean matchingState = current.state() == ObjectState.Active.INSTANCE
                || current.state() == ObjectState.Creating.INSTANCE
                || current.state() == ObjectState.Suspended.INSTANCE
                || current.state() == ObjectState.Failed.INSTANCE;
        if (!current.incarnation().equals(command.generation().incarnation())
                || current.definitionRevision() != command.generation().definitionRevision()
                || !matchingState) {
            throw MaterializationException.invalidStateTransition(
                    "publication head does not match the candidate generation for "
                            + command.object().canonicalName());
        }
    }

    private static void moveHead(Connection connection, PublishCommand command,
            long expectedRevision, Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.objects SET revision=?, published_generation_id=?, "
                        + "state='ACTIVE', updated_at=? "
                        + "WHERE tenant_id=? AND object_name=? AND revision=?")) {
            statement.setLong(1, expectedRevision + 1);
            statement.setObject(2, command.generation().id().value());
            setInstant(statement, 3, now);
            JdbcMaterializationRows.bindObjectId(statement, 4, command.object());
            statement.setLong(6, expectedRevision);
            if (statement.executeUpdate() != 1) {
                throw MaterializationException.concurrentPublication(
                        "object head CAS lost for " + command.object().canonicalName());
            }
        }
    }

    private static void retirePreviousGeneration(Connection connection, UUID publishedGeneration,
            PublishCommand command) throws SQLException {
        if (publishedGeneration == null
                || publishedGeneration.equals(command.generation().id().value())) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.generations SET state='RETIRED' "
                        + "WHERE generation_id=? AND state='PUBLISHED'")) {
            statement.setObject(1, publishedGeneration);
            if (statement.executeUpdate() != 1) {
                throw MaterializationException.invalidStateTransition(
                        "previous head generation is not published");
            }
        }
    }

    private static void publishGeneration(Connection connection, PublishCommand command,
            Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.generations SET state='PUBLISHED', published_at=? "
                        + "WHERE generation_id=? AND state='COMMITTED'")) {
            setInstant(statement, 1, now);
            statement.setObject(2, command.generation().id().value());
            if (statement.executeUpdate() != 1) {
                throw MaterializationException.evidenceMismatch(
                        "generation " + command.generation().id().value()
                                + " is not committed and cannot be published");
            }
        }
    }

    private static void publishAttempt(Connection connection, PublishCommand command, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.attempts SET state='PUBLISHED', updated_at=? "
                        + "WHERE attempt_id=? AND state='COMMITTED'")) {
            setInstant(statement, 1, now);
            statement.setObject(2, command.attempt().id().value());
            if (statement.executeUpdate() != 1) {
                throw MaterializationException.evidenceMismatch(
                        "attempt " + command.attempt().id().value()
                                + " is not committed and cannot be published");
            }
        }
    }

    private void recordPublication(Connection connection, PublishCommand command,
            MaterializationObject desired, MaterializationEvent event, Instant now)
            throws SQLException {
        JdbcMaterializationWrites.recordPublicationCommand(connection,
                new CommandRecord(command.object().tenant(), command.commandId(),
                        JdbcMaterializationRows.publicationReference(command.object(),
                                command.generation().id(), desired.revision()), now));
        advanceDependentSchedules(connection, command.object(), now);
        JdbcMaterializationWrites.insertEvent(connection, event);
    }

    private record Head(MaterializationObject current, UUID publishedGeneration) {
    }

    /**
     * Brings unleased active dependent schedules due in the publish transaction.
     *
     * <p>Only current-definition edges count, and the scheduler still claims
     * each dependent under its own lease, so an upstream publication never
     * executes downstream work inline. Advancing the deadlines is part of the
     * publication atom: a committed upstream head always marks its dependents.
     */
    private void advanceDependentSchedules(Connection connection, MaterializationId upstream,
            Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.schedules s SET next_due_at=?, version=s.version+1 "
                        + "FROM materialization.dependencies d "
                        + "JOIN materialization.objects o ON o.tenant_id=d.tenant_id "
                        + "AND o.object_name=d.downstream_object "
                        + "WHERE d.tenant_id=? AND d.upstream_object=? "
                        + "AND d.downstream_definition_revision=o.definition_revision "
                        + "AND d.downstream_object<>d.upstream_object "
                        + "AND o.state NOT IN ('DROPPING','DROPPED') "
                        + "AND s.tenant_id=d.tenant_id "
                        + "AND s.object_name=d.downstream_object AND s.state='ACTIVE' "
                        + "AND s.lease_owner IS NULL "
                        + "AND (s.next_due_at IS NULL OR s.next_due_at>?)")) {
            setInstant(statement, 1, now);
            JdbcMaterializationRows.bindObjectId(statement, 2, upstream);
            setInstant(statement, 4, now);
            statement.executeUpdate();
        }
    }

    private PublicationResult replayPublication(PublishCommand command, CommandRecord receipt) {
        JsonNode reference = JdbcMaterializationRows.parseCommandReference(receipt.resultReference());
        String objectName = reference.path("object").asText();
        MaterializationId object = MaterializationId.parse(objectName);
        GenerationId generationId = new GenerationId(
                UUID.fromString(reference.path("generation").asText()));
        MaterializationObject header = repository.findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        Generation published = repository.findGeneration(generationId)
                .orElseThrow(() -> MaterializationException.requestRejected(
                        "recorded publication generation is missing: " + generationId.value()));
        return new PublicationResult(header, published, false);
    }

    private void requireCommittedAttempt(Connection connection, PublishCommand command)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state FROM materialization.attempts WHERE attempt_id=? FOR SHARE")) {
            statement.setObject(1, command.attempt().id().value());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || !result.getString("state").equals("COMMITTED")) {
                    throw MaterializationException.evidenceMismatch(
                            "attempt " + command.attempt().id().value()
                                    + " is not in the committed state");
                }
            }
        }
        Optional<CommitEvidence> recorded = findEvidence(connection, command.attempt().id());
        if (recorded.isEmpty() || !recorded.orElseThrow().equals(command.evidence())) {
            throw MaterializationException.evidenceMismatch(
                    "publication evidence does not match the recorded attempt evidence");
        }
    }

    private void requireCommittedGeneration(Connection connection, PublishCommand command)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT state, producing_attempt_id, physical_catalog, physical_schema, "
                        + "physical_table FROM materialization.generations "
                        + "WHERE generation_id=? FOR SHARE")) {
            statement.setObject(1, command.generation().id().value());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()
                        || !result.getString("state").equals("COMMITTED")
                        || !result.getObject("producing_attempt_id", UUID.class)
                                .equals(command.attempt().id().value())
                        || !result.getString("physical_catalog")
                                .equals(command.generation().physicalTable().catalog())
                        || !result.getString("physical_schema")
                                .equals(command.generation().physicalTable().schema())
                        || !result.getString("physical_table")
                                .equals(command.generation().physicalTable().table())) {
                    throw MaterializationException.evidenceMismatch(
                            "generation " + command.generation().id().value()
                                    + " is not the committed candidate of its attempt");
                }
            }
        }
    }

    private void requireLiveFence(Connection connection, ExecutionFence fence) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT l.epoch, l.expires_at > clock_timestamp() AS leader_live, "
                        + "a.expires_at > clock_timestamp() AS anchor_live, "
                        + "? > clock_timestamp() AS lease_live "
                        + "FROM distributed_lease l CROSS JOIN execution_anchor a "
                        + "WHERE l.lease_key='coordinator_leader' AND a.execution_id=?")) {
            setInstant(statement, 1, fence.leaseExpiresAt());
            statement.setString(2, fence.executionAnchor());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()
                        || result.getLong("epoch") != fence.controlEpoch()
                        || !result.getBoolean("leader_live")
                        || !result.getBoolean("anchor_live")
                        || !result.getBoolean("lease_live")) {
                    throw MaterializationException.fenceRejected(
                            "execution fence for anchor " + fence.executionAnchor()
                                    + " is not the live coordinator authority");
                }
            }
        }
    }

    private Optional<CommitEvidence> findEvidence(Connection connection, AttemptId attempt)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT connector_id, commit_fingerprint, manifest_hash, format_version, "
                        + "committed_at FROM materialization.commit_evidence WHERE attempt_id=?")) {
            statement.setObject(1, attempt.value());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new CommitEvidence(
                        result.getString("connector_id"),
                        result.getString("commit_fingerprint"),
                        result.getString("manifest_hash"),
                        result.getString("format_version"),
                        getInstant(result, "committed_at")));
            }
        }
    }

    private void moveToCommitted(Connection connection, AttemptId attempt, Instant now)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.attempts SET state='COMMITTED', updated_at=? "
                        + "WHERE attempt_id=? AND state IN ('PREPARING','BUILDING','QUARANTINED')")) {
            setInstant(statement, 1, now);
            statement.setObject(2, attempt.value());
            if (statement.executeUpdate() == 0) {
                try (PreparedStatement check = connection.prepareStatement(
                        "SELECT state FROM materialization.attempts WHERE attempt_id=?")) {
                    check.setObject(1, attempt.value());
                    try (ResultSet result = check.executeQuery()) {
                        if (!result.next() || !List.of("COMMITTED", "PUBLISHED")
                                .contains(result.getString("state"))) {
                            throw MaterializationException.evidenceMismatch(
                                    "attempt " + attempt.value() + " cannot move to committed");
                        }
                    }
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.generations SET state='COMMITTED' "
                        + "WHERE producing_attempt_id=? AND state IN ('CANDIDATE','QUARANTINED')")) {
            statement.setObject(1, attempt.value());
            if (statement.executeUpdate() == 0) {
                try (PreparedStatement check = connection.prepareStatement(
                        "SELECT state FROM materialization.generations WHERE producing_attempt_id=?")) {
                    check.setObject(1, attempt.value());
                    try (ResultSet result = check.executeQuery()) {
                        if (!result.next() || !List.of("COMMITTED", "PUBLISHED")
                                .contains(result.getString("state"))) {
                            throw MaterializationException.evidenceMismatch(
                                    "generation of attempt " + attempt.value()
                                            + " cannot move to committed");
                        }
                    }
                }
            }
        }
    }

}
