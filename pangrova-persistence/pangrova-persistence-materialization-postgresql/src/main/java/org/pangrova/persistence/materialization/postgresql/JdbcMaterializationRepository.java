package org.pangrova.persistence.materialization.postgresql;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.domain.GcLedgerEntry;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.ObjectState;
import org.pangrova.materialization.api.domain.ReaderId;
import org.pangrova.materialization.api.domain.ReaderPin;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.domain.SourcePin;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.CommandRecord;
import org.pangrova.materialization.api.port.GcHistoryEntry;
import org.pangrova.materialization.api.port.MaterializationEvent;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.materialization.api.port.PublicationResult;
import org.pangrova.materialization.api.port.PublishCommand;
import org.pangrova.materialization.api.port.ReclaimableSourcePin;
import org.pangrova.connector.api.materialization.CommitEvidence;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.canonicalInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setJson;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setNullableInstant;

/**
 * PostgreSQL authority of the unified materialization kernel.
 *
 * <p>All invariants that must survive a crashed coordinator are enforced again
 * in V68 triggers: head CAS, immutable attempt and generation identity,
 * execution fences, exact Native commit evidence, source-vector identity, and
 * legal state transitions. This class owns the lifecycle transactions; row
 * mapping, read queries, publication, and lease statements live in package
 * collaborators so each file stays reviewable.
 */
public final class JdbcMaterializationRepository implements MaterializationRepository {

    private final DataSource dataSource;
    private final JdbcMaterializationSql sql;
    private final JdbcMaterializationQueries queries;
    private final JdbcMaterializationPublications publications;
    private final JdbcMaterializationReaderPins readerPins;

    /**
     * Creates the repository over the metadata connection pool.
     *
     * @param dataSource pool that owns the materialization schema
     */
    public JdbcMaterializationRepository(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
        this.sql = new JdbcMaterializationSql(dataSource);
        this.queries = new JdbcMaterializationQueries(sql);
        this.publications = new JdbcMaterializationPublications(sql, this);
        this.readerPins = new JdbcMaterializationReaderPins(dataSource);
    }

    @Override
    public void createObject(MaterializationObject object, DefinitionRevision definition,
            Schedule schedule, CommandId commandId, MaterializationEvent event) {
        if (!createObjectIfAbsent(object, definition, schedule, commandId, event)) {
            throw MaterializationException.requestRejected(
                    "materialization " + object.id().canonicalName() + " already exists");
        }
    }

    @Override
    public boolean createObjectIfAbsent(MaterializationObject object,
            DefinitionRevision definition, Schedule schedule, CommandId commandId,
            MaterializationEvent event) {
        JdbcMaterializationRows.requireMatchingObject(object, definition);
        JdbcMaterializationRows.requireMatchingObject(object, schedule);
        if (object.revision() != 0 || object.definitionRevision() != 1) {
            throw MaterializationException.invalidDefinition(
                    "a new materialization must start at revision 0 with definition revision 1");
        }
        if (definition.revision() != 1) {
            throw MaterializationException.invalidDefinition(
                    "the first definition revision must be 1");
        }
        return sql.inTransaction("createObjectIfAbsent", connection -> {
            if (JdbcMaterializationWrites.insertObjectIfAbsent(connection, object) == 0) {
                return false;
            }
            JdbcMaterializationWrites.insertDefinition(connection, object.id(), definition);
            JdbcMaterializationWrites.insertSchedule(connection, schedule);
            JdbcMaterializationWrites.recordCommand(connection, new CommandRecord(object.id().tenant(), commandId,
                    JdbcMaterializationRows.objectReference(object.id(), 0, "created"), event.occurredAt()));
            JdbcMaterializationWrites.insertEvent(connection, event);
            return true;
        });
    }

    @Override
    public boolean reclaimDroppedObject(MaterializationObject expectedDropped,
            MaterializationObject replacement, DefinitionRevision definition, Schedule schedule,
            CommandId commandId, MaterializationEvent event) {
        JdbcMaterializationRows.requireMatchingObject(replacement, definition);
        JdbcMaterializationRows.requireMatchingObject(replacement, schedule);
        if (!expectedDropped.id().equals(replacement.id())) {
            throw MaterializationException.invalidDefinition(
                    "a reclaimed object must keep its logical identity");
        }
        if (expectedDropped.state() != ObjectState.Dropped.INSTANCE) {
            throw MaterializationException.invalidDefinition(
                    "only a dropped materialization can be reclaimed");
        }
        if (replacement.revision() != 0 || replacement.definitionRevision() != 1) {
            throw MaterializationException.invalidDefinition(
                    "a reclaimed incarnation must start at revision 0 with definition revision 1");
        }
        return sql.inTransaction("reclaimDroppedObject", connection -> {
            try (java.sql.Statement marker = connection.createStatement()) {
                marker.execute("SET LOCAL pangrova.materialization_reclaim = 'on'");
            }
            int updated;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET object_kind=?, incarnation_id=?, "
                            + "definition_revision=?, revision=?, published_generation_id=NULL, "
                            + "state=?, definer_principal=?, security_mode=?, refresh_mode=?, "
                            + "incremental_strategy=?, created_at=?, updated_at=? "
                            + "WHERE tenant_id=? AND object_name=? AND incarnation_id=? "
                            + "AND state='DROPPED'")) {
                statement.setString(1, replacement.kind().name());
                statement.setObject(2, replacement.incarnation().value());
                statement.setLong(3, replacement.definitionRevision());
                statement.setLong(4, replacement.revision());
                statement.setString(5, JdbcMaterializationRows.objectStateName(replacement.state()));
                statement.setString(6, replacement.definer().name());
                statement.setString(7, replacement.securityMode().name());
                statement.setString(8, replacement.refreshMode().name());
                if (replacement.incrementalStrategy().isPresent()) {
                    statement.setString(9,
                            replacement.incrementalStrategy().orElseThrow().value());
                } else {
                    statement.setNull(9, java.sql.Types.VARCHAR);
                }
                setInstant(statement, 10, replacement.createdAt());
                setInstant(statement, 11, replacement.updatedAt());
                JdbcMaterializationRows.bindObjectId(statement, 12, replacement.id());
                statement.setObject(14, expectedDropped.incarnation().value());
                updated = statement.executeUpdate();
            }
            if (updated == 0) {
                return false;
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM materialization.definition_revisions "
                            + "WHERE tenant_id=? AND object_name=?")) {
                JdbcMaterializationRows.bindObjectId(statement, 1, replacement.id());
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM materialization.schedules "
                            + "WHERE tenant_id=? AND object_name=?")) {
                JdbcMaterializationRows.bindObjectId(statement, 1, replacement.id());
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM materialization.dependencies "
                            + "WHERE tenant_id=? AND (downstream_object=? OR upstream_object=?)")) {
                JdbcMaterializationRows.bindObjectId(statement, 1, replacement.id());
                statement.setString(2, replacement.id().name().toString());
                statement.setString(3, replacement.id().name().toString());
                statement.executeUpdate();
            }
            JdbcMaterializationWrites.insertDefinition(connection, replacement.id(), definition);
            JdbcMaterializationWrites.insertSchedule(connection, schedule);
            JdbcMaterializationWrites.recordCommand(connection, new CommandRecord(replacement.id().tenant(), commandId,
                    JdbcMaterializationRows.objectReference(replacement.id(), 0, "created"), event.occurredAt()));
            JdbcMaterializationWrites.insertEvent(connection, event);
            return true;
        });
    }

    @Override
    public void renameObject(MaterializationObject object, MaterializationId previous,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        if (previous == null || !previous.tenant().equals(object.id().tenant())) {
            throw MaterializationException.invalidDefinition(
                    "a rename must keep the tenant and name an existing identity");
        }
        if (object.revision() != expectedRevision + 1) {
            throw MaterializationException.invalidDefinition(
                    "a renamed object header must advance exactly one revision");
        }
        sql.inTransaction("renameObject", connection -> {
            try (java.sql.Statement marker = connection.createStatement()) {
                marker.execute("SET LOCAL pangrova.materialization_rename = 'on'");
            }
            int updated;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET object_name=?, revision=?, updated_at=? "
                            + "WHERE tenant_id=? AND object_name=? AND revision=? "
                            + "AND state IN ('CREATING', 'ACTIVE', 'SUSPENDED', 'FAILED')")) {
                statement.setString(1, object.id().name().toString());
                statement.setLong(2, object.revision());
                setInstant(statement, 3, object.updatedAt());
                JdbcMaterializationRows.bindObjectId(statement, 4, previous);
                statement.setLong(6, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + previous.canonicalName()
                                + " is no longer at revision " + expectedRevision);
            }
            JdbcMaterializationWrites.recordCommand(connection, new CommandRecord(object.id().tenant(), commandId,
                    JdbcMaterializationRows.objectReference(object.id(), object.revision(), "renamed"),
                    event.occurredAt()));
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }

    @Override
    public void recordDefinition(DefinitionRevision definition) {
        JdbcMaterializationRows.requireStorableFingerprint(definition);
        sql.inTransaction("recordDefinition", connection -> {
            long current = JdbcMaterializationWrites.currentDefinitionRevision(connection, definition.object());
            if (definition.revision() != current + 1) {
                throw MaterializationException.invalidDefinition(
                        "definition revision must advance exactly one: expected " + (current + 1)
                                + " but found " + definition.revision());
            }
            JdbcMaterializationWrites.insertDefinition(connection, definition.object(), definition);
            return null;
        });
    }

    @Override
    public void replaceDefinition(DefinitionRevision definition, MaterializationObject object,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        JdbcMaterializationRows.requireStorableFingerprint(definition);
        JdbcMaterializationRows.requireMatchingObject(object, definition);
        if (object.revision() != expectedRevision + 1) {
            throw MaterializationException.invalidDefinition(
                    "an updated object header must advance exactly one revision");
        }
        sql.inTransaction("replaceDefinition", connection -> {
            long current = JdbcMaterializationWrites.currentDefinitionRevision(connection, definition.object());
            if (!definition.object().equals(object.id())
                    || definition.revision() != current + 1
                    || definition.revision() != object.definitionRevision()) {
                throw MaterializationException.invalidDefinition(
                        "definition revision must advance exactly one: expected " + (current + 1)
                                + " but found " + definition.revision());
            }
            // The header trigger requires the referenced definition revision to
            // exist, so the revision row must land before the head moves.
            JdbcMaterializationWrites.insertDefinition(connection, object.id(), definition);
            int updated;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET definition_revision=?, revision=?, "
                            + "refresh_mode=?, incremental_strategy=?, definer_principal=?, "
                            + "updated_at=? "
                            + "WHERE tenant_id=? AND object_name=? AND revision=? "
                            + "AND state IN ('ACTIVE', 'SUSPENDED')")) {
                statement.setLong(1, object.definitionRevision());
                statement.setLong(2, object.revision());
                statement.setString(3, object.refreshMode().name());
                if (object.incrementalStrategy().isPresent()) {
                    statement.setString(4, object.incrementalStrategy().orElseThrow().value());
                } else {
                    statement.setNull(4, java.sql.Types.VARCHAR);
                }
                statement.setString(5, object.definer().name());
                setInstant(statement, 6, object.updatedAt());
                JdbcMaterializationRows.bindObjectId(statement, 7, object.id());
                statement.setLong(9, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + object.id().canonicalName()
                                + " is no longer at revision " + expectedRevision);
            }
            JdbcMaterializationWrites.recordCommand(connection,
                    new CommandRecord(object.id().tenant(), commandId,
                            JdbcMaterializationRows.objectReference(
                                    object.id(), object.revision(), "redefined"),
                            event.occurredAt()));
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }

    @Override
    public void updateObject(MaterializationObject object, long expectedRevision,
            CommandId commandId, MaterializationEvent event) {
        if (object.revision() != expectedRevision + 1) {
            throw MaterializationException.invalidDefinition(
                    "an updated object header must advance exactly one revision");
        }
        sql.inTransaction("updateObject", connection -> {
            int updated;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET definition_revision=?, revision=?, "
                            + "published_generation_id=?, state=?, refresh_mode=?, "
                            + "incremental_strategy=?, updated_at=? "
                            + "WHERE tenant_id=? AND object_name=? AND revision=?")) {
                statement.setLong(1, object.definitionRevision());
                statement.setLong(2, object.revision());
                if (object.publishedGeneration().isPresent()) {
                    statement.setObject(3, object.publishedGeneration().orElseThrow().value());
                } else {
                    statement.setNull(3, java.sql.Types.OTHER);
                }
                statement.setString(4, JdbcMaterializationRows.objectStateName(object.state()));
                statement.setString(5, object.refreshMode().name());
                if (object.incrementalStrategy().isPresent()) {
                    statement.setString(6, object.incrementalStrategy().orElseThrow().value());
                } else {
                    statement.setNull(6, java.sql.Types.VARCHAR);
                }
                setInstant(statement, 7, object.updatedAt());
                JdbcMaterializationRows.bindObjectId(statement, 8, object.id());
                statement.setLong(10, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + object.id().canonicalName()
                                + " is no longer at revision " + expectedRevision);
            }
            JdbcMaterializationWrites.recordCommand(connection,
                    new CommandRecord(object.id().tenant(), commandId,
                            JdbcMaterializationRows.objectReference(
                                    object.id(), object.revision(), "altered"),
                            event.occurredAt()));
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }
    @Override
    public void reserveAttempt(RefreshAttempt attempt, Generation candidate, List<SourcePin> sourcePins,
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
                statement.setString(8, JdbcMaterializationRows.refreshKindName(attempt.refreshKind()));
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
                statement.setString(20, JdbcMaterializationRows.attemptStateName(attempt.state()));
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
                statement.setString(11, JdbcMaterializationRows.refreshKindName(candidate.refreshKind()));
                setJson(statement, 12, MaterializationJson.sourceVector(candidate.sourceVector()));
                statement.setString(13, candidate.sourceVector().canonicalHash());
                statement.setString(14, JdbcMaterializationRows.generationStateName(candidate.state()));
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

    @Override
    public void settleAttempt(RefreshAttempt previous, RefreshAttempt attempt, Generation generation,
            MaterializationEvent event) {
        if (attempt.updatedAt().isBefore(previous.updatedAt())) {
            throw MaterializationException.requestRejected(
                    "attempt update instant must not move backwards");
        }
        sql.inTransaction("settleAttempt", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.attempts SET state=?, error_code=?, updated_at=? "
                            + "WHERE attempt_id=? AND state=?")) {
                statement.setString(1, JdbcMaterializationRows.attemptStateName(attempt.state()));
                if (attempt.errorCode().isPresent()) {
                    statement.setString(2, attempt.errorCode().orElseThrow());
                } else {
                    statement.setNull(2, java.sql.Types.VARCHAR);
                }
                setInstant(statement, 3, attempt.updatedAt());
                statement.setObject(4, attempt.id().value());
                statement.setString(5, JdbcMaterializationRows.attemptStateName(previous.state()));
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "attempt " + attempt.id().value() + " is no longer in state "
                                    + JdbcMaterializationRows.attemptStateName(previous.state()));
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state=?, published_at=? "
                            + "WHERE generation_id=?")) {
                statement.setString(1, JdbcMaterializationRows.generationStateName(generation.state()));
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

    @Override
    public void dropObject(MaterializationObject dropped,
            Optional<GenerationId> publishedGeneration, long expectedRevision, CommandId commandId,
            MaterializationEvent event) {
        if (dropped.revision() != expectedRevision + 1) {
            throw MaterializationException.invalidDefinition(
                    "a dropped object header must advance exactly one revision");
        }
        Instant now = dropped.updatedAt();
        sql.inTransaction("dropObject", connection -> {
            int updated;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET revision=?, published_generation_id=NULL, "
                            + "state='DROPPED', updated_at=? WHERE tenant_id=? AND object_name=? "
                            + "AND revision=? AND state='DROPPING'")) {
                statement.setLong(1, dropped.revision());
                setInstant(statement, 2, now);
                JdbcMaterializationRows.bindObjectId(statement, 3, dropped.id());
                statement.setLong(5, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + dropped.id().canonicalName()
                                + " is no longer dropping at revision " + expectedRevision);
            }
            if (publishedGeneration.isPresent()) {
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE materialization.generations SET state='RETIRED' "
                                + "WHERE generation_id=? AND state='PUBLISHED'")) {
                    statement.setObject(1, publishedGeneration.orElseThrow().value());
                    statement.executeUpdate();
                }
            }
            settleDroppedAttempts(connection, dropped.id(), now);
            JdbcMaterializationWrites.recordCommand(connection, new CommandRecord(
                    dropped.id().tenant(), commandId,
                    JdbcMaterializationRows.objectReference(dropped.id(), dropped.revision(), "dropped"),
                    now));
            JdbcMaterializationWrites.insertEvent(connection, event);
            return null;
        });
    }

    private static void settleDroppedAttempts(Connection connection, MaterializationId object,
            Instant now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.attempts SET state='SUPERSEDED', "
                        + "error_code='object dropped', updated_at=? "
                        + "WHERE tenant_id=? AND object_name=? AND state='COMMITTED'")) {
            setInstant(statement, 1, now);
            JdbcMaterializationRows.bindObjectId(statement, 2, object);
            statement.executeUpdate();
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE materialization.attempts SET state='ABANDONED', "
                        + "error_code='object dropped', updated_at=? "
                        + "WHERE tenant_id=? AND object_name=? "
                        + "AND state IN ('PREPARING','BUILDING','QUARANTINED')")) {
            setInstant(statement, 1, now);
            JdbcMaterializationRows.bindObjectId(statement, 2, object);
            statement.executeUpdate();
        }
    }

    @Override
    public boolean collectGeneration(Generation generation, GcLedgerEntry entry,
            MaterializationEvent event, Runnable physicalDrop) {
        String expectedState = JdbcMaterializationRows.generationStateName(generation.state());
        return sql.inTransaction("collectGeneration", connection -> {
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
            physicalDrop.run();
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

    @Override
    public void saveGeneration(Generation generation, MaterializationEvent event) {
        sql.inTransaction("saveGeneration", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state=?, published_at=? "
                            + "WHERE generation_id=?")) {
                statement.setString(1, JdbcMaterializationRows.generationStateName(generation.state()));
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

    @Override
    public void updateSchedule(Schedule schedule, long expectedVersion) {
        if (schedule.version() != expectedVersion + 1) {
            throw MaterializationException.requestRejected(
                    "an updated schedule must advance exactly one version");
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE materialization.schedules SET target_lag_ms=?, next_due_at=?, "
                             + "last_success_at=?, consecutive_failures=?, state=?, lease_owner=?, "
                             + "lease_expires_at=?, version=? WHERE tenant_id=? AND object_name=? "
                             + "AND version=?")) {
            if (schedule.targetLag().isPresent()) {
                statement.setLong(1, schedule.targetLag().orElseThrow().value().toMillis());
            } else {
                statement.setNull(1, java.sql.Types.BIGINT);
            }
            setNullableInstant(statement, 2, schedule.nextDueAt().orElse(null));
            setNullableInstant(statement, 3, schedule.lastSuccessAt().orElse(null));
            statement.setInt(4, schedule.consecutiveFailures());
            statement.setString(5, schedule.state().name());
            if (schedule.lease().isPresent()) {
                statement.setString(6, schedule.lease().orElseThrow().owner());
                setInstant(statement, 7, schedule.lease().orElseThrow().expiresAt());
            } else {
                statement.setNull(6, java.sql.Types.VARCHAR);
                statement.setNull(7, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
            }
            statement.setLong(8, schedule.version());
            JdbcMaterializationRows.bindObjectId(statement, 9, schedule.object());
            statement.setLong(11, expectedVersion);
            if (statement.executeUpdate() != 1) {
                throw MaterializationException.concurrentPublication(
                        "schedule for " + schedule.object().canonicalName()
                                + " is no longer at version " + expectedVersion);
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("updateSchedule failed", failure);
        }
    }

    @Override
    public Optional<Schedule> claimDueSchedule(String owner, Instant now, Duration leaseDuration) {
        Instant claimedAt = canonicalInstant(now);
        return sql.inTransaction("claimDueSchedule", connection -> {
            Schedule schedule;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT " + JdbcMaterializationRows.SCHEDULE_COLUMNS + " FROM materialization.schedules "
                            + "WHERE state='ACTIVE' AND next_due_at<=? "
                            + "AND (lease_expires_at IS NULL OR lease_expires_at<=?) "
                            + "ORDER BY next_due_at, object_name LIMIT 1 FOR UPDATE SKIP LOCKED")) {
                setInstant(statement, 1, claimedAt);
                setInstant(statement, 2, claimedAt);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return Optional.empty();
                    }
                    schedule = JdbcMaterializationRows.mapSchedule(result);
                }
            }
            Schedule claimed = schedule.claim(owner, claimedAt, leaseDuration);
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.schedules SET lease_owner=?, lease_expires_at=?, "
                            + "version=? WHERE tenant_id=? AND object_name=? AND version=?")) {
                statement.setString(1, claimed.lease().orElseThrow().owner());
                setInstant(statement, 2, claimed.lease().orElseThrow().expiresAt());
                statement.setLong(3, claimed.version());
                JdbcMaterializationRows.bindObjectId(statement, 4, schedule.object());
                statement.setLong(6, schedule.version());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "schedule claim CAS lost for " + schedule.object().canonicalName());
                }
            }
            return Optional.of(claimed);
        });
    }

    @Override
    public void recordCommand(CommandRecord record) {
        try (Connection connection = dataSource.getConnection()) {
            JdbcMaterializationWrites.recordCommand(connection, record);
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("recordCommand failed", failure);
        }
    }

    @Override
    public Optional<MaterializationObject> findObject(MaterializationId object) {
        return queries.findObject(object);
    }

    @Override
    public Optional<DefinitionRevision> findDefinition(MaterializationId object, long revision) {
        return queries.findDefinition(object, revision);
    }

    @Override
    public List<MaterializationId> findLiveDependents(MaterializationId upstream) {
        return queries.findLiveDependents(upstream);
    }

    @Override
    public List<MaterializationObject> listObjects(TenantId tenant) {
        return queries.listObjects(tenant);
    }

    @Override
    public List<RefreshAttempt> findAttempts(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        return queries.findAttempts(tenant, object, limit);
    }

    @Override
    public List<Dependency> findDependencies(TenantId tenant,
            Optional<MaterializationId> object) {
        return queries.findDependencies(tenant, object);
    }

    @Override
    public List<GcHistoryEntry> findGcHistory(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        return queries.findGcHistory(tenant, object, limit);
    }

    @Override
    public Optional<Generation> findGeneration(GenerationId generation) {
        return queries.findGeneration(generation);
    }

    @Override
    public Optional<Generation> findPublishedGeneration(MaterializationId object) {
        return queries.findPublishedGeneration(object);
    }

    @Override
    public List<Generation> findGenerations(MaterializationId object) {
        return queries.findGenerations(object);
    }

    @Override
    public Optional<RefreshAttempt> findAttempt(AttemptId attempt) {
        return queries.findAttempt(attempt);
    }

    @Override
    public Optional<RefreshAttempt> findActiveAttempt(MaterializationId object) {
        return queries.findActiveAttempt(object);
    }

    @Override
    public Optional<Generation> findGenerationByAttempt(AttemptId attempt) {
        return queries.findGenerationByAttempt(attempt);
    }

    @Override
    public List<RefreshAttempt> findReconcilableAttempts(Instant observedBefore) {
        return queries.findReconcilableAttempts(observedBefore);
    }

    @Override
    public void markAttemptBuilding(AttemptId attempt, Instant now) {
        sql.inTransaction("markAttemptBuilding", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.attempts SET state='BUILDING', updated_at=? "
                            + "WHERE attempt_id=? AND state IN ('PREPARING','BUILDING')")) {
                setInstant(statement, 1, now);
                statement.setObject(2, attempt.value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "attempt " + attempt.value()
                                    + " is no longer preparing for execution");
                }
            }
            return null;
        });
    }

    @Override
    public Optional<Schedule> findSchedule(MaterializationId object) {
        return queries.findSchedule(object);
    }

    @Override
    public Optional<CommandRecord> findCommand(TenantId tenant, CommandId commandId) {
        return queries.findCommand(tenant, commandId);
    }

    @Override
    public Optional<MaterializationWriteCapability> findWriteCapability(AttemptId attempt) {
        return queries.findWriteCapability(attempt);
    }

    @Override
    public List<ReclaimableSourcePin> findReclaimableSourcePins(
            Instant now, Instant settledBefore, int attempts) {
        return queries.findReclaimableSourcePins(now, settledBefore, attempts);
    }

    @Override
    public List<ReaderPin> findReaderPins(GenerationId generation) {
        return queries.findReaderPins(generation);
    }

    @Override
    public List<Generation> findCollectableGenerations(Instant now, Instant eligibleBefore,
            int limit) {
        return queries.findCollectableGenerations(now, eligibleBefore, limit);
    }

    @Override
    public void recordCommitEvidence(AttemptId attemptId, CommitEvidence evidence, Instant now) {
        publications.recordCommitEvidence(attemptId, evidence, now);
    }

    @Override
    public PublicationResult publish(PublishCommand command, MaterializationEvent event) {
        return publications.publish(command, event);
    }

    @Override
    public void releaseSourcePins(AttemptId attempt, Instant now) {
        readerPins.releaseSourcePins(attempt, now);
    }

    @Override
    public void pinReader(ReaderPin pin) {
        readerPins.pinReader(pin);
    }

    @Override
    public void releaseReaderPin(GenerationId generation, ReaderId reader) {
        readerPins.releaseReaderPin(generation, reader);
    }

    @Override
    public void releaseReaderPins(ReaderId reader) {
        readerPins.releaseReaderPins(reader);
    }

    @Override
    public void renewReaderPins(ReaderId reader, Instant expiresAt, Instant heartbeatAt) {
        readerPins.renewReaderPins(reader, expiresAt, heartbeatAt);
    }

    @Override
    public int purgeExpiredReaderPins(Instant now) {
        return readerPins.purgeExpiredReaderPins(now);
    }

    @Override
    public void recordGcDecision(GcLedgerEntry entry) {
        readerPins.recordGcDecision(entry);
    }

    @Override
    public void updateGcDecision(UUID entryId, GcLedgerEntry.GcState state, Instant now) {
        readerPins.updateGcDecision(entryId, state, now);
    }
}
