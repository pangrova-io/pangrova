package org.pangrova.materialization.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.MaterializationEvent;

import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.canonicalInstant;
import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.setInstant;
import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.setNullableInstant;

/**
 * Object, definition, and schedule lifecycle transactions of the JDBC authority.
 *
 * <p>Every method moves one durable entity exactly one step and is fenced by
 * head-revision CAS or schedule-version CAS. Command receipts and events are
 * appended inside the same transaction as the mutation they describe, so a
 * crash can never leave a state change without its audit trail.
 */
final class JdbcMaterializationObjectLifecycle {

    private final JdbcMaterializationSql sql;
    private final DataSource dataSource;

    /**
     * Creates the lifecycle over the shared SQL plumbing and connection pool.
     *
     * @param sql transaction plumbing shared with the owning repository
     * @param dataSource pool that owns the materialization schema
     */
    JdbcMaterializationObjectLifecycle(JdbcMaterializationSql sql, DataSource dataSource) {
        if (sql == null || dataSource == null) {
            throw new IllegalArgumentException("sql and dataSource must not be null");
        }
        this.sql = sql;
        this.dataSource = dataSource;
    }

    void createObject(MaterializationObject object, DefinitionRevision definition,
            Schedule schedule, CommandId commandId, MaterializationEvent event) {
        if (!createObjectIfAbsent(object, definition, schedule, commandId, event)) {
            throw MaterializationException.requestRejected(
                    "materialization " + object.id().canonicalName() + " already exists");
        }
    }

    boolean createObjectIfAbsent(MaterializationObject object,
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

    boolean reclaimDroppedObject(MaterializationObject expectedDropped,
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
                statement.setString(5, JdbcMaterializationStateCodec.objectStateName(replacement.state()));
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

    void renameObject(MaterializationObject object, MaterializationId previous,
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

    void recordDefinition(DefinitionRevision definition) {
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

    void replaceDefinition(DefinitionRevision definition, MaterializationObject object,
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

    void updateObject(MaterializationObject object, long expectedRevision,
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
                statement.setString(4, JdbcMaterializationStateCodec.objectStateName(object.state()));
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

    void dropObject(MaterializationObject dropped,
            Optional<GenerationId> publishedGeneration, long expectedRevision,
            CommandId commandId, MaterializationEvent event) {
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

    void updateSchedule(Schedule schedule, long expectedVersion) {
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

    Optional<Schedule> claimDueSchedule(String owner, Instant now, Duration leaseDuration) {
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
}
