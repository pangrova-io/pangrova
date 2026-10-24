package org.pangrova.metadata.materialization;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.AttemptState;
import org.pangrova.materialization.domain.Binding;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.DependencyKind;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.GenerationState;
import org.pangrova.materialization.domain.IncarnationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.PolicySnapshot;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.domain.ReaderPin;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.ScheduleLease;
import org.pangrova.materialization.domain.ScheduleState;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.GcHistoryEntry;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationRepository;
import org.pangrova.materialization.port.PublicationResult;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.materialization.port.ReclaimableSourcePin;
import org.pangrova.spi.materialization.CommitEvidence;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

import static org.pangrova.metadata.store.JdbcSupport.canonicalInstant;
import static org.pangrova.metadata.store.JdbcSupport.getInstant;
import static org.pangrova.metadata.store.JdbcSupport.setInstant;
import static org.pangrova.metadata.store.JdbcSupport.setJson;
import static org.pangrova.metadata.store.JdbcSupport.setNullableInstant;

/**
 * PostgreSQL authority of the unified materialization kernel.
 *
 * <p>All invariants that must survive a crashed coordinator are enforced again
 * in V68 triggers: head CAS, immutable attempt and generation identity,
 * execution fences, exact Native commit evidence, source-vector identity, and
 * legal state transitions. This class validates before writing so callers see
 * domain error codes instead of raw constraint failures.
 */
public final class JdbcMaterializationRepository implements MaterializationRepository {

    private static final String OBJECT_COLUMNS =
            "tenant_id, object_name, object_kind, incarnation_id, definition_revision, revision, "
                    + "published_generation_id, state, definer_principal, security_mode, "
                    + "refresh_mode, incremental_strategy, created_at, updated_at";

    private static final String GENERATION_COLUMNS =
            "g.generation_id, g.tenant_id, g.object_name, g.incarnation_id, g.definition_revision, "
                    + "g.producing_attempt_id, g.base_generation_id, g.physical_catalog, "
                    + "g.physical_schema, g.physical_table, g.refresh_kind, g.source_vector, "
                    + "g.source_vector_hash, g.state, g.created_at, g.published_at, "
                    + "e.connector_id AS evidence_connector_id, "
                    + "e.commit_fingerprint AS evidence_commit_fingerprint, "
                    + "e.manifest_hash AS evidence_manifest_hash, "
                    + "e.format_version AS evidence_format_version, "
                    + "e.committed_at AS evidence_committed_at";

    private static final String ATTEMPT_COLUMNS =
            "a.attempt_id, a.tenant_id, a.object_name, a.incarnation_id, a.definition_revision, "
                    + "a.base_object_revision, a.base_generation_id, a.refresh_kind, a.source_vector, "
                    + "a.source_vector_hash, a.trigger_kind, a.requesting_principal, "
                    + "a.definer_principal, a.execution_anchor, a.control_epoch, a.lease_expires_at, "
                    + "a.candidate_catalog, a.candidate_schema, a.candidate_table, a.state, "
                    + "a.error_code, a.created_at, a.updated_at, "
                    + "e.connector_id AS evidence_connector_id, "
                    + "e.commit_fingerprint AS evidence_commit_fingerprint, "
                    + "e.manifest_hash AS evidence_manifest_hash, "
                    + "e.format_version AS evidence_format_version, "
                    + "e.committed_at AS evidence_committed_at";

    private static final String SCHEDULE_COLUMNS =
            "tenant_id, object_name, target_lag_ms, next_due_at, last_success_at, "
                    + "consecutive_failures, state, lease_owner, lease_expires_at, version";

    private final DataSource dataSource;

    public JdbcMaterializationRepository(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    @Override
    public Optional<MaterializationObject> findObject(MaterializationId object) {
        String sql = "SELECT " + OBJECT_COLUMNS + " FROM materialization.objects "
                + "WHERE tenant_id=? AND object_name=?";
        return queryOne("findObject", sql, statement -> {
            bindObjectId(statement, 1, object);
        }, this::mapObject);
    }

    @Override
    public Optional<DefinitionRevision> findDefinition(MaterializationId object, long revision) {
        String sql = "SELECT revision, definition_sql, canonical_sql, plan_fingerprint, resolved, "
                + "output_schema, output_schema_hash, sources, policy_snapshot, security_mode, "
                + "definer_principal, session_properties, created_at "
                + "FROM materialization.definition_revisions "
                + "WHERE tenant_id=? AND object_name=? AND revision=?";
        return queryOne("findDefinition", sql, statement -> {
            bindObjectId(statement, 1, object);
            statement.setLong(3, revision);
        }, result -> mapDefinition(object, result));
    }

    @Override
    public List<MaterializationId> findLiveDependents(MaterializationId upstream) {
        String sql = "SELECT d.downstream_object FROM materialization.dependencies d "
                + "JOIN materialization.objects o ON o.tenant_id=d.tenant_id "
                + "AND o.object_name=d.downstream_object "
                + "WHERE d.tenant_id=? AND d.upstream_object=? "
                + "AND d.downstream_definition_revision=o.definition_revision "
                + "AND o.state NOT IN ('DROPPING','DROPPED') "
                + "ORDER BY d.downstream_object";
        return queryList("findLiveDependents", sql, statement -> {
            statement.setString(1, upstream.tenant().value());
            statement.setString(2, upstream.name().toString());
        }, result -> new MaterializationId(upstream.tenant(),
                QualifiedName.parse(result.getString("downstream_object"))));
    }

    @Override
    public List<MaterializationObject> listObjects(TenantId tenant) {
        String sql = "SELECT " + OBJECT_COLUMNS + " FROM materialization.objects "
                + "WHERE tenant_id=? ORDER BY object_name";
        return queryList("listObjects", sql,
                statement -> statement.setString(1, tenant.value()), this::mapObject);
    }

    @Override
    public List<RefreshAttempt> findAttempts(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        requireLimit(limit);
        boolean scoped = object.isPresent();
        String sql = "SELECT " + ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.tenant_id=?" + (scoped ? " AND a.object_name=?" : "")
                + " ORDER BY a.created_at DESC, a.attempt_id DESC LIMIT ?";
        return queryList("findAttempts", sql, statement -> {
            statement.setString(1, tenant.value());
            int index = 2;
            if (scoped) {
                statement.setString(index++, object.orElseThrow().name().toString());
            }
            statement.setInt(index, limit);
        }, this::mapAttempt);
    }

    @Override
    public List<Dependency> findDependencies(TenantId tenant,
            Optional<MaterializationId> object) {
        boolean scoped = object.isPresent();
        String sql = "SELECT tenant_id, downstream_object, upstream_object, "
                + "downstream_definition_revision, dependency_kind "
                + "FROM materialization.dependencies WHERE tenant_id=?"
                + (scoped ? " AND (downstream_object=? OR upstream_object=?)" : "")
                + " ORDER BY downstream_object, downstream_definition_revision, upstream_object";
        return queryList("findDependencies", sql, statement -> {
            statement.setString(1, tenant.value());
            if (scoped) {
                String name = object.orElseThrow().name().toString();
                statement.setString(2, name);
                statement.setString(3, name);
            }
        }, result -> {
            TenantId owningTenant = new TenantId(result.getString("tenant_id"));
            return new Dependency(
                    new MaterializationId(owningTenant,
                            QualifiedName.parse(result.getString("upstream_object"))),
                    new MaterializationId(owningTenant,
                            QualifiedName.parse(result.getString("downstream_object"))),
                    result.getLong("downstream_definition_revision"),
                    DependencyKind.valueOf(result.getString("dependency_kind")));
        });
    }

    @Override
    public List<GcHistoryEntry> findGcHistory(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        requireLimit(limit);
        boolean scoped = object.isPresent();
        String sql = "SELECT g.tenant_id, g.object_name, l.entry_id, l.generation_id, "
                + "l.decision, l.evidence, l.state, l.created_at, l.updated_at "
                + "FROM materialization.gc_ledger l "
                + "JOIN materialization.generations g ON g.generation_id=l.generation_id "
                + "WHERE g.tenant_id=?" + (scoped ? " AND g.object_name=?" : "")
                + " ORDER BY l.created_at DESC, l.entry_id DESC LIMIT ?";
        return queryList("findGcHistory", sql, statement -> {
            statement.setString(1, tenant.value());
            int index = 2;
            if (scoped) {
                statement.setString(index++, object.orElseThrow().name().toString());
            }
            statement.setInt(index, limit);
        }, result -> new GcHistoryEntry(
                new MaterializationId(new TenantId(result.getString("tenant_id")),
                        QualifiedName.parse(result.getString("object_name"))),
                result.getObject("entry_id", UUID.class),
                new GenerationId(result.getObject("generation_id", UUID.class)),
                GcLedgerEntry.GcDecision.valueOf(result.getString("decision")),
                result.getString("evidence"),
                GcLedgerEntry.GcState.valueOf(result.getString("state")),
                getInstant(result, "created_at"),
                getInstant(result, "updated_at")));
    }

    private static void requireLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
    }

    @Override
    public Optional<Generation> findGeneration(GenerationId generation) {
        String sql = "SELECT " + GENERATION_COLUMNS + " FROM materialization.generations g "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE g.generation_id=?";
        return queryOne("findGeneration", sql, statement ->
                statement.setObject(1, generation.value()), this::mapGeneration);
    }

    @Override
    public Optional<Generation> findPublishedGeneration(MaterializationId object) {
        String sql = "SELECT " + GENERATION_COLUMNS + " FROM materialization.generations g "
                + "JOIN materialization.objects o ON o.tenant_id=g.tenant_id "
                + "AND o.object_name=g.object_name AND o.published_generation_id=g.generation_id "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE o.tenant_id=? AND o.object_name=?";
        return queryOne("findPublishedGeneration", sql, statement -> {
            bindObjectId(statement, 1, object);
        }, this::mapGeneration);
    }

    @Override
    public List<Generation> findGenerations(MaterializationId object) {
        String sql = "SELECT " + GENERATION_COLUMNS + " FROM materialization.generations g "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE g.tenant_id=? AND g.object_name=? ORDER BY g.created_at, g.generation_id";
        return queryList("findGenerations", sql, statement -> {
            bindObjectId(statement, 1, object);
        }, this::mapGeneration);
    }

    @Override
    public Optional<RefreshAttempt> findAttempt(AttemptId attempt) {
        String sql = "SELECT " + ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.attempt_id=?";
        return queryOne("findAttempt", sql, statement ->
                statement.setObject(1, attempt.value()), this::mapAttempt);
    }

    @Override
    public Optional<RefreshAttempt> findActiveAttempt(MaterializationId object) {
        String sql = "SELECT " + ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.tenant_id=? AND a.object_name=? "
                + "AND a.state IN ('PREPARING','BUILDING','COMMITTED','QUARANTINED') "
                + "ORDER BY a.created_at DESC, a.attempt_id DESC LIMIT 1";
        return queryOne("findActiveAttempt", sql, statement -> {
            bindObjectId(statement, 1, object);
        }, this::mapAttempt);
    }

    @Override
    public Optional<Generation> findGenerationByAttempt(AttemptId attempt) {
        String sql = "SELECT " + GENERATION_COLUMNS + " FROM materialization.generations g "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE g.producing_attempt_id=?";
        return queryOne("findGenerationByAttempt", sql, statement ->
                statement.setObject(1, attempt.value()), this::mapGeneration);
    }

    @Override
    public List<RefreshAttempt> findUnsettledAttempts(Instant updatedBefore) {
        String sql = "SELECT " + ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.state IN ('PREPARING','BUILDING','COMMITTED','QUARANTINED') "
                + "AND a.updated_at < ? ORDER BY a.updated_at, a.attempt_id";
        return queryList("findUnsettledAttempts", sql, statement ->
                setInstant(statement, 1, updatedBefore), this::mapAttempt);
    }

    @Override
    public Optional<Schedule> findSchedule(MaterializationId object) {
        String sql = "SELECT " + SCHEDULE_COLUMNS + " FROM materialization.schedules "
                + "WHERE tenant_id=? AND object_name=?";
        return queryOne("findSchedule", sql, statement -> {
            bindObjectId(statement, 1, object);
        }, this::mapSchedule);
    }

    @Override
    public Optional<CommandRecord> findCommand(TenantId tenant, CommandId commandId) {
        String sql = "SELECT command_id, result_reference, created_at FROM materialization.commands "
                + "WHERE tenant_id=? AND command_id=?";
        return queryOne("findCommand", sql, statement -> {
            statement.setString(1, tenant.value());
            statement.setString(2, commandId.value());
        }, result -> new CommandRecord(tenant, commandId, result.getString("result_reference"),
                getInstant(result, "created_at")));
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
        requireMatchingObject(object, definition);
        requireMatchingObject(object, schedule);
        if (object.revision() != 0 || object.definitionRevision() != 1) {
            throw MaterializationException.invalidDefinition(
                    "a new materialization must start at revision 0 with definition revision 1");
        }
        if (definition.revision() != 1) {
            throw MaterializationException.invalidDefinition(
                    "the first definition revision must be 1");
        }
        return inTransaction("createObjectIfAbsent", connection -> {
            if (insertObjectIfAbsent(connection, object) == 0) {
                return false;
            }
            insertDefinition(connection, object.id(), definition);
            insertSchedule(connection, schedule);
            recordCommand(connection, new CommandRecord(object.id().tenant(), commandId,
                    objectReference(object.id(), 0, "created"), event.occurredAt()));
            insertEvent(connection, event);
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
        inTransaction("renameObject", connection -> {
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
                bindObjectId(statement, 4, previous);
                statement.setLong(6, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + previous.canonicalName()
                                + " is no longer at revision " + expectedRevision);
            }
            recordCommand(connection, new CommandRecord(object.id().tenant(), commandId,
                    objectReference(object.id(), object.revision(), "renamed"),
                    event.occurredAt()));
            insertEvent(connection, event);
            return null;
        });
    }

    @Override
    public void recordDefinition(DefinitionRevision definition) {
        requireStorableFingerprint(definition);
        inTransaction("recordDefinition", connection -> {
            long current = currentDefinitionRevision(connection, definition.object());
            if (definition.revision() != current + 1) {
                throw MaterializationException.invalidDefinition(
                        "definition revision must advance exactly one: expected " + (current + 1)
                                + " but found " + definition.revision());
            }
            insertDefinition(connection, definition.object(), definition);
            return null;
        });
    }

    @Override
    public void replaceDefinition(DefinitionRevision definition, MaterializationObject object,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        requireStorableFingerprint(definition);
        if (object.revision() != expectedRevision + 1) {
            throw MaterializationException.invalidDefinition(
                    "an updated object header must advance exactly one revision");
        }
        inTransaction("replaceDefinition", connection -> {
            long current = currentDefinitionRevision(connection, definition.object());
            if (!definition.object().equals(object.id())
                    || definition.revision() != current + 1
                    || definition.revision() != object.definitionRevision()) {
                throw MaterializationException.invalidDefinition(
                        "definition revision must advance exactly one: expected " + (current + 1)
                                + " but found " + definition.revision());
            }
            int updated;
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET definition_revision=?, revision=?, "
                            + "refresh_mode=?, incremental_strategy=?, updated_at=? "
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
                setInstant(statement, 5, object.updatedAt());
                bindObjectId(statement, 6, object.id());
                statement.setLong(8, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + object.id().canonicalName()
                                + " is no longer at revision " + expectedRevision);
            }
            insertDefinition(connection, object.id(), definition);
            recordCommand(connection, new CommandRecord(object.id().tenant(), commandId,
                    objectReference(object.id(), object.revision(), "redefined"), event.occurredAt()));
            insertEvent(connection, event);
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
        inTransaction("updateObject", connection -> {
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
                statement.setString(4, objectStateName(object.state()));
                statement.setString(5, object.refreshMode().name());
                if (object.incrementalStrategy().isPresent()) {
                    statement.setString(6, object.incrementalStrategy().orElseThrow().value());
                } else {
                    statement.setNull(6, java.sql.Types.VARCHAR);
                }
                setInstant(statement, 7, object.updatedAt());
                bindObjectId(statement, 8, object.id());
                statement.setLong(10, expectedRevision);
                updated = statement.executeUpdate();
            }
            if (updated != 1) {
                throw MaterializationException.concurrentPublication(
                        "object header for " + object.id().canonicalName()
                                + " is no longer at revision " + expectedRevision);
            }
            recordCommand(connection, new CommandRecord(object.id().tenant(), commandId,
                    objectReference(object.id(), object.revision(), "altered"), event.occurredAt()));
            insertEvent(connection, event);
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
        requireCapability(attempt, candidate, capability);
        String sourceVectorHash = attempt.sourceVector().canonicalHash();
        if (!sourceVectorHash.equals(candidate.sourceVector().canonicalHash())) {
            throw MaterializationException.evidenceMismatch(
                    "attempt and candidate carry different source vectors");
        }
        inTransaction("reserveAttempt", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.attempts (attempt_id, tenant_id, object_name, "
                            + "incarnation_id, definition_revision, base_object_revision, "
                            + "base_generation_id, refresh_kind, source_vector, source_vector_hash, "
                            + "trigger_kind, requesting_principal, definer_principal, "
                            + "execution_anchor, control_epoch, lease_expires_at, candidate_catalog, "
                            + "candidate_schema, candidate_table, state, error_code, created_at, "
                            + "updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                statement.setObject(1, attempt.id().value());
                bindObjectId(statement, 2, attempt.object());
                statement.setObject(4, attempt.incarnation().value());
                statement.setLong(5, attempt.definitionRevision());
                statement.setLong(6, attempt.baseObjectRevision());
                bindNullableUuid(statement, 7, attempt.baseGeneration().map(GenerationId::value).orElse(null));
                statement.setString(8, refreshKindName(attempt.refreshKind()));
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
                statement.setString(20, attemptStateName(attempt.state()));
                statement.setNull(21, java.sql.Types.VARCHAR);
                setInstant(statement, 22, attempt.createdAt());
                setInstant(statement, 23, attempt.updatedAt());
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.generations (generation_id, tenant_id, object_name, "
                            + "incarnation_id, definition_revision, producing_attempt_id, "
                            + "base_generation_id, physical_catalog, physical_schema, physical_table, "
                            + "refresh_kind, source_vector, source_vector_hash, state, created_at) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                statement.setObject(1, candidate.id().value());
                bindObjectId(statement, 2, candidate.object());
                statement.setObject(4, candidate.incarnation().value());
                statement.setLong(5, candidate.definitionRevision());
                statement.setObject(6, candidate.producingAttempt().value());
                bindNullableUuid(statement, 7,
                        candidate.baseGeneration().map(GenerationId::value).orElse(null));
                statement.setString(8, candidate.physicalTable().catalog());
                statement.setString(9, candidate.physicalTable().schema());
                statement.setString(10, candidate.physicalTable().table());
                statement.setString(11, refreshKindName(candidate.refreshKind()));
                setJson(statement, 12, MaterializationJson.sourceVector(candidate.sourceVector()));
                statement.setString(13, candidate.sourceVector().canonicalHash());
                statement.setString(14, generationStateName(candidate.state()));
                setInstant(statement, 15, candidate.createdAt());
                statement.executeUpdate();
            }
            for (SourcePin pin : sourcePins) {
                insertSourcePin(connection, pin);
            }
            insertCapability(connection, capability);
            insertEvent(connection, event);
            return null;
        });
    }

    @Override
    public Optional<MaterializationWriteCapability> findWriteCapability(AttemptId attempt) {
        String sql = "SELECT c.attempt_id, c.tenant_id, c.object_name, c.incarnation_id, "
                + "c.definition_revision, c.generation_id, c.candidate_catalog, c.candidate_schema, "
                + "c.candidate_table, c.source_vector_hash, c.definer_principal, c.control_epoch, "
                + "c.issued_at, c.expires_at FROM materialization.write_capabilities c "
                + "WHERE c.attempt_id=?";
        return queryOne("findWriteCapability", sql, statement ->
                statement.setObject(1, attempt.value()), this::mapCapability);
    }

    @Override
    public void recordCommitEvidence(AttemptId attemptId, CommitEvidence evidence, Instant now) {
        Instant recordedAt = canonicalInstant(now);
        inTransaction("recordCommitEvidence", connection -> {
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

    @Override
    public PublicationResult publish(PublishCommand command, MaterializationEvent event) {
        Optional<CommandRecord> receipt = findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent() && isPublicationReceipt(receipt.orElseThrow())) {
            return replayPublication(command, receipt.orElseThrow());
        }
        Instant now = canonicalInstant(event.occurredAt());
        return inTransaction("publish", connection -> {
            MaterializationObject current;
            UUID publishedGeneration;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT " + OBJECT_COLUMNS + " FROM materialization.objects "
                            + "WHERE tenant_id=? AND object_name=? FOR UPDATE")) {
                bindObjectId(statement, 1, command.object());
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        throw MaterializationException.notFound(command.object());
                    }
                    current = mapObject(result);
                    publishedGeneration =
                            result.getObject("published_generation_id", UUID.class);
                }
            }
            long revision = current.revision();
            if (revision != command.expectedObjectRevision()) {
                throw MaterializationException.concurrentPublication(
                        "expected head revision " + command.expectedObjectRevision()
                                + " but found " + revision + " for " + command.object().canonicalName());
            }
            if (!current.incarnation().equals(command.generation().incarnation())
                    || current.definitionRevision() != command.generation().definitionRevision()
                    || current.state() != ObjectState.Active.INSTANCE
                    && current.state() != ObjectState.Creating.INSTANCE
                    && current.state() != ObjectState.Suspended.INSTANCE
                    && current.state() != ObjectState.Failed.INSTANCE) {
                throw MaterializationException.invalidStateTransition(
                        "publication head does not match the candidate generation for "
                                + command.object().canonicalName());
            }
            requireLiveFence(connection, command.fence());
            requireCommittedAttempt(connection, command);
            requireCommittedGeneration(connection, command);
            MaterializationObject desired = current.publish(
                    command.generation().id(), revision, now);
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.objects SET revision=?, published_generation_id=?, "
                            + "state='ACTIVE', updated_at=? "
                            + "WHERE tenant_id=? AND object_name=? AND revision=?")) {
                statement.setLong(1, revision + 1);
                statement.setObject(2, command.generation().id().value());
                setInstant(statement, 3, now);
                bindObjectId(statement, 4, command.object());
                statement.setLong(6, revision);
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "object head CAS lost for " + command.object().canonicalName());
                }
            }
            if (publishedGeneration != null
                    && !publishedGeneration.equals(command.generation().id().value())) {
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
            recordPublicationCommand(connection, new CommandRecord(command.object().tenant(),
                    command.commandId(),
                    publicationReference(command.object(), command.generation().id(),
                            desired.revision()), now));
            advanceDependentSchedules(connection, command.object(), now);
            insertEvent(connection, event);
            return new PublicationResult(desired, publishedView(command.generation(), command.evidence(), now), true);
        });
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
            bindObjectId(statement, 2, upstream);
            setInstant(statement, 4, now);
            statement.executeUpdate();
        }
    }

    @Override
    public void settleAttempt(RefreshAttempt attempt, Generation generation, MaterializationEvent event) {
        inTransaction("settleAttempt", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.attempts SET state=?, error_code=?, updated_at=? "
                            + "WHERE attempt_id=?")) {
                statement.setString(1, attemptStateName(attempt.state()));
                if (attempt.errorCode().isPresent()) {
                    statement.setString(2, attempt.errorCode().orElseThrow());
                } else {
                    statement.setNull(2, java.sql.Types.VARCHAR);
                }
                setInstant(statement, 3, attempt.updatedAt());
                statement.setObject(4, attempt.id().value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.requestRejected(
                            "attempt " + attempt.id().value() + " does not exist");
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state=?, published_at=? "
                            + "WHERE generation_id=?")) {
                statement.setString(1, generationStateName(generation.state()));
                setNullableInstant(statement, 2, generation.publishedAt().orElse(null));
                statement.setObject(3, generation.id().value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.requestRejected(
                            "generation " + generation.id().value() + " does not exist");
                }
            }
            insertEvent(connection, event);
            return null;
        });
    }

    @Override
    public List<ReclaimableSourcePin> findReclaimableSourcePins(
            Instant now, Instant settledBefore, int attempts) {
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be positive");
        }
        String sql = "SELECT p.attempt_id, a.tenant_id, p.connector_id, p.catalog_name, "
                + "p.schema_name, p.table_name, p.snapshot_token, p.schema_fingerprint, "
                + "p.policy_fingerprint, p.observed_at FROM materialization.source_pins p "
                + "JOIN materialization.attempts a ON a.attempt_id=p.attempt_id "
                + "WHERE p.state='ACQUIRED' AND p.attempt_id IN ("
                + "  SELECT q.attempt_id FROM materialization.source_pins q "
                + "  JOIN materialization.attempts b ON b.attempt_id=q.attempt_id "
                + "  WHERE q.state='ACQUIRED' AND ("
                + "    (b.state IN ('PUBLISHED','FAILED','ABANDONED','SUPERSEDED','QUARANTINED') "
                + "      AND b.updated_at<?) OR q.expires_at<=?) "
                + "  ORDER BY q.expires_at, q.attempt_id LIMIT ?) "
                + "ORDER BY p.expires_at, p.attempt_id, p.catalog_name, p.schema_name, p.table_name";
        return queryList("findReclaimableSourcePins", sql, statement -> {
            setInstant(statement, 1, settledBefore);
            setInstant(statement, 2, now);
            statement.setInt(3, attempts);
        }, this::mapReclaimableSourcePin);
    }

    @Override
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

    @Override
    public void saveGeneration(Generation generation, MaterializationEvent event) {
        inTransaction("saveGeneration", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.generations SET state=?, published_at=? "
                            + "WHERE generation_id=?")) {
                statement.setString(1, generationStateName(generation.state()));
                setNullableInstant(statement, 2, generation.publishedAt().orElse(null));
                statement.setObject(3, generation.id().value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.requestRejected(
                            "generation " + generation.id().value() + " does not exist");
                }
            }
            insertEvent(connection, event);
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
            bindObjectId(statement, 9, schedule.object());
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
        return inTransaction("claimDueSchedule", connection -> {
            Schedule schedule;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT " + SCHEDULE_COLUMNS + " FROM materialization.schedules "
                            + "WHERE state='ACTIVE' AND next_due_at<=? "
                            + "AND (lease_expires_at IS NULL OR lease_expires_at<=?) "
                            + "ORDER BY next_due_at, object_name LIMIT 1 FOR UPDATE SKIP LOCKED")) {
                setInstant(statement, 1, claimedAt);
                setInstant(statement, 2, claimedAt);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next()) {
                        return Optional.empty();
                    }
                    schedule = mapSchedule(result);
                }
            }
            Schedule claimed = schedule.claim(owner, claimedAt, leaseDuration);
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.schedules SET lease_owner=?, lease_expires_at=?, "
                            + "version=? WHERE tenant_id=? AND object_name=? AND version=?")) {
                statement.setString(1, claimed.lease().orElseThrow().owner());
                setInstant(statement, 2, claimed.lease().orElseThrow().expiresAt());
                statement.setLong(3, claimed.version());
                bindObjectId(statement, 4, schedule.object());
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
            recordCommand(connection, record);
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("recordCommand failed", failure);
        }
    }

    @Override
    public void pinReader(ReaderPin pin) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO materialization.reader_pins (generation_id, query_id, owner, "
                             + "expires_at, heartbeat_at) VALUES (?,?,?,?,?) "
                             + "ON CONFLICT (generation_id, query_id) DO UPDATE SET "
                             + "heartbeat_at=GREATEST(materialization.reader_pins.heartbeat_at, "
                             + "EXCLUDED.heartbeat_at), "
                             + "expires_at=GREATEST(materialization.reader_pins.expires_at, "
                             + "EXCLUDED.expires_at)")) {
            statement.setObject(1, pin.generation().value());
            statement.setObject(2, pin.query().value());
            statement.setString(3, pin.owner());
            setInstant(statement, 4, pin.expiresAt());
            setInstant(statement, 5, pin.heartbeatAt());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("pinReader failed", failure);
        }
    }

    @Override
    public void releaseReaderPin(GenerationId generation, QueryId query) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM materialization.reader_pins "
                             + "WHERE generation_id=? AND query_id=?")) {
            statement.setObject(1, generation.value());
            statement.setObject(2, query.value());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("releaseReaderPin failed", failure);
        }
    }

    @Override
    public void releaseReaderPins(QueryId query) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "DELETE FROM materialization.reader_pins WHERE query_id=?")) {
            statement.setString(1, query.value());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("releaseReaderPins failed", failure);
        }
    }

    @Override
    public void renewReaderPins(QueryId query, Instant expiresAt, Instant heartbeatAt) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE materialization.reader_pins SET "
                             + "heartbeat_at=GREATEST(heartbeat_at, ?), "
                             + "expires_at=GREATEST(expires_at, ?) WHERE query_id=?")) {
            setInstant(statement, 1, heartbeatAt);
            setInstant(statement, 2, expiresAt);
            statement.setString(3, query.value());
            statement.executeUpdate();
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("renewReaderPins failed", failure);
        }
    }

    @Override
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

    @Override
    public List<ReaderPin> findReaderPins(GenerationId generation) {
        String sql = "SELECT generation_id, query_id, owner, expires_at, heartbeat_at "
                + "FROM materialization.reader_pins WHERE generation_id=? "
                + "ORDER BY expires_at, query_id";
        return queryList("findReaderPins", sql, statement ->
                statement.setObject(1, generation.value()), result -> new ReaderPin(
                        new GenerationId(result.getObject("generation_id", UUID.class)),
                        new QueryId(result.getString("query_id")),
                        result.getString("owner"),
                        getInstant(result, "expires_at"),
                        getInstant(result, "heartbeat_at")));
    }

    @Override
    public List<Generation> findCollectableGenerations(Instant now, Instant eligibleBefore,
            int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        String sql = "SELECT " + GENERATION_COLUMNS + " FROM materialization.generations g "
                + "JOIN materialization.attempts a ON a.attempt_id=g.producing_attempt_id "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE ("
                + "  (g.state='RETIRED' AND g.published_at<=?) "
                + "  OR (g.state='CANDIDATE' AND a.state IN ('FAILED','ABANDONED') "
                + "      AND g.created_at<=?) "
                + "  OR (g.state='COMMITTED' AND a.state='SUPERSEDED' AND g.created_at<=?)"
                + ") "
                + "AND NOT EXISTS (SELECT 1 FROM materialization.reader_pins p "
                + "  WHERE p.generation_id=g.generation_id AND p.expires_at>?) "
                + "AND NOT EXISTS (SELECT 1 FROM materialization.generations d "
                + "  WHERE d.base_generation_id=g.generation_id "
                + "  AND d.state IN ('CANDIDATE','COMMITTED','PUBLISHED','QUARANTINED')) "
                + "AND NOT EXISTS (SELECT 1 FROM materialization.objects o "
                + "  WHERE o.tenant_id=g.tenant_id AND o.object_name=g.object_name "
                + "  AND o.published_generation_id=g.generation_id) "
                + "ORDER BY COALESCE(g.published_at, g.created_at), g.generation_id LIMIT ?";
        return queryList("findCollectableGenerations", sql, statement -> {
            setInstant(statement, 1, eligibleBefore);
            setInstant(statement, 2, eligibleBefore);
            setInstant(statement, 3, eligibleBefore);
            setInstant(statement, 4, now);
            statement.setInt(5, limit);
        }, this::mapGeneration);
    }

    @Override
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

    @Override
    public void updateGcDecision(UUID entryId, GcLedgerEntry.GcState state, Instant now) {
        if (state == GcLedgerEntry.GcState.PENDING) {
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

    private PublicationResult replayPublication(PublishCommand command, CommandRecord receipt) {
        JsonNode reference = parseCommandReference(receipt.resultReference());
        String objectName = reference.path("object").asText();
        MaterializationId object = MaterializationId.parse(objectName);
        GenerationId generationId = new GenerationId(
                UUID.fromString(reference.path("generation").asText()));
        MaterializationObject header = findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        Generation published = findGeneration(generationId)
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
                        + "a.expires_at > clock_timestamp() AS anchor_live "
                        + "FROM distributed_lease l CROSS JOIN execution_anchor a "
                        + "WHERE l.lease_key='coordinator_leader' AND a.execution_id=?")) {
            statement.setString(1, fence.executionAnchor());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()
                        || result.getLong("epoch") != fence.controlEpoch()
                        || !result.getBoolean("leader_live")
                        || !result.getBoolean("anchor_live")) {
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

    private int insertObjectIfAbsent(Connection connection, MaterializationObject object)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.objects (tenant_id, object_name, object_kind, "
                        + "incarnation_id, definition_revision, revision, published_generation_id, "
                        + "state, definer_principal, security_mode, refresh_mode, "
                        + "incremental_strategy, created_at, updated_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                        + "ON CONFLICT (tenant_id, object_name) DO NOTHING")) {
            bindObjectId(statement, 1, object.id());
            statement.setString(3, object.kind().name());
            statement.setObject(4, object.incarnation().value());
            statement.setLong(5, object.definitionRevision());
            statement.setLong(6, object.revision());
            statement.setNull(7, java.sql.Types.OTHER);
            statement.setString(8, objectStateName(object.state()));
            statement.setString(9, object.definer().name());
            statement.setString(10, object.securityMode().name());
            statement.setString(11, object.refreshMode().name());
            if (object.incrementalStrategy().isPresent()) {
                statement.setString(12, object.incrementalStrategy().orElseThrow().value());
            } else {
                statement.setNull(12, java.sql.Types.VARCHAR);
            }
            setInstant(statement, 13, object.createdAt());
            setInstant(statement, 14, object.updatedAt());
            return statement.executeUpdate();
        }
    }

    private void insertDefinition(Connection connection, MaterializationId object,
            DefinitionRevision definition) throws SQLException {
        requireStorableFingerprint(definition);
        lockDependencyGraph(connection, object.tenant());
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.definition_revisions (tenant_id, object_name, revision, "
                        + "definition_sql, canonical_sql, plan_fingerprint, resolved, output_schema, "
                        + "output_schema_hash, sources, policy_snapshot, security_mode, "
                        + "definer_principal, session_properties, created_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            bindObjectId(statement, 1, object);
            statement.setLong(3, definition.revision());
            statement.setString(4, definition.sql());
            statement.setString(5, definition.binding().canonicalSql());
            statement.setString(6, definition.binding().planFingerprint());
            statement.setBoolean(7, true);
            setJson(statement, 8, MaterializationJson.outputSchema(definition.outputSchema()));
            statement.setString(9, definition.outputSchema().fingerprint());
            setJson(statement, 10, MaterializationJson.sources(definition.sources()));
            setJson(statement, 11, MaterializationJson.policy(definition.policySnapshot()));
            statement.setString(12, definition.securityMode().name());
            statement.setString(13, definition.definer().name());
            setJson(statement, 14, MaterializationJson.sessionProperties(
                    definition.sessionProperties()));
            setInstant(statement, 15, definition.createdAt());
            statement.executeUpdate();
        }
        for (Dependency dependency : definition.dependencies()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO materialization.dependencies (tenant_id, downstream_object, "
                            + "upstream_object, downstream_definition_revision, dependency_kind) "
                            + "VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING")) {
                statement.setString(1, object.tenant().value());
                statement.setString(2, object.name().toString());
                statement.setString(3, dependency.upstream().name().toString());
                statement.setLong(4, definition.revision());
                statement.setString(5, dependency.kind().name());
                statement.executeUpdate();
            }
        }
        requireAcyclicDefinition(connection, object);
    }

    /**
     * Serializes definition-graph writes per tenant so two concurrent redefines
     * cannot each miss the other's new edge and commit a cycle.
     */
    private static void lockDependencyGraph(Connection connection, TenantId tenant)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_advisory_xact_lock(hashtext(?))")) {
            statement.setString(1, "materialization-graph:" + tenant.value());
            statement.execute();
        }
    }

    /**
     * Rejects a definition whose upstream closure reaches the object itself.
     *
     * <p>Only the latest recorded revision of each downstream object counts, so
     * superseded edges never block a legitimate redefine.
     */
    private static void requireAcyclicDefinition(Connection connection, MaterializationId object)
            throws SQLException {
        String currentRevision = "(SELECT MAX(r.revision) FROM materialization.definition_revisions r "
                + "WHERE r.tenant_id=d.tenant_id AND r.object_name=d.downstream_object)";
        String sql = "WITH RECURSIVE upstream(object_name) AS ("
                + " SELECT d.upstream_object FROM materialization.dependencies d"
                + " WHERE d.tenant_id=? AND d.downstream_object=?"
                + " AND d.downstream_definition_revision=" + currentRevision
                + " UNION"
                + " SELECT d.upstream_object FROM materialization.dependencies d"
                + " JOIN upstream u ON d.downstream_object=u.object_name"
                + " WHERE d.tenant_id=?"
                + " AND d.downstream_definition_revision=" + currentRevision + ")"
                + " SELECT 1 FROM upstream WHERE object_name=? LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, object.tenant().value());
            statement.setString(2, object.name().toString());
            statement.setString(3, object.tenant().value());
            statement.setString(4, object.name().toString());
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    throw MaterializationException.dependencyConflict(
                            "definition of " + object.canonicalName()
                                    + " would create a dependency cycle");
                }
            }
        }
    }

    private void insertSchedule(Connection connection, Schedule schedule) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.schedules (tenant_id, object_name, target_lag_ms, "
                        + "next_due_at, last_success_at, consecutive_failures, state, lease_owner, "
                        + "lease_expires_at, version) VALUES (?,?,?,?,?,?,?,?,?,?)")) {
            bindObjectId(statement, 1, schedule.object());
            if (schedule.targetLag().isPresent()) {
                statement.setLong(3, schedule.targetLag().orElseThrow().value().toMillis());
            } else {
                statement.setNull(3, java.sql.Types.BIGINT);
            }
            setNullableInstant(statement, 4, schedule.nextDueAt().orElse(null));
            setNullableInstant(statement, 5, schedule.lastSuccessAt().orElse(null));
            statement.setInt(6, schedule.consecutiveFailures());
            statement.setString(7, schedule.state().name());
            statement.setNull(8, java.sql.Types.VARCHAR);
            statement.setNull(9, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
            statement.setLong(10, schedule.version());
            statement.executeUpdate();
        }
    }

    private void insertSourcePin(Connection connection, SourcePin pin) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.source_pins (attempt_id, connector_id, catalog_name, "
                        + "schema_name, table_name, snapshot_token, schema_fingerprint, "
                        + "policy_fingerprint, observed_at, expires_at, state) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
            statement.setObject(1, pin.attempt().value());
            statement.setString(2, pin.source().connectorId());
            statement.setString(3, pin.source().catalog());
            statement.setString(4, pin.source().schema());
            statement.setString(5, pin.source().table());
            statement.setString(6, pin.snapshot().snapshotToken());
            statement.setString(7, pin.snapshot().schemaFingerprint());
            statement.setString(8, pin.snapshot().policyFingerprint());
            setInstant(statement, 9, pin.snapshot().observedAt());
            setInstant(statement, 10, pin.expiresAt());
            statement.setString(11, pin.state().name());
            statement.executeUpdate();
        }
    }

    private void insertCapability(Connection connection, MaterializationWriteCapability capability)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.write_capabilities (attempt_id, tenant_id, object_name, "
                        + "incarnation_id, definition_revision, generation_id, candidate_catalog, "
                        + "candidate_schema, candidate_table, source_vector_hash, definer_principal, "
                        + "control_epoch, issued_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            statement.setObject(1, capability.attempt().value());
            bindObjectId(statement, 2, capability.object());
            statement.setObject(4, capability.incarnation().value());
            statement.setLong(5, capability.definitionRevision());
            statement.setObject(6, capability.generation().value());
            statement.setString(7, capability.candidateTable().catalog());
            statement.setString(8, capability.candidateTable().schema());
            statement.setString(9, capability.candidateTable().table());
            statement.setString(10, capability.sourceVectorHash());
            statement.setString(11, capability.definer().name());
            statement.setLong(12, capability.controlEpoch());
            setInstant(statement, 13, capability.issuedAt());
            setInstant(statement, 14, capability.expiresAt());
            statement.executeUpdate();
        }
    }

    private void insertEvent(Connection connection, MaterializationEvent event) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.events (event_id, event_type, tenant_id, object_name, "
                        + "command_id, attempt_id, generation_id, query_id, requesting_principal, "
                        + "effective_principal, detail, occurred_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")) {
            statement.setObject(1, UUID.fromString(event.eventId()));
            statement.setString(2, event.eventType().name());
            bindObjectId(statement, 3, event.object());
            if (event.command().isPresent()) {
                statement.setString(5, event.command().orElseThrow().value());
            } else {
                statement.setNull(5, java.sql.Types.VARCHAR);
            }
            if (event.attempt().isPresent()) {
                statement.setObject(6, event.attempt().orElseThrow().value());
            } else {
                statement.setNull(6, java.sql.Types.OTHER);
            }
            if (event.generation().isPresent()) {
                statement.setObject(7, event.generation().orElseThrow().value());
            } else {
                statement.setNull(7, java.sql.Types.OTHER);
            }
            if (event.query().isPresent()) {
                statement.setObject(8, event.query().orElseThrow().value());
            } else {
                statement.setNull(8, java.sql.Types.OTHER);
            }
            statement.setString(9, event.requesting().name());
            statement.setString(10, event.effective().name());
            statement.setString(11, event.detail());
            setInstant(statement, 12, event.occurredAt());
            statement.executeUpdate();
        }
    }

    private void recordCommand(Connection connection, CommandRecord record) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.commands (tenant_id, command_id, result_reference, "
                        + "created_at) VALUES (?,?,?,?) ON CONFLICT (tenant_id, command_id) DO NOTHING")) {
            statement.setString(1, record.tenant().value());
            statement.setString(2, record.commandId().value());
            statement.setString(3, record.resultReference());
            setInstant(statement, 4, record.createdAt());
            statement.executeUpdate();
        }
    }

    /**
     * Records the terminal publication result, replacing the admission receipt a
     * create or refresh command wrote before execution started.
     */
    private void recordPublicationCommand(Connection connection, CommandRecord record)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.commands (tenant_id, command_id, result_reference, "
                        + "created_at) VALUES (?,?,?,?) ON CONFLICT (tenant_id, command_id) "
                        + "DO UPDATE SET result_reference=EXCLUDED.result_reference")) {
            statement.setString(1, record.tenant().value());
            statement.setString(2, record.commandId().value());
            statement.setString(3, record.resultReference());
            setInstant(statement, 4, record.createdAt());
            statement.executeUpdate();
        }
    }

    private static boolean isPublicationReceipt(CommandRecord receipt) {
        JsonNode reference = MaterializationJson.tree(receipt.resultReference());
        return reference != null && reference.path("object").isTextual()
                && reference.path("generation").isTextual();
    }

    private long currentDefinitionRevision(Connection connection, MaterializationId object)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT definition_revision FROM materialization.objects "
                        + "WHERE tenant_id=? AND object_name=? FOR UPDATE")) {
            bindObjectId(statement, 1, object);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw MaterializationException.notFound(object);
                }
                return result.getLong("definition_revision");
            }
        }
    }

    private MaterializationObject mapObject(ResultSet result) throws SQLException {
        UUID published = result.getObject("published_generation_id", UUID.class);
        String strategy = result.getString("incremental_strategy");
        return new MaterializationObject(
                new MaterializationId(new TenantId(result.getString("tenant_id")),
                        QualifiedName.parse(result.getString("object_name"))),
                MaterializationKind.valueOf(result.getString("object_kind")),
                new IncarnationId(result.getObject("incarnation_id", UUID.class)),
                result.getLong("definition_revision"),
                result.getLong("revision"),
                published == null ? Optional.empty() : Optional.of(new GenerationId(published)),
                objectState(result.getString("state")),
                new Principal(result.getString("definer_principal")),
                SecurityMode.valueOf(result.getString("security_mode")),
                RefreshMode.valueOf(result.getString("refresh_mode")),
                strategy == null ? Optional.empty() : Optional.of(new StrategyId(strategy)),
                getInstant(result, "created_at"),
                getInstant(result, "updated_at"));
    }

    private DefinitionRevision mapDefinition(MaterializationId object, ResultSet result)
            throws SQLException {
        if (!result.getBoolean("resolved")) {
            throw MaterializationException.invalidDefinition(
                    "definition " + object.canonicalName() + " revision "
                            + result.getLong("revision")
                            + " is unresolved; a full rebuild is required before execution");
        }
        JsonNode schemaNode = MaterializationJson.tree(
                result.getString("output_schema"));
        var outputSchema = MaterializationJson.outputSchema(schemaNode);
        String expectedHash = result.getString("output_schema_hash");
        if (expectedHash == null || !outputSchema.fingerprint().equals(expectedHash.trim())) {
            throw MaterializationJson.corrupt(
                    "persisted output schema hash does not match its content");
        }
        var sources = MaterializationJson.sources(
                MaterializationJson.tree(result.getString("sources")));
        var policy = MaterializationJson.policy(
                MaterializationJson.tree(result.getString("policy_snapshot")));
        long revision = result.getLong("revision");
        List<Dependency> dependencies = new ArrayList<>();
        try (PreparedStatement statement = result.getStatement().getConnection().prepareStatement(
                "SELECT upstream_object, dependency_kind FROM materialization.dependencies "
                        + "WHERE tenant_id=? AND downstream_object=? "
                        + "AND downstream_definition_revision=?")) {
            statement.setString(1, object.tenant().value());
            statement.setString(2, object.name().toString());
            statement.setLong(3, revision);
            try (ResultSet edges = statement.executeQuery()) {
                while (edges.next()) {
                    dependencies.add(new Dependency(
                            new MaterializationId(object.tenant(),
                                    QualifiedName.parse(edges.getString("upstream_object"))),
                            object, revision,
                            DependencyKind.valueOf(edges.getString("dependency_kind"))));
                }
            }
        }
        return new DefinitionRevision(object, revision, result.getString("definition_sql"),
                new Binding(result.getString("canonical_sql"),
                        result.getString("plan_fingerprint").trim()),
                outputSchema, sources, java.util.Set.copyOf(dependencies), policy,
                SecurityMode.valueOf(result.getString("security_mode")),
                new Principal(result.getString("definer_principal")),
                MaterializationJson.sessionProperties(
                        MaterializationJson.tree(result.getString("session_properties"))),
                getInstant(result, "created_at"));
    }

    private Generation mapGeneration(ResultSet result) throws SQLException {
        UUID base = result.getObject("base_generation_id", UUID.class);
        Instant publishedAt = getInstant(result, "published_at");
        SourceVector sourceVector = MaterializationJson.sourceVector(
                MaterializationJson.tree(result.getString("source_vector")));
        String storedHash = result.getString("source_vector_hash");
        if (storedHash == null || !sourceVector.canonicalHash().equals(storedHash.trim())) {
            throw MaterializationJson.corrupt(
                    "persisted source vector hash does not match its content");
        }
        return new Generation(
                new GenerationId(result.getObject("generation_id", UUID.class)),
                objectId(result),
                new IncarnationId(result.getObject("incarnation_id", UUID.class)),
                result.getLong("definition_revision"),
                new AttemptId(result.getObject("producing_attempt_id", UUID.class)),
                base == null ? Optional.empty() : Optional.of(new GenerationId(base)),
                new PhysicalTableRef(result.getString("physical_catalog"),
                        result.getString("physical_schema"), result.getString("physical_table")),
                refreshKind(result.getString("refresh_kind")),
                sourceVector,
                evidence(result),
                generationState(result.getString("state")),
                getInstant(result, "created_at"),
                publishedAt == null ? Optional.empty() : Optional.of(publishedAt));
    }

    private RefreshAttempt mapAttempt(ResultSet result) throws SQLException {
        UUID base = result.getObject("base_generation_id", UUID.class);
        String error = result.getString("error_code");
        SourceVector sourceVector = MaterializationJson.sourceVector(
                MaterializationJson.tree(result.getString("source_vector")));
        String storedHash = result.getString("source_vector_hash");
        if (storedHash == null || !sourceVector.canonicalHash().equals(storedHash.trim())) {
            throw MaterializationJson.corrupt(
                    "persisted attempt source vector hash does not match its content");
        }
        return new RefreshAttempt(
                new AttemptId(result.getObject("attempt_id", UUID.class)),
                objectId(result),
                new IncarnationId(result.getObject("incarnation_id", UUID.class)),
                result.getLong("definition_revision"),
                result.getLong("base_object_revision"),
                base == null ? Optional.empty() : Optional.of(new GenerationId(base)),
                refreshKind(result.getString("refresh_kind")),
                sourceVector,
                TriggerKind.valueOf(result.getString("trigger_kind")),
                new Principal(result.getString("requesting_principal")),
                new Principal(result.getString("definer_principal")),
                new ExecutionFence(result.getString("execution_anchor"),
                        result.getLong("control_epoch"), getInstant(result, "lease_expires_at")),
                new PhysicalTableRef(result.getString("candidate_catalog"),
                        result.getString("candidate_schema"), result.getString("candidate_table")),
                attemptState(result.getString("state")),
                evidence(result),
                error == null ? Optional.empty() : Optional.of(error),
                getInstant(result, "created_at"),
                getInstant(result, "updated_at"));
    }

    private ReclaimableSourcePin mapReclaimableSourcePin(ResultSet result) throws SQLException {
        String connectorId = result.getString("connector_id");
        return new ReclaimableSourcePin(
                new TenantId(result.getString("tenant_id")),
                new AttemptId(result.getObject("attempt_id", UUID.class)),
                new SourceRef(connectorId, result.getString("catalog_name"),
                        result.getString("schema_name"), result.getString("table_name")),
                new SourceSnapshotRef(connectorId, result.getString("snapshot_token"),
                        result.getString("schema_fingerprint"),
                        result.getString("policy_fingerprint"),
                        getInstant(result, "observed_at"), true));
    }

    private MaterializationWriteCapability mapCapability(ResultSet result) throws SQLException {
        return new MaterializationWriteCapability(
                objectId(result),
                new IncarnationId(result.getObject("incarnation_id", UUID.class)),
                result.getLong("definition_revision"),
                new AttemptId(result.getObject("attempt_id", UUID.class)),
                new PhysicalTableRef(result.getString("candidate_catalog"),
                        result.getString("candidate_schema"), result.getString("candidate_table")),
                new GenerationId(result.getObject("generation_id", UUID.class)),
                result.getString("source_vector_hash").trim(),
                new Principal(result.getString("definer_principal")),
                result.getLong("control_epoch"),
                getInstant(result, "issued_at"),
                getInstant(result, "expires_at"));
    }

    private Schedule mapSchedule(ResultSet result) throws SQLException {
        long targetLag = result.getLong("target_lag_ms");
        Optional<TargetLag> lag = result.wasNull()
                ? Optional.empty() : Optional.of(new TargetLag(Duration.ofMillis(targetLag)));
        Instant nextDue = getInstant(result, "next_due_at");
        Instant lastSuccess = getInstant(result, "last_success_at");
        String leaseOwner = result.getString("lease_owner");
        return new Schedule(
                objectId(result),
                lag,
                nextDue == null ? Optional.empty() : Optional.of(nextDue),
                lastSuccess == null ? Optional.empty() : Optional.of(lastSuccess),
                result.getInt("consecutive_failures"),
                ScheduleState.valueOf(result.getString("state")),
                leaseOwner == null ? Optional.empty()
                        : Optional.of(new ScheduleLease(leaseOwner,
                                getInstant(result, "lease_expires_at"))),
                result.getLong("version"));
    }

    private static Optional<CommitEvidence> evidence(ResultSet result) throws SQLException {
        String connector = result.getString("evidence_connector_id");
        if (connector == null) {
            return Optional.empty();
        }
        return Optional.of(new CommitEvidence(
                connector,
                result.getString("evidence_commit_fingerprint"),
                result.getString("evidence_manifest_hash"),
                result.getString("evidence_format_version"),
                getInstant(result, "evidence_committed_at")));
    }

    private static MaterializationId objectId(ResultSet result) throws SQLException {
        return new MaterializationId(new TenantId(result.getString("tenant_id")),
                QualifiedName.parse(result.getString("object_name")));
    }

    private static ObjectState objectState(String value) {
        return switch (value) {
            case "CREATING" -> ObjectState.Creating.INSTANCE;
            case "ACTIVE" -> ObjectState.Active.INSTANCE;
            case "SUSPENDED" -> ObjectState.Suspended.INSTANCE;
            case "FAILED" -> ObjectState.Failed.INSTANCE;
            case "DROPPING" -> ObjectState.Dropping.INSTANCE;
            case "DROPPED" -> ObjectState.Dropped.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown object state " + value);
        };
    }

    private static AttemptState attemptState(String value) {
        return switch (value) {
            case "PREPARING" -> AttemptState.Preparing.INSTANCE;
            case "BUILDING" -> AttemptState.Building.INSTANCE;
            case "COMMITTED" -> AttemptState.Committed.INSTANCE;
            case "PUBLISHED" -> AttemptState.Published.INSTANCE;
            case "FAILED" -> AttemptState.Failed.INSTANCE;
            case "ABANDONED" -> AttemptState.Abandoned.INSTANCE;
            case "QUARANTINED" -> AttemptState.Quarantined.INSTANCE;
            case "SUPERSEDED" -> AttemptState.Superseded.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown attempt state " + value);
        };
    }

    private static GenerationState generationState(String value) {
        return switch (value) {
            case "CANDIDATE" -> GenerationState.Candidate.INSTANCE;
            case "COMMITTED" -> GenerationState.Committed.INSTANCE;
            case "PUBLISHED" -> GenerationState.Published.INSTANCE;
            case "RETIRED" -> GenerationState.Retired.INSTANCE;
            case "QUARANTINED" -> GenerationState.Quarantined.INSTANCE;
            case "COLLECTED" -> GenerationState.Collected.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown generation state " + value);
        };
    }

    private static RefreshKind refreshKind(String value) {
        return switch (value) {
            case "FULL" -> RefreshKind.Full.INSTANCE;
            case "INCREMENTAL_APPEND_ONLY" -> RefreshKind.IncrementalAppendOnly.INSTANCE;
            case "INCREMENTAL_PARTITION_REPLACE" -> RefreshKind.IncrementalPartitionReplace.INSTANCE;
            case "INCREMENTAL_AFFECTED_KEY" -> RefreshKind.IncrementalAffectedKey.INSTANCE;
            case "INCREMENTAL_CDC" -> RefreshKind.IncrementalCdc.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown refresh kind " + value);
        };
    }

    private static String refreshKindName(RefreshKind kind) {
        if (kind == RefreshKind.Full.INSTANCE) {
            return "FULL";
        }
        if (kind == RefreshKind.IncrementalAppendOnly.INSTANCE) {
            return "INCREMENTAL_APPEND_ONLY";
        }
        if (kind == RefreshKind.IncrementalPartitionReplace.INSTANCE) {
            return "INCREMENTAL_PARTITION_REPLACE";
        }
        if (kind == RefreshKind.IncrementalAffectedKey.INSTANCE) {
            return "INCREMENTAL_AFFECTED_KEY";
        }
        return "INCREMENTAL_CDC";
    }

    private static void requireMatchingObject(MaterializationObject object,
            DefinitionRevision definition) {
        if (!object.id().equals(definition.object())) {
            throw MaterializationException.invalidDefinition(
                    "definition does not belong to the creating object");
        }
        if (!definition.definer().equals(object.definer())
                || definition.securityMode() != object.securityMode()) {
            throw MaterializationException.invalidDefinition(
                    "definition security context must match the object header");
        }
    }

    private static void requireMatchingObject(MaterializationObject object, Schedule schedule) {
        if (!object.id().equals(schedule.object())) {
            throw MaterializationException.invalidDefinition(
                    "schedule does not belong to the creating object");
        }
    }

    private static void requireStorableFingerprint(DefinitionRevision definition) {
        if (!definition.binding().planFingerprint().matches("[0-9a-f]{64}")) {
            throw MaterializationException.invalidDefinition(
                    "plan fingerprint must be a lowercase SHA-256 digest");
        }
    }

    private static void requireCapability(RefreshAttempt attempt, Generation candidate,
            MaterializationWriteCapability capability) {
        if (!attempt.id().equals(capability.attempt())
                || !attempt.object().equals(capability.object())
                || !attempt.incarnation().equals(capability.incarnation())
                || attempt.definitionRevision() != capability.definitionRevision()
                || !candidate.id().equals(capability.generation())
                || !attempt.candidateTable().sameTable(capability.candidateTable())
                || !candidate.physicalTable().sameTable(capability.candidateTable())
                || !attempt.sourceVector().canonicalHash().equals(capability.sourceVectorHash())
                || !attempt.definer().equals(capability.definer())
                || attempt.fence().controlEpoch() != capability.controlEpoch()) {
            throw MaterializationException.writeCapabilityInvalid(
                    "write capability does not describe the reserved attempt and candidate");
        }
    }

    private static void bindObjectId(PreparedStatement statement, int index,
            MaterializationId object) throws SQLException {
        statement.setString(index, object.tenant().value());
        statement.setString(index + 1, object.name().toString());
    }

    private static void bindNullableUuid(PreparedStatement statement, int index, UUID value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.OTHER);
        } else {
            statement.setObject(index, value);
        }
    }

    private static Generation publishedView(
            Generation generation, CommitEvidence evidence, Instant publishedAt) {
        return new Generation(generation.id(), generation.object(), generation.incarnation(),
                generation.definitionRevision(), generation.producingAttempt(),
                generation.baseGeneration(), generation.physicalTable(), generation.refreshKind(),
                generation.sourceVector(), Optional.of(evidence),
                GenerationState.Published.INSTANCE, generation.createdAt(),
                Optional.of(publishedAt));
    }

    private static String objectReference(MaterializationId object, long revision, String action) {
        var reference = MaterializationJson.mapper().createObjectNode();
        reference.put("object", object.canonicalName());
        reference.put("revision", revision);
        reference.put("action", action);
        return reference.toString();
    }

    private static String publicationReference(MaterializationId object, GenerationId generation,
            long revision) {
        var reference = MaterializationJson.mapper().createObjectNode();
        reference.put("object", object.canonicalName());
        reference.put("generation", generation.value().toString());
        reference.put("revision", revision);
        return reference.toString();
    }

    private static String objectStateName(ObjectState state) {
        if (state == ObjectState.Creating.INSTANCE) {
            return "CREATING";
        }
        if (state == ObjectState.Active.INSTANCE) {
            return "ACTIVE";
        }
        if (state == ObjectState.Suspended.INSTANCE) {
            return "SUSPENDED";
        }
        if (state == ObjectState.Failed.INSTANCE) {
            return "FAILED";
        }
        if (state == ObjectState.Dropping.INSTANCE) {
            return "DROPPING";
        }
        return "DROPPED";
    }

    private static String attemptStateName(AttemptState state) {
        if (state == AttemptState.Preparing.INSTANCE) {
            return "PREPARING";
        }
        if (state == AttemptState.Building.INSTANCE) {
            return "BUILDING";
        }
        if (state == AttemptState.Committed.INSTANCE) {
            return "COMMITTED";
        }
        if (state == AttemptState.Published.INSTANCE) {
            return "PUBLISHED";
        }
        if (state == AttemptState.Failed.INSTANCE) {
            return "FAILED";
        }
        if (state == AttemptState.Abandoned.INSTANCE) {
            return "ABANDONED";
        }
        if (state == AttemptState.Quarantined.INSTANCE) {
            return "QUARANTINED";
        }
        return "SUPERSEDED";
    }

    private static String generationStateName(GenerationState state) {
        if (state == GenerationState.Candidate.INSTANCE) {
            return "CANDIDATE";
        }
        if (state == GenerationState.Committed.INSTANCE) {
            return "COMMITTED";
        }
        if (state == GenerationState.Published.INSTANCE) {
            return "PUBLISHED";
        }
        if (state == GenerationState.Retired.INSTANCE) {
            return "RETIRED";
        }
        if (state == GenerationState.Quarantined.INSTANCE) {
            return "QUARANTINED";
        }
        return "COLLECTED";
    }

    private static JsonNode parseCommandReference(String reference) {
        JsonNode node = MaterializationJson.tree(reference);
        if (node == null || !node.path("object").isTextual()
                || !node.path("generation").isTextual()) {
            throw MaterializationJson.corrupt("recorded publication command is malformed");
        }
        return node;
    }

    private <T> Optional<T> queryOne(String action, String sql,
            org.pangrova.metadata.store.JdbcSupport.SqlConsumer binder,
            org.pangrova.metadata.store.JdbcSupport.SqlFunction<ResultSet, T> mapper) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.accept(statement);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapper.apply(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(action + " failed", failure);
        }
    }

    private <T> List<T> queryList(String action, String sql,
            org.pangrova.metadata.store.JdbcSupport.SqlConsumer binder,
            org.pangrova.metadata.store.JdbcSupport.SqlFunction<ResultSet, T> mapper) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.accept(statement);
            try (ResultSet result = statement.executeQuery()) {
                List<T> values = new ArrayList<>();
                while (result.next()) {
                    values.add(mapper.apply(result));
                }
                return values;
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(action + " failed", failure);
        }
    }

    private <T> T inTransaction(String action,
            org.pangrova.metadata.store.JdbcSupport.SqlFunction<Connection, T> work) {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = work.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException failure) {
                rollback(connection, failure);
                throw MaterializationException.repositoryFailure(action + " failed", failure);
            } catch (RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure(action + " failed", failure);
        }
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }
}
