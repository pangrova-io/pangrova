package org.pangrova.persistence.materialization.postgresql;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.enums.DependencyKind;
import org.pangrova.materialization.api.domain.GcLedgerEntry;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.materialization.api.domain.ReaderId;
import org.pangrova.materialization.api.enums.ReaderKind;
import org.pangrova.materialization.api.domain.ReaderPin;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.CommandRecord;
import org.pangrova.materialization.api.port.GcHistoryEntry;
import org.pangrova.materialization.api.port.ReclaimableSourcePin;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/**
 * Read-side JDBC queries of the materialization repository.
 *
 * <p>Queries never take locks and never mutate state; callers that need a
 * consistent snapshot ask the repository for the specific aggregate they act on.
 */
final class JdbcMaterializationQueries {

    private final JdbcMaterializationSql jdbc;

    JdbcMaterializationQueries(JdbcMaterializationSql jdbc) {
        if (jdbc == null) {
            throw new IllegalArgumentException("jdbc must not be null");
        }
        this.jdbc = jdbc;
    }

    public Optional<MaterializationObject> findObject(MaterializationId object) {
        String sql = "SELECT " + JdbcMaterializationRows.OBJECT_COLUMNS + " FROM materialization.objects "
                + "WHERE tenant_id=? AND object_name=?";
        return jdbc.queryOne("findObject", sql, statement -> {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
        }, JdbcMaterializationRows::mapObject);
    }

    public Optional<DefinitionRevision> findDefinition(MaterializationId object, long revision) {
        String sql = "SELECT revision, definition_sql, canonical_sql, plan_fingerprint, resolved, "
                + "output_schema, output_schema_hash, sources, policy_snapshot, security_mode, "
                + "definer_principal, definer_groups, session_properties, created_at "
                + "FROM materialization.definition_revisions "
                + "WHERE tenant_id=? AND object_name=? AND revision=?";
        return jdbc.queryOne("findDefinition", sql, statement -> {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
            statement.setLong(3, revision);
        }, result -> JdbcMaterializationRows.mapDefinition(object, result));
    }

    public List<MaterializationId> findLiveDependents(MaterializationId upstream) {
        String sql = "SELECT d.downstream_object FROM materialization.dependencies d "
                + "JOIN materialization.objects o ON o.tenant_id=d.tenant_id "
                + "AND o.object_name=d.downstream_object "
                + "WHERE d.tenant_id=? AND d.upstream_object=? "
                + "AND d.downstream_definition_revision=o.definition_revision "
                + "AND o.state NOT IN ('DROPPING','DROPPED') "
                + "ORDER BY d.downstream_object";
        return jdbc.queryList("findLiveDependents", sql, statement -> {
            statement.setString(1, upstream.tenant().value());
            statement.setString(2, upstream.name().toString());
        }, result -> new MaterializationId(upstream.tenant(),
                QualifiedName.parse(result.getString("downstream_object"))));
    }

    public List<MaterializationObject> listObjects(TenantId tenant) {
        String sql = "SELECT " + JdbcMaterializationRows.OBJECT_COLUMNS + " FROM materialization.objects "
                + "WHERE tenant_id=? ORDER BY object_name";
        return jdbc.queryList("listObjects", sql,
                statement -> statement.setString(1, tenant.value()), JdbcMaterializationRows::mapObject);
    }

    public List<RefreshAttempt> findAttempts(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        requireLimit(limit);
        boolean scoped = object.isPresent();
        String sql = "SELECT " + JdbcMaterializationRows.ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.tenant_id=?" + (scoped ? " AND a.object_name=?" : "")
                + " ORDER BY a.created_at DESC, a.attempt_id DESC LIMIT ?";
        return jdbc.queryList("findAttempts", sql, statement -> {
            statement.setString(1, tenant.value());
            int index = 2;
            if (scoped) {
                statement.setString(index++, object.orElseThrow().name().toString());
            }
            statement.setInt(index, limit);
        }, JdbcMaterializationRows::mapAttempt);
    }

    public List<Dependency> findDependencies(TenantId tenant,
            Optional<MaterializationId> object) {
        boolean scoped = object.isPresent();
        String sql = "SELECT tenant_id, downstream_object, upstream_object, "
                + "downstream_definition_revision, dependency_kind "
                + "FROM materialization.dependencies WHERE tenant_id=?"
                + (scoped ? " AND (downstream_object=? OR upstream_object=?)" : "")
                + " ORDER BY downstream_object, downstream_definition_revision, upstream_object";
        return jdbc.queryList("findDependencies", sql, statement -> {
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
        return jdbc.queryList("findGcHistory", sql, statement -> {
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

    public Optional<Generation> findGeneration(GenerationId generation) {
        String sql = "SELECT " + JdbcMaterializationRows.GENERATION_COLUMNS + " FROM materialization.generations g "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE g.generation_id=?";
        return jdbc.queryOne("findGeneration", sql, statement ->
                statement.setObject(1, generation.value()), JdbcMaterializationRows::mapGeneration);
    }

    public Optional<Generation> findPublishedGeneration(MaterializationId object) {
        String sql = "SELECT " + JdbcMaterializationRows.GENERATION_COLUMNS + " FROM materialization.generations g "
                + "JOIN materialization.objects o ON o.tenant_id=g.tenant_id "
                + "AND o.object_name=g.object_name AND o.published_generation_id=g.generation_id "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE o.tenant_id=? AND o.object_name=?";
        return jdbc.queryOne("findPublishedGeneration", sql, statement -> {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
        }, JdbcMaterializationRows::mapGeneration);
    }

    public List<Generation> findGenerations(MaterializationId object) {
        String sql = "SELECT " + JdbcMaterializationRows.GENERATION_COLUMNS + " FROM materialization.generations g "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE g.tenant_id=? AND g.object_name=? ORDER BY g.created_at, g.generation_id";
        return jdbc.queryList("findGenerations", sql, statement -> {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
        }, JdbcMaterializationRows::mapGeneration);
    }

    public Optional<RefreshAttempt> findAttempt(AttemptId attempt) {
        String sql = "SELECT " + JdbcMaterializationRows.ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.attempt_id=?";
        return jdbc.queryOne("findAttempt", sql, statement ->
                statement.setObject(1, attempt.value()), JdbcMaterializationRows::mapAttempt);
    }

    public Optional<RefreshAttempt> findActiveAttempt(MaterializationId object) {
        String sql = "SELECT " + JdbcMaterializationRows.ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.tenant_id=? AND a.object_name=? "
                + "AND a.state IN ('PREPARING','BUILDING','COMMITTED','QUARANTINED') "
                + "ORDER BY a.created_at DESC, a.attempt_id DESC LIMIT 1";
        return jdbc.queryOne("findActiveAttempt", sql, statement -> {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
        }, JdbcMaterializationRows::mapAttempt);
    }

    public Optional<Generation> findGenerationByAttempt(AttemptId attempt) {
        String sql = "SELECT " + JdbcMaterializationRows.GENERATION_COLUMNS + " FROM materialization.generations g "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE g.producing_attempt_id=?";
        return jdbc.queryOne("findGenerationByAttempt", sql, statement ->
                statement.setObject(1, attempt.value()), JdbcMaterializationRows::mapGeneration);
    }

    public List<RefreshAttempt> findReconcilableAttempts(Instant observedBefore) {
        String sql = "SELECT " + JdbcMaterializationRows.ATTEMPT_COLUMNS + " FROM materialization.attempts a "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=a.attempt_id "
                + "WHERE a.state IN ('PREPARING','BUILDING','COMMITTED','QUARANTINED') "
                + "AND (NOT EXISTS (SELECT 1 FROM execution_anchor x "
                + "  WHERE x.execution_id=a.execution_anchor AND x.expires_at > clock_timestamp()) "
                + "  OR (a.state='QUARANTINED' AND a.updated_at < ?)) "
                + "ORDER BY a.updated_at, a.attempt_id";
        return jdbc.queryList("findReconcilableAttempts", sql, statement ->
                setInstant(statement, 1, observedBefore), JdbcMaterializationRows::mapAttempt);
    }

    public Optional<Schedule> findSchedule(MaterializationId object) {
        String sql = "SELECT " + JdbcMaterializationRows.SCHEDULE_COLUMNS + " FROM materialization.schedules "
                + "WHERE tenant_id=? AND object_name=?";
        return jdbc.queryOne("findSchedule", sql, statement -> {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
        }, JdbcMaterializationRows::mapSchedule);
    }

    public Optional<CommandRecord> findCommand(TenantId tenant, CommandId commandId) {
        String sql = "SELECT command_id, result_reference, created_at FROM materialization.commands "
                + "WHERE tenant_id=? AND command_id=?";
        return jdbc.queryOne("findCommand", sql, statement -> {
            statement.setString(1, tenant.value());
            statement.setString(2, commandId.value());
        }, result -> new CommandRecord(tenant, commandId, result.getString("result_reference"),
                getInstant(result, "created_at")));
    }

    public Optional<MaterializationWriteCapability> findWriteCapability(AttemptId attempt) {
        String sql = "SELECT c.attempt_id, c.tenant_id, c.object_name, c.incarnation_id, "
                + "c.definition_revision, c.generation_id, c.candidate_catalog, c.candidate_schema, "
                + "c.candidate_table, c.source_vector_hash, c.definer_principal, c.control_epoch, "
                + "c.issued_at, c.expires_at FROM materialization.write_capabilities c "
                + "WHERE c.attempt_id=?";
        return jdbc.queryOne("findWriteCapability", sql, statement ->
                statement.setObject(1, attempt.value()), JdbcMaterializationRows::mapCapability);
    }

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
        return jdbc.queryList("findReclaimableSourcePins", sql, statement -> {
            setInstant(statement, 1, settledBefore);
            setInstant(statement, 2, now);
            statement.setInt(3, attempts);
        }, JdbcMaterializationRows::mapReclaimableSourcePin);
    }
    public List<ReaderPin> findReaderPins(GenerationId generation) {
        String sql = "SELECT generation_id, reader_kind, reader_id, owner, expires_at, heartbeat_at "
                + "FROM materialization.reader_pins WHERE generation_id=? "
                + "ORDER BY expires_at, reader_kind, reader_id";
        return jdbc.queryList("findReaderPins", sql, statement ->
                statement.setObject(1, generation.value()), result -> new ReaderPin(
                        new GenerationId(result.getObject("generation_id", UUID.class)),
                        new ReaderId(ReaderKind.valueOf(result.getString("reader_kind")), result.getString("reader_id")),
                        result.getString("owner"),
                        getInstant(result, "expires_at"),
                        getInstant(result, "heartbeat_at")));
    }

    public List<Generation> findCollectableGenerations(Instant now, Instant eligibleBefore,
            int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
        String sql = "SELECT " + JdbcMaterializationRows.GENERATION_COLUMNS + " FROM materialization.generations g "
                + "JOIN materialization.attempts a ON a.attempt_id=g.producing_attempt_id "
                + "LEFT JOIN materialization.commit_evidence e ON e.attempt_id=g.producing_attempt_id "
                + "WHERE ("
                + "  (g.state='RETIRED' AND g.published_at<=?) "
                + "  OR (g.state='CANDIDATE' AND a.state IN ('FAILED','ABANDONED') "
                + "      AND g.created_at<=?) "
                + "  OR (g.state='COMMITTED' AND a.state='SUPERSEDED' AND g.created_at<=?)"
                + "  OR (g.state='QUARANTINED' AND a.state='ABANDONED' AND g.created_at<=?)"
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
        return jdbc.queryList("findCollectableGenerations", sql, statement -> {
            setInstant(statement, 1, eligibleBefore);
            setInstant(statement, 2, eligibleBefore);
            setInstant(statement, 3, eligibleBefore);
            setInstant(statement, 4, eligibleBefore);
            setInstant(statement, 5, now);
            statement.setInt(6, limit);
        }, JdbcMaterializationRows::mapGeneration);
    }

}
