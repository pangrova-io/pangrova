package org.pangrova.persistence.materialization.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.AttemptState;
import org.pangrova.materialization.domain.Binding;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.DependencyKind;
import org.pangrova.materialization.domain.ExecutionFence;
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
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.ScheduleLease;
import org.pangrova.materialization.domain.ScheduleState;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TargetLag;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.ReclaimableSourcePin;
import org.pangrova.connector.api.materialization.CommitEvidence;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;

/**
 * Column projections, row mapping, and state-code translation for the
 * materialization JDBC adapters.
 *
 * <p>Every mapper re-derives and re-checks persisted fingerprints so a
 * corrupted row fails closed instead of becoming a domain object.
 */
final class JdbcMaterializationRows {

    static final String OBJECT_COLUMNS =
            "tenant_id, object_name, object_kind, incarnation_id, definition_revision, revision, "
                    + "published_generation_id, state, definer_principal, security_mode, "
                    + "refresh_mode, incremental_strategy, created_at, updated_at";

    static final String GENERATION_COLUMNS =
            "g.generation_id, g.tenant_id, g.object_name, g.incarnation_id, g.definition_revision, "
                    + "g.producing_attempt_id, g.base_generation_id, g.physical_catalog, "
                    + "g.physical_schema, g.physical_table, g.refresh_kind, g.source_vector, "
                    + "g.source_vector_hash, g.state, g.created_at, g.published_at, "
                    + "e.connector_id AS evidence_connector_id, "
                    + "e.commit_fingerprint AS evidence_commit_fingerprint, "
                    + "e.manifest_hash AS evidence_manifest_hash, "
                    + "e.format_version AS evidence_format_version, "
                    + "e.committed_at AS evidence_committed_at";

    static final String ATTEMPT_COLUMNS =
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

    static final String SCHEDULE_COLUMNS =
            "tenant_id, object_name, target_lag_ms, next_due_at, last_success_at, "
                    + "consecutive_failures, state, lease_owner, lease_expires_at, version";

    private JdbcMaterializationRows() {
    }

    static MaterializationObject mapObject(ResultSet result) throws SQLException {
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

    static DefinitionRevision mapDefinition(MaterializationId object, ResultSet result)
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
                MaterializationJson.definerGroups(
                        MaterializationJson.tree(result.getString("definer_groups"))),
                MaterializationJson.sessionProperties(
                        MaterializationJson.tree(result.getString("session_properties"))),
                getInstant(result, "created_at"));
    }

    static Generation mapGeneration(ResultSet result) throws SQLException {
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

    static RefreshAttempt mapAttempt(ResultSet result) throws SQLException {
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

    static ReclaimableSourcePin mapReclaimableSourcePin(ResultSet result) throws SQLException {
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

    static MaterializationWriteCapability mapCapability(ResultSet result) throws SQLException {
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

    static Schedule mapSchedule(ResultSet result) throws SQLException {
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

    static Optional<CommitEvidence> evidence(ResultSet result) throws SQLException {
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

    static MaterializationId objectId(ResultSet result) throws SQLException {
        return new MaterializationId(new TenantId(result.getString("tenant_id")),
                QualifiedName.parse(result.getString("object_name")));
    }
    static ObjectState objectState(String value) {
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

    static AttemptState attemptState(String value) {
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

    static GenerationState generationState(String value) {
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

    static RefreshKind refreshKind(String value) {
        return switch (value) {
            case "FULL" -> RefreshKind.Full.INSTANCE;
            case "INCREMENTAL_APPEND_ONLY" -> RefreshKind.IncrementalAppendOnly.INSTANCE;
            case "INCREMENTAL_PARTITION_REPLACE" -> RefreshKind.IncrementalPartitionReplace.INSTANCE;
            case "INCREMENTAL_AFFECTED_KEY" -> RefreshKind.IncrementalAffectedKey.INSTANCE;
            case "INCREMENTAL_CDC" -> RefreshKind.IncrementalCdc.INSTANCE;
            default -> throw MaterializationJson.corrupt("unknown refresh kind " + value);
        };
    }

    static String refreshKindName(RefreshKind kind) {
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
    static void requireMatchingObject(MaterializationObject object,
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

    static void requireMatchingObject(MaterializationObject object, Schedule schedule) {
        if (!object.id().equals(schedule.object())) {
            throw MaterializationException.invalidDefinition(
                    "schedule does not belong to the creating object");
        }
    }

    static void requireStorableFingerprint(DefinitionRevision definition) {
        if (!definition.binding().planFingerprint().matches("[0-9a-f]{64}")) {
            throw MaterializationException.invalidDefinition(
                    "plan fingerprint must be a lowercase SHA-256 digest");
        }
    }

    static void requireCapability(RefreshAttempt attempt, Generation candidate,
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
    static void bindObjectId(PreparedStatement statement, int index,
            MaterializationId object) throws SQLException {
        statement.setString(index, object.tenant().value());
        statement.setString(index + 1, object.name().toString());
    }

    static void bindNullableUuid(PreparedStatement statement, int index, UUID value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, java.sql.Types.OTHER);
        } else {
            statement.setObject(index, value);
        }
    }
    static Generation publishedView(
            Generation generation, CommitEvidence evidence, Instant publishedAt) {
        return new Generation(generation.id(), generation.object(), generation.incarnation(),
                generation.definitionRevision(), generation.producingAttempt(),
                generation.baseGeneration(), generation.physicalTable(), generation.refreshKind(),
                generation.sourceVector(), Optional.of(evidence),
                GenerationState.Published.INSTANCE, generation.createdAt(),
                Optional.of(publishedAt));
    }

    static String objectReference(MaterializationId object, long revision, String action) {
        var reference = MaterializationJson.mapper().createObjectNode();
        reference.put("object", object.canonicalName());
        reference.put("revision", revision);
        reference.put("action", action);
        return reference.toString();
    }

    static String publicationReference(MaterializationId object, GenerationId generation,
            long revision) {
        var reference = MaterializationJson.mapper().createObjectNode();
        reference.put("object", object.canonicalName());
        reference.put("generation", generation.value().toString());
        reference.put("revision", revision);
        return reference.toString();
    }

    static String objectStateName(ObjectState state) {
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

    static String attemptStateName(AttemptState state) {
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

    static String generationStateName(GenerationState state) {
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

    static JsonNode parseCommandReference(String reference) {
        JsonNode node = MaterializationJson.tree(reference);
        if (node == null || !node.path("object").isTextual()
                || !node.path("generation").isTextual()) {
            throw MaterializationJson.corrupt("recorded publication command is malformed");
        }
        return node;
    }
}
