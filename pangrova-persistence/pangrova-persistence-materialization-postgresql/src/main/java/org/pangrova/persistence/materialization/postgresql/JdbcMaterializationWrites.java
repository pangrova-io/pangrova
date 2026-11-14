package org.pangrova.persistence.materialization.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.domain.SourcePin;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.CommandRecord;
import org.pangrova.materialization.api.port.MaterializationEvent;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setJson;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setNullableInstant;

/**
 * Insert and update statements shared by the materialization repository.
 *
 * <p>Each helper is a single statement group that must run inside the caller's
 * transaction; ownership, locking, and commit stay with the repository.
 */
final class JdbcMaterializationWrites {

    private JdbcMaterializationWrites() {
    }

    static int insertObjectIfAbsent(Connection connection, MaterializationObject object)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.objects (tenant_id, object_name, object_kind, "
                        + "incarnation_id, definition_revision, revision, published_generation_id, "
                        + "state, definer_principal, security_mode, refresh_mode, "
                        + "incremental_strategy, created_at, updated_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                        + "ON CONFLICT (tenant_id, object_name) DO NOTHING")) {
            JdbcMaterializationRows.bindObjectId(statement, 1, object.id());
            statement.setString(3, object.kind().name());
            statement.setObject(4, object.incarnation().value());
            statement.setLong(5, object.definitionRevision());
            statement.setLong(6, object.revision());
            statement.setNull(7, java.sql.Types.OTHER);
            statement.setString(8, JdbcMaterializationRows.objectStateName(object.state()));
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

    static void insertDefinition(Connection connection, MaterializationId object,
            DefinitionRevision definition) throws SQLException {
        JdbcMaterializationRows.requireStorableFingerprint(definition);
        lockDependencyGraph(connection, object.tenant());
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.definition_revisions (tenant_id, object_name, revision, "
                        + "definition_sql, canonical_sql, plan_fingerprint, resolved, output_schema, "
                        + "output_schema_hash, sources, policy_snapshot, security_mode, "
                        + "definer_principal, definer_groups, session_properties, created_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
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
            setJson(statement, 14, MaterializationJson.definerGroups(
                    definition.definerGroups()));
            setJson(statement, 15, MaterializationJson.sessionProperties(
                    definition.sessionProperties()));
            setInstant(statement, 16, definition.createdAt());
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
    static void lockDependencyGraph(Connection connection, TenantId tenant)
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
    static void requireAcyclicDefinition(Connection connection, MaterializationId object)
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

    static void insertSchedule(Connection connection, Schedule schedule) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.schedules (tenant_id, object_name, target_lag_ms, "
                        + "next_due_at, last_success_at, consecutive_failures, state, lease_owner, "
                        + "lease_expires_at, version) VALUES (?,?,?,?,?,?,?,?,?,?)")) {
            JdbcMaterializationRows.bindObjectId(statement, 1, schedule.object());
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

    static void insertSourcePin(Connection connection, SourcePin pin) throws SQLException {
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

    static void insertCapability(Connection connection, MaterializationWriteCapability capability)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.write_capabilities (attempt_id, tenant_id, object_name, "
                        + "incarnation_id, definition_revision, generation_id, candidate_catalog, "
                        + "candidate_schema, candidate_table, source_vector_hash, definer_principal, "
                        + "control_epoch, issued_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            statement.setObject(1, capability.attempt().value());
            JdbcMaterializationRows.bindObjectId(statement, 2, capability.object());
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

    static void insertEvent(Connection connection, MaterializationEvent event) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO materialization.events (event_id, event_type, tenant_id, object_name, "
                        + "command_id, attempt_id, generation_id, query_id, requesting_principal, "
                        + "effective_principal, detail, occurred_at) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")) {
            statement.setObject(1, UUID.fromString(event.eventId()));
            statement.setString(2, event.eventType().name());
            JdbcMaterializationRows.bindObjectId(statement, 3, event.object());
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
                statement.setString(8, event.query().orElseThrow().id());
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

    static void recordCommand(Connection connection, CommandRecord record) throws SQLException {
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
    static void recordPublicationCommand(Connection connection, CommandRecord record)
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

    static boolean isPublicationReceipt(CommandRecord receipt) {
        JsonNode reference = MaterializationJson.tree(receipt.resultReference());
        return reference != null && reference.path("object").isTextual()
                && reference.path("generation").isTextual();
    }

    static long currentDefinitionRevision(Connection connection, MaterializationId object)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT definition_revision FROM materialization.objects "
                        + "WHERE tenant_id=? AND object_name=? FOR UPDATE")) {
            JdbcMaterializationRows.bindObjectId(statement, 1, object);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw MaterializationException.notFound(object.canonicalName());
                }
                return result.getLong("definition_revision");
            }
        }
    }
}
