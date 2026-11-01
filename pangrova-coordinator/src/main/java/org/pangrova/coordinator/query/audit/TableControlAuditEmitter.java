package org.pangrova.coordinator.query.audit;

import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.metadata.core.control.TableControlRecord;
import org.pangrova.metadata.core.control.TableMaintenanceRun;
import org.pangrova.metadata.core.event.MetadataEvent;
import org.pangrova.metadata.core.event.MetadataEventListener;
import org.pangrova.security.audit.AccessedObject;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Converts table-control actions into durable audit events.
 *
 * <p>ADOPT/UNLINK, every maintenance attempt, and external commits observed on a
 * controlled table are all attributed to a {@code table_id}; a name alone cannot
 * distinguish a table from a later drop/recreate. External commits carry
 * {@code writer_kind=EXTERNAL} so they are never mistaken for a Pangrova
 * commit in audit review.</p>
 */
public final class TableControlAuditEmitter implements MetadataEventListener {

    private static final String SYSTEM_PRINCIPAL = "system";

    private final AuditEventEmitter emitter;

    public TableControlAuditEmitter(AuditEventEmitter emitter) {
        this.emitter = java.util.Objects.requireNonNull(emitter, "emitter");
    }

    /** Audits an external table becoming LINKED. */
    public void adopted(CoordinatorSessionContext context, QualifiedTableName ref,
            TableControlRecord record) {
        emit(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.TABLE_ADOPTED, Instant.now(),
                principal(context), null, null, null, queryId(context), null,
                ref.catalog(), ref.schema(),
                List.of(accessed(ref, "ADOPT")), true, null, 0, 0, 0,
                recordProperties(context, record)));
    }

    /** Audits removal of a control record; the physical table is left untouched. */
    public void unlinked(CoordinatorSessionContext context, QualifiedTableName ref,
            TableControlRecord record) {
        emit(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.TABLE_UNLINKED, Instant.now(),
                principal(context), null, null, null, queryId(context), null,
                ref.catalog(), ref.schema(),
                List.of(accessed(ref, "UNLINK")), true, null, 0, 0, 0,
                recordProperties(context, record)));
    }

    /**
     * Records a terminal maintenance run. SKIPPED runs are audited as failures:
     * an operator needs to see that maintenance was requested but did not run.
     */
    public void maintenance(CoordinatorSessionContext context, QualifiedTableName ref,
            TableMaintenanceRun run, String error) {
        Map<String, String> properties = new LinkedHashMap<>(run.details());
        properties.put("table_id", run.tableId().toString());
        properties.put("operation", run.operation().name());
        properties.put("state", run.state().name());
        properties.put("mode", run.mode());
        properties.put("version_before", run.versionBefore());
        properties.put("version_after", run.versionAfter());
        properties.put("started_at", run.startedAt().toString());
        if (run.finishedAt() != null) {
            properties.put("finished_at", run.finishedAt().toString());
        }
        if (context != null && context.tenantId() != null) {
            properties.put("tenant_id", context.tenantId());
        }
        long durationMs = run.finishedAt() == null ? 0
                : Duration.between(run.startedAt(), run.finishedAt()).toMillis();
        emit(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.TABLE_MAINTENANCE,
                run.finishedAt() != null ? run.finishedAt() : Instant.now(),
                run.actor().isBlank() ? SYSTEM_PRINCIPAL : run.actor(),
                null, null, null, queryId(context), null,
                ref.catalog(), ref.schema(),
                List.of(accessed(ref, run.operation().name())),
                run.state() == org.pangrova.metadata.core.control.MaintenanceRunState.SUCCEEDED,
                error, durationMs, 0, 0, Map.copyOf(properties)));
    }

    /** Audits an external commit; the writer is not Pangrova, so it is never a write intent. */
    @Override
    public void onEvent(MetadataEvent event) {
        if (!(event instanceof MetadataEvent.ExternalCommitObservedEvent commit)) {
            return;
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("table_id", commit.tableId().toString());
        properties.put("ownership_mode", commit.ownershipMode().name());
        properties.put("writer_kind", "EXTERNAL");
        properties.put("previous_version", nullToEmpty(commit.previousVersionToken()));
        properties.put("version_token", nullToEmpty(commit.versionToken()));
        properties.put("observed_at", commit.observedAt().toString());
        emit(new AuditEvent(
                UUID.randomUUID().toString(), AuditEventType.EXTERNAL_COMMIT_OBSERVED,
                commit.observedAt(), SYSTEM_PRINCIPAL, null, null, null, null, null,
                commit.tableRef().catalog(), commit.tableRef().schema(),
                List.of(new AccessedObject("TABLE", commit.tableRef().catalog(),
                        commit.tableRef().schema(), commit.tableRef().table(),
                        List.of(), "COMMIT_OBSERVED")),
                true, null, 0, 0, 0, Map.copyOf(properties)));
    }

    private void emit(AuditEvent event) {
        emitter.emit(event);
    }

    private static Map<String, String> recordProperties(
            CoordinatorSessionContext context, TableControlRecord record) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("table_id", record.tableId().toString());
        properties.put("ownership_mode", record.ownershipMode().name());
        properties.put("provenance", record.provenance().name());
        if (record.format() != null) {
            properties.put("format", record.format());
        }
        if (record.formatTableUuid() != null) {
            properties.put("format_table_uuid", record.formatTableUuid());
        }
        if (record.versionToken() != null) {
            properties.put("version_token", record.versionToken());
        }
        if (context != null && context.tenantId() != null) {
            properties.put("tenant_id", context.tenantId());
        }
        return Map.copyOf(properties);
    }

    private static AccessedObject accessed(QualifiedTableName ref, String accessType) {
        return new AccessedObject("TABLE", ref.catalog(), ref.schema(), ref.table(),
                List.of(), accessType);
    }

    private static String principal(CoordinatorSessionContext context) {
        return context == null || context.userIdentity() == null
                ? SYSTEM_PRINCIPAL : context.userIdentity().principal();
    }

    private static String queryId(CoordinatorSessionContext context) {
        return context == null ? null : context.queryId();
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
