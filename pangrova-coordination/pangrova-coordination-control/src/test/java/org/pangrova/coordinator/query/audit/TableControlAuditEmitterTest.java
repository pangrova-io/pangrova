package org.pangrova.coordinator.query.audit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.table.api.enums.MaintenanceOperation;
import org.pangrova.table.api.enums.MaintenanceRunState;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlProvenance;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;
import org.pangrova.metadata.api.event.MetadataEvent;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;

class TableControlAuditEmitterTest {

    private static final UUID TABLE_ID = UUID.randomUUID();
    private static final QualifiedTableName REF =
            new QualifiedTableName("cat", "db", "orders");

    private RecordingWriter writer;
    private AuditEventEmitter emitter;
    private TableControlAuditEmitter audit;

    @BeforeEach
    void setUp() {
        writer = new RecordingWriter();
        emitter = new AuditEventEmitter(writer);
        audit = new TableControlAuditEmitter(emitter);
    }

    @AfterEach
    void tearDown() {
        emitter.close();
    }

    @Test
    void adoptionRecordsTheStableTableIdNotJustTheName() {
        audit.adopted(context(), REF, record(OwnershipMode.LINKED));

        AuditEvent event = awaitEvent(AuditEventType.TABLE_ADOPTED);
        assertThat(event.properties())
                .containsEntry("table_id", TABLE_ID.toString())
                .containsEntry("ownership_mode", "LINKED")
                .containsEntry("format_table_uuid", "uuid-orders");
        assertThat(event.catalog()).isEqualTo("cat");
        assertThat(event.schema()).isEqualTo("db");
        assertThat(event.accessedObjects()).singleElement()
                .satisfies(object -> {
                    assertThat(object.table()).isEqualTo("orders");
                    assertThat(object.accessType()).isEqualTo("ADOPT");
                });
    }

    @Test
    void unlinkIsAuditedWithTheRetiredRecord() {
        audit.unlinked(context(), REF, record(OwnershipMode.LINKED));

        AuditEvent event = awaitEvent(AuditEventType.TABLE_UNLINKED);
        assertThat(event.properties()).containsEntry("table_id", TABLE_ID.toString());
        assertThat(event.accessedObjects()).singleElement()
                .satisfies(object -> assertThat(object.accessType()).isEqualTo("UNLINK"));
    }

    @Test
    void skippedMaintenanceIsAuditedAsFailureWithTheReason() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        TableMaintenanceRun run = TableMaintenanceRun.running(
                record(OwnershipMode.LINKED), MaintenanceOperation.COMPACT, "MANUAL",
                "uuid-orders:7", "alice", start)
                .finished(MaintenanceRunState.SKIPPED, "",
                        java.util.Map.of("reason", "an external writer is active"),
                        start.plusSeconds(3));

        audit.maintenance(context(), REF, run, "an external writer is active");

        AuditEvent event = awaitEvent(AuditEventType.TABLE_MAINTENANCE);
        assertThat(event.success()).isFalse();
        assertThat(event.durationMs()).isEqualTo(3000);
        assertThat(event.principal()).isEqualTo("alice");
        assertThat(event.errorMessage()).isEqualTo("an external writer is active");
        assertThat(event.properties())
                .containsEntry("table_id", TABLE_ID.toString())
                .containsEntry("operation", "COMPACT")
                .containsEntry("state", "SKIPPED")
                .containsEntry("version_before", "uuid-orders:7")
                .containsEntry("reason", "an external writer is active");
    }

    @Test
    void observedExternalCommitIsNeverAttributedToPangrova() {
        audit.onEvent(new MetadataEvent.ExternalCommitObservedEvent(
                new QualifiedTableRef("cat", "db", "orders"), TABLE_ID,
                OwnershipMode.LINKED, "uuid-orders:7", "uuid-orders:8",
                Instant.parse("2026-01-01T00:00:00Z")));

        AuditEvent event = awaitEvent(AuditEventType.EXTERNAL_COMMIT_OBSERVED);
        assertThat(event.principal()).isEqualTo("system");
        assertThat(event.properties())
                .containsEntry("writer_kind", "EXTERNAL")
                .containsEntry("table_id", TABLE_ID.toString())
                .containsEntry("previous_version", "uuid-orders:7")
                .containsEntry("version_token", "uuid-orders:8");
    }

    @Test
    void unrelatedMetadataEventsAreIgnored() {
        audit.onEvent(new MetadataEvent.TableDroppedEvent(
                new QualifiedTableRef("cat", "db", "orders")));

        await().during(Duration.ofMillis(200)).until(() -> writer.events.isEmpty());
    }

    private AuditEvent awaitEvent(AuditEventType type) {
        await().atMost(Duration.ofSeconds(5))
                .until(() -> writer.events.stream().anyMatch(e -> e.eventType() == type));
        return writer.events.stream()
                .filter(e -> e.eventType() == type)
                .findFirst()
                .orElseThrow();
    }

    private static TableControlRecord record(OwnershipMode mode) {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        return TableControlRecord.builder()
                .tableId(TABLE_ID)
                .name("cat", "db", "orders")
                .format("ICEBERG")
                .formatTableUuid("uuid-orders")
                .ownershipMode(mode)
                .state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.ADOPTED)
                .metadataRevision(1)
                .version("uuid-orders:7", now)
                .ownerPrincipal("alice")
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static final class RecordingWriter implements AuditEventWriter {
        private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void write(AuditEvent event) {
            events.add(event);
        }
    }
}
