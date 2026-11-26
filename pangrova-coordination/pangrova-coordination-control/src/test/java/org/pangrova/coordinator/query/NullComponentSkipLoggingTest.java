package org.pangrova.coordinator.query;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that null component skip paths emit structured DEBUG logs
 * when optional modules (governance, metadata) are disabled.
 *
 * <p>Uses Logback ListAppender to capture log output (no mocks).
 * Each test constructs the component with null dependencies and
 * invokes the skip path, then asserts the structured log format.</p>
 *
 * <p>Validates: Requirements R2.4 — Null Propagation Logging</p>
 */
class NullComponentSkipLoggingTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger bridgeLogger;

    @BeforeEach
    void setUp() {
        bridgeLogger = (Logger) LoggerFactory.getLogger(QueryAuditDispatcher.class);
        appender = new ListAppender<>();
        appender.start();
        bridgeLogger.addAppender(appender);
        bridgeLogger.setLevel(Level.DEBUG);
    }

    @AfterEach
    void tearDown() {
        bridgeLogger.detachAppender(appender);
        appender.stop();
    }

    @Test
    void terminalAuditRecordIsWrittenWhenSecurityEventDeliveryIsAbsent() {
        var records = new java.util.ArrayList<org.pangrova.query.api.audit.QueryAuditRecord>();
        var audit = QueryAuditDispatcher.createAuditEmitter(null, records::add);
        var completedAt = java.time.Instant.now();
        audit.emitQueryTerminal(org.pangrova.query.api.enums.QueryState.FINISHED,
                org.pangrova.query.api.audit.QueryAuditRecord.wallTimeOnly(
                        "q-finished", "alice", "SELECT 1", "tenant-a", 15, completedAt, true), null);
        audit.emitQueryTerminal(org.pangrova.query.api.enums.QueryState.FAILED,
                org.pangrova.query.api.audit.QueryAuditRecord.wallTimeOnly(
                        "q-failed", "alice", "SELECT 2", "tenant-a", 11, completedAt, false), "denied");
        assertThat(records).extracting(record -> record.queryId(), record -> record.success(),
                        record -> record.tenantId(), record -> record.sql())
                .containsExactly(org.assertj.core.groups.Tuple.tuple("q-finished", true, "tenant-a", "SELECT 1"),
                        org.assertj.core.groups.Tuple.tuple("q-failed", false, "tenant-a", "SELECT 2"));
    }

    @Test
    void lineageEmitterNull_emitsDebugLogWithSkipEvent() {
        QueryAuditDispatcher bridge = new QueryAuditDispatcher(null, null, null);

        bridge.captureTerminalLineage("q-test-001");

        assertThat(appender.list).hasSize(1);
        ILoggingEvent event = appender.list.get(0);
        assertThat(event.getLevel()).isEqualTo(Level.DEBUG);
        String msg = event.getFormattedMessage();
        assertThat(msg).contains("event=lineage_skip");
        assertThat(msg).contains("reason=governance_disabled");
        assertThat(msg).contains("queryId=q-test-001");
    }

    @Test
    void lineageEmitterNull_doesNotCallQueryDispatcher() {
        // queryDispatcher is also null — if lineage skip works correctly,
        // we never reach queryDispatcher.removeLineageInfo() and no NPE occurs
        QueryAuditDispatcher bridge = new QueryAuditDispatcher(null, null, null);

        bridge.captureTerminalLineage("q-safe");

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getFormattedMessage())
                .contains("event=lineage_skip");
    }
}
