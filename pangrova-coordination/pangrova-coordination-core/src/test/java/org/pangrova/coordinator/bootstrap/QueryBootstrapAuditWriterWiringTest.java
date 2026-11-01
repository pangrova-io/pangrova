package org.pangrova.coordinator.bootstrap;

import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.query.assembly.audit.QueryAuditServices;
import org.pangrova.spi.security.audit.AuditEvent;
import org.pangrova.spi.security.audit.AuditEventType;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgres;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the actual audit emitter selected by application composition. */
class QueryBootstrapAuditWriterWiringTest extends ContainerTestBase {

    @Test
    void inMemoryDeploymentWritesToLogWithoutRequestingJdbc() {
        emitAndVerifyLog(null);
    }

    @Test
    void durableDeploymentWritesToBothLogAndDatabase() throws Exception {
        DataSource source = SharedPostgres.isolatedDataSource();
        String eventId = emitAndVerifyLog(source);
        try (var connection = source.getConnection();
                var statement = connection.prepareStatement("SELECT principal FROM audit_logs WHERE event_id = ?")) {
            statement.setString(1, eventId);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo("audit-test");
                assertThat(result.next()).isFalse();
            }
        }
    }

    private static String emitAndVerifyLog(DataSource source) {
        var wiring = new QueryRuntimeWiring(new QueryRuntimeWiring.Dependencies(
                CoordinatorBootstrapConfig.withDefaults("audit-test", 0), new NoOpTracingProvider(),
                null, new ModuleIntegrationBootstrap(), source, new QueryAuditServices().auditService()));
        var emitter = wiring.createAuditEmitter();
        String id = UUID.randomUUID().toString();
        var logger = (Logger) LoggerFactory.getLogger("pangrova.audit");
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            emitter.emit(new AuditEvent(id, AuditEventType.EXTERNAL_COMMIT_OBSERVED,
                    Instant.now(), "audit-test", null, null, null, null, null, "catalog", "schema",
                    List.of(), true, null, 0, 0, 0, Map.of()));
            emitter.close();
            assertThat(emitter.shutdownTimedOut()).isFalse();
            assertThat(appender.list).anySatisfy(event ->
                    assertThat(event.getFormattedMessage()).contains("eventId=" + id));
            return id;
        } finally {
            emitter.close();
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
