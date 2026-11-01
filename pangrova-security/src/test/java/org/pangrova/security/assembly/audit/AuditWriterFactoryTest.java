package org.pangrova.security.assembly.audit;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgres;
import org.pangrova.security.core.audit.LoggingAuditEventWriter;
import org.pangrova.security.core.audit.CompositeAuditEventWriter;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.spi.security.audit.AccessedObject;
import org.pangrova.spi.security.audit.AuditEvent;
import org.pangrova.spi.security.audit.AuditEventQuery;
import org.pangrova.spi.security.audit.AuditEventType;
import org.pangrova.spi.security.audit.AuditEventWriter;
import org.pangrova.persistence.jdbc.security.audit.JdbcAuditEventWriter;

/**
 * Tests for {@link AuditWriterFactory}: verifies backend selection logic including
 * composite mode (LOG + JDBC) and rejection of invalid configuration.
 */
class AuditWriterFactoryTest extends ContainerTestBase {

    @Test
    void defaultsToLoggingWriter() {
        AuditEventWriter writer = AuditWriterFactory.create(null, null);
        assertThat(writer).isInstanceOf(LoggingAuditEventWriter.class);
    }

    @Test
    void logConfigReturnsLoggingWriter() {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.LOG, null);
        assertThat(writer).isInstanceOf(LoggingAuditEventWriter.class);
    }

    @Test
    void jdbcConfigReturnsJdbcWriter() throws Exception {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.JDBC, pgDataSource());
        assertThat(writer).isInstanceOf(JdbcAuditEventWriter.class);
    }

    @Test
    void jdbcConfigRejectsMissingDataSource() {
        assertThatThrownBy(() -> AuditWriterFactory.create(AuditWriterConfig.JDBC, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DataSource");
    }

    @Test
    void compositeConfigReturnsCompositeWriter() throws Exception {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.COMPOSITE, pgDataSource());
        assertThat(writer).isInstanceOf(CompositeAuditEventWriter.class);

        CompositeAuditEventWriter composite = (CompositeAuditEventWriter) writer;
        assertThat(composite.delegates()).hasSize(2);
        assertThat(composite.delegates().get(0)).isInstanceOf(LoggingAuditEventWriter.class);
        assertThat(composite.delegates().get(1)).isInstanceOf(JdbcAuditEventWriter.class);
    }

    @Test
    void compositeConfigRejectsMissingDataSource() {
        assertThatThrownBy(() -> AuditWriterFactory.create(AuditWriterConfig.COMPOSITE, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DataSource");
    }

    @Test
    void tableControlEventTypeIsDurableAndQueryable() throws Exception {
        JdbcAuditEventWriter writer = (JdbcAuditEventWriter)
                AuditWriterFactory.create(AuditWriterConfig.JDBC, pgDataSource());
        String eventId = UUID.randomUUID().toString();
        writer.write(new AuditEvent(
                eventId, AuditEventType.EXTERNAL_COMMIT_OBSERVED, Instant.now(), "system",
                null, null, null, null, null, "cat", "db",
                List.of(new AccessedObject("TABLE", "cat", "db", "orders", List.of(),
                        "COMMIT_OBSERVED")),
                true, null, 0, 0, 0,
                Map.of("table_id", "6b1d9f02-52b4-4f1c-9a6e-2f0dc8f31c44",
                        "writer_kind", "EXTERNAL")));

        assertThat(writer.query(AuditEventQuery.byType(
                AuditEventType.EXTERNAL_COMMIT_OBSERVED)))
                .anySatisfy(event -> {
                    assertThat(event.eventId()).isEqualTo(eventId);
                    assertThat(event.properties()).containsEntry("writer_kind", "EXTERNAL");
                    assertThat(event.accessedObjects()).singleElement()
                            .satisfies(object -> assertThat(object.table()).isEqualTo("orders"));
                });
    }

    @Test
    void configFromTokenParsesComposite() {
        assertThat(AuditWriterConfig.fromToken("composite")).isEqualTo(AuditWriterConfig.COMPOSITE);
        assertThat(AuditWriterConfig.fromToken("COMPOSITE")).isEqualTo(AuditWriterConfig.COMPOSITE);
    }

    @Test
    void invalidBackendDoesNotSilentlySelectLog() {
        assertThatThrownBy(() -> AuditWriterConfig.fromToken("jbdc"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(AuditWriterConfig.fromToken(null)).isEqualTo(AuditWriterConfig.LOG);
        assertThat(AuditWriterConfig.fromToken(" ")).isEqualTo(AuditWriterConfig.LOG);
    }

    private static DataSource pgDataSource() throws java.sql.SQLException {
        return SharedPostgres.isolatedDataSource();
    }
}
