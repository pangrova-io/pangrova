package org.pangrova.security.audit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AuditWriterFactory}: verifies backend selection logic including
 * composite mode (LOG + JDBC) and fail-safe fallbacks.
 */
class AuditWriterFactoryTest {

    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pangrova_test")
            .withUsername("pangrova")
            .withPassword("pangrova_test");

    @BeforeAll
    static void startContainer() { PG.start(); }

    @AfterAll
    static void stopContainer() { PG.stop(); }

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
    void jdbcConfigReturnsJdbcWriter() {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.JDBC, pgDataSource());
        assertThat(writer).isInstanceOf(JdbcAuditEventWriter.class);
    }

    @Test
    void jdbcConfigFallsBackToLogWithoutDataSource() {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.JDBC, null);
        assertThat(writer).isInstanceOf(LoggingAuditEventWriter.class);
    }

    @Test
    void compositeConfigReturnsCompositeWriter() {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.COMPOSITE, pgDataSource());
        assertThat(writer).isInstanceOf(CompositeAuditEventWriter.class);

        CompositeAuditEventWriter composite = (CompositeAuditEventWriter) writer;
        assertThat(composite.delegates()).hasSize(2);
        assertThat(composite.delegates().get(0)).isInstanceOf(LoggingAuditEventWriter.class);
        assertThat(composite.delegates().get(1)).isInstanceOf(JdbcAuditEventWriter.class);
    }

    @Test
    void compositeConfigFallsBackToLogWithoutDataSource() {
        AuditEventWriter writer = AuditWriterFactory.create(AuditWriterConfig.COMPOSITE, null);
        assertThat(writer).isInstanceOf(LoggingAuditEventWriter.class);
    }

    @Test
    void tableControlEventTypeIsDurableAndQueryable() {
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

    private static DataSource pgDataSource() {
        org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        return ds;
    }
}
