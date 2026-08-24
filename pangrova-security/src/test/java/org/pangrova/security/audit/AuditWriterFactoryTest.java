package org.pangrova.security.audit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
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
