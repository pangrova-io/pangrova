package org.pangrova.security.assembly.audit;

import org.pangrova.security.core.audit.LoggingAuditEventWriter;
import org.pangrova.security.core.audit.CompositeAuditEventWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;
import org.pangrova.spi.security.audit.AuditEventWriter;
import org.pangrova.persistence.jdbc.security.audit.JdbcAuditEventWriter;

/**
 * Creates the configured {@link AuditEventWriter} backend.
 *
 * <p>Backend selection is config-driven via {@link AuditWriterConfig}. The JDBC
 * backend requires a {@link DataSource}; if {@code JDBC} is requested but no
 * data source is available the factory rejects the configuration. A request for
 * durable auditing must not silently become log-only auditing.</p>
 */
public final class AuditWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AuditWriterFactory.class);

    private AuditWriterFactory() {}

    /**
     * @param backend    the selected backend (null defaults to LOG)
     * @param dataSource data source for the JDBC backend; may be null for LOG
     * @return the writer instance
     */
    public static AuditEventWriter create(AuditWriterConfig backend, DataSource dataSource) {
        AuditWriterConfig effective = backend != null ? backend : AuditWriterConfig.LOG;
        if (effective != AuditWriterConfig.LOG && dataSource == null) {
            throw new IllegalArgumentException("Audit backend " + effective + " requires a DataSource");
        }
        switch (effective) {
            case JDBC -> {
                LOG.info("event=audit_backend_selected backend=jdbc");
                return new JdbcAuditEventWriter(dataSource);
            }
            case COMPOSITE -> {
                LOG.info("event=audit_backend_selected backend=composite sinks=[log,jdbc]");
                return new CompositeAuditEventWriter(List.of(
                        new LoggingAuditEventWriter(),
                        new JdbcAuditEventWriter(dataSource)));
            }
            default -> {
                LOG.info("event=audit_backend_selected backend=log");
                return new LoggingAuditEventWriter();
            }
        }
    }
}
