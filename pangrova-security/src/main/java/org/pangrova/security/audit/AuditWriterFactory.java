package org.pangrova.security.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.List;

/**
 * Creates the configured {@link AuditEventWriter} backend.
 *
 * <p>Backend selection is config-driven via {@link AuditWriterConfig}. The JDBC
 * backend requires a {@link DataSource}; if {@code JDBC} is requested but no
 * data source is available the factory fails safe to the log-only writer rather
 * than silently disabling auditing.</p>
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
        switch (effective) {
            case JDBC -> {
                if (dataSource == null) {
                    LOG.warn("event=audit_backend_fallback requested=jdbc reason=no_datasource "
                            + "fallback=log");
                    return new LoggingAuditEventWriter();
                }
                LOG.info("event=audit_backend_selected backend=jdbc");
                return new JdbcAuditEventWriter(dataSource);
            }
            case COMPOSITE -> {
                if (dataSource == null) {
                    LOG.warn("event=audit_backend_fallback requested=composite reason=no_datasource "
                            + "fallback=log");
                    return new LoggingAuditEventWriter();
                }
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
