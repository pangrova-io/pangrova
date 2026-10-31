package org.pangrova.coordinator.http.endpoint.copy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

/** Rejects COPY ticket transport when Jetty could log raw HTTP buffers. */
public final class CopySourceWireLoggingPolicy {
    private CopySourceWireLoggingPolicy() {}

    /** Must run before the sensitive endpoint is registered on a listening server. */
    public static void requireSafeJettyLogging() {
        if (!(LoggerFactory.getILoggerFactory() instanceof LoggerContext context)) {
            throw new IllegalStateException("COPY source-read requires controlled Logback wire logging");
        }
        Logger jetty = context.getLogger("org.eclipse.jetty");
        Level configured = jetty.getLevel();
        if (configured == null || configured.toInt() < Level.INFO_INT) {
            throw unsafe();
        }
        for (Logger logger : context.getLoggerList()) {
            if (logger.getName().startsWith("org.eclipse.jetty.")
                    && logger.getEffectiveLevel().toInt() < Level.INFO_INT) {
                throw unsafe();
            }
        }
        synchronized (context) {
            boolean installed = context.getTurboFilterList().stream()
                    .anyMatch(CopySourceWireLogFilter.class::isInstance);
            if (!installed) {
                var filter = new CopySourceWireLogFilter();
                filter.setContext(context);
                filter.start();
                context.addTurboFilter(filter);
            }
        }
    }

    private static IllegalStateException unsafe() {
        return new IllegalStateException("COPY source-read requires Jetty wire logging at INFO or higher");
    }
}
