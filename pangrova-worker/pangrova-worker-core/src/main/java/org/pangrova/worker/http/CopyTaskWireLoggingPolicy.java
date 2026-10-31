package org.pangrova.worker.http;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

/** Denies Jetty wire logging before a private COPY bearer listener starts. */
public final class CopyTaskWireLoggingPolicy {
    private CopyTaskWireLoggingPolicy() {}

    /** Requires Jetty wire logging to remain below the level that exposes private tickets. */
    public static void requireSafeJettyLogging() {
        if (!(LoggerFactory.getILoggerFactory() instanceof LoggerContext context)) {
            throw new IllegalStateException("COPY task transport requires controlled Logback logging");
        }
        Logger jetty = context.getLogger("org.eclipse.jetty");
        if (jetty.getLevel() == null || jetty.getLevel().toInt() < Level.INFO_INT) {
            throw unsafe();
        }
        for (Logger logger : context.getLoggerList()) {
            if (logger.getName().startsWith("org.eclipse.jetty.")
                    && logger.getEffectiveLevel().toInt() < Level.INFO_INT) {
                throw unsafe();
            }
        }
        synchronized (context) {
            if (context.getTurboFilterList().stream()
                    .noneMatch(JettyWireLogFilter.class::isInstance)) {
                var filter = new JettyWireLogFilter();
                filter.setContext(context);
                filter.start();
                context.addTurboFilter(filter);
            }
        }
    }

    private static IllegalStateException unsafe() {
        return new IllegalStateException("COPY task transport requires Jetty logging at INFO or higher");
    }

    private static final class JettyWireLogFilter extends TurboFilter {
        @Override public FilterReply decide(Marker marker, Logger logger, Level level,
                String format, Object[] params, Throwable error) {
            if (logger != null && ("org.eclipse.jetty".equals(logger.getName())
                    || logger.getName().startsWith("org.eclipse.jetty."))
                    && level.toInt() < Level.INFO_INT) {
                return FilterReply.DENY;
            }
            return FilterReply.NEUTRAL;
        }
    }
}
