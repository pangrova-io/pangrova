package org.pangrova.server.coordinator.http.endpoint.copy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.Marker;

/** Keeps a later Jetty logger-level mutation from rendering private HTTP bodies. */
final class CopySourceWireLogFilter extends TurboFilter {
    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level,
                              String format, Object[] params, Throwable error) {
        if (logger != null && ("org.eclipse.jetty".equals(logger.getName())
                || logger.getName().startsWith("org.eclipse.jetty."))
                && level.toInt() < Level.INFO_INT) {
            return FilterReply.DENY;
        }
        return FilterReply.NEUTRAL;
    }
}
