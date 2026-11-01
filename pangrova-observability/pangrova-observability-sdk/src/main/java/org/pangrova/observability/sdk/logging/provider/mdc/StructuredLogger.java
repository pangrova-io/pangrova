package org.pangrova.observability.sdk.logging.provider.mdc;

import org.pangrova.spi.observability.Logger;

import java.util.Map;

/**
 * SLF4J-backed {@link Logger} that enriches log entries with trace context via MDC
 * and emits structured key=value formatted log lines.
 *
 * <p>Trace correlation is managed by {@link LogContext}.</p>
 *
 * <p>The {@link #info(String, Map)} family of methods format context fields as
 * {@code key=value} pairs appended to the message, following the structured
 * logging convention used throughout Pangrova (e.g., {@code event=task_started taskId=abc}).</p>
 */
public final class StructuredLogger implements Logger {

    private final org.slf4j.Logger delegate;

    public StructuredLogger(org.slf4j.Logger delegate) {
        this.delegate = delegate;
    }

    @Override
    public void info(String message) {
        delegate.info(message);
    }

    @Override
    public void warn(String message) {
        delegate.warn(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        delegate.error(message, throwable);
    }

    @Override
    public void debug(String message) {
        delegate.debug(message);
    }

    /**
     * Logs an info message with structured key=value context fields.
     *
     * @param message the base log message (typically an event= prefix)
     * @param context key-value pairs appended to the message
     */
    public void info(String message, Map<String, Object> context) {
        if (delegate.isInfoEnabled()) {
            delegate.info(formatStructured(message, context));
        }
    }

    /**
     * Logs a warn message with structured key=value context fields.
     */
    public void warn(String message, Map<String, Object> context) {
        if (delegate.isWarnEnabled()) {
            delegate.warn(formatStructured(message, context));
        }
    }

    /**
     * Logs an error message with structured key=value context fields and a throwable.
     */
    public void error(String message, Map<String, Object> context, Throwable throwable) {
        if (delegate.isErrorEnabled()) {
            delegate.error(formatStructured(message, context), throwable);
        }
    }

    /**
     * Logs a debug message with structured key=value context fields.
     */
    public void debug(String message, Map<String, Object> context) {
        if (delegate.isDebugEnabled()) {
            delegate.debug(formatStructured(message, context));
        }
    }

    /**
     * Formats a message with key=value pairs appended.
     * Values containing spaces are quoted. Null values are rendered as empty string.
     */
    static String formatStructured(String message, Map<String, Object> context) {
        if (context == null || context.isEmpty()) {
            return message;
        }
        StringBuilder sb = new StringBuilder(message);
        for (Map.Entry<String, Object> entry : context.entrySet()) {
            sb.append(' ');
            sb.append(entry.getKey());
            sb.append('=');
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            String strValue = value.toString();
            if (strValue.indexOf(' ') >= 0) {
                sb.append('"').append(strValue).append('"');
            } else {
                sb.append(strValue);
            }
        }
        return sb.toString();
    }
}
