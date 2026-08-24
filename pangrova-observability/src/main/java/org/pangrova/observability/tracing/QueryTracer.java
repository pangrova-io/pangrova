package org.pangrova.observability.tracing;

import org.pangrova.spi.observability.Context;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.SpanKind;
import org.pangrova.spi.observability.TracingProvider;

/**
 * Creates query-scoped trace spans for each execution phase.
 * Attaches query metadata (queryId, truncated SQL) as span attributes.
 */
public final class QueryTracer {

    static final int MAX_SQL_LENGTH = 1024;

    private final TracingProvider tracingProvider;

    public QueryTracer(TracingProvider tracingProvider) {
        this.tracingProvider = tracingProvider;
    }

    /**
     * Starts a root span for a query, attaching queryId and truncated SQL.
     */
    public Span startQuerySpan(String queryId, String sql) {
        Span span = tracingProvider.startSpan("query", SpanKind.INTERNAL);
        span.setAttribute("pangrova.query.id", queryId);
        span.setAttribute("pangrova.query.sql", truncateSql(sql));
        return span;
    }

    /**
     * Starts a query span as a child of the given parent context.
     * Use this overload when the query originates from an existing trace
     * (e.g. an HTTP request with propagated trace headers).
     */
    public Span startQuerySpan(String queryId, String sql, Context parent) {
        Span span = tracingProvider.startSpan("query", SpanKind.INTERNAL, parent);
        span.setAttribute("pangrova.query.id", queryId);
        span.setAttribute("pangrova.query.sql", truncateSql(sql));
        return span;
    }

    /**
     * Starts a child span for a specific query phase (parse, optimize, schedule, execute).
     */
    public Span startPhaseSpan(String phase, Context parent) {
        return tracingProvider.startSpan("query." + phase, SpanKind.INTERNAL, parent);
    }

    static String truncateSql(String sql) {
        if (sql == null) {
            return "";
        }
        if (sql.length() <= MAX_SQL_LENGTH) {
            return sql;
        }
        return sql.substring(0, MAX_SQL_LENGTH) + "...";
    }
}
