package org.pangrova.worker.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.spi.observability.Scope;
import org.pangrova.spi.observability.Span;
import org.pangrova.spi.observability.TextMapGetter;

/**
 * JAX-RS request/response filter that extracts the W3C trace context injected by the
 * coordinator, opens a SERVER span as the parent for worker-side task execution, and
 * activates it so resource-method work (nested spans, native traceId, MDC) attaches to
 * the same trace instead of detaching into a fresh root.
 *
 * <p>Mirrors the coordinator's {@code TracingFilter}: the request leg extracts + activates,
 * the response leg deactivates before ending the span. Without this filter the coordinator→worker
 * hop unconditionally loses the trace (the inbound traceparent header was never extracted).
 */
@Provider
public final class WorkerTracingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final String SPAN_PROPERTY = "pangrova.tracing.span";
    private static final String SCOPE_PROPERTY = "pangrova.tracing.scope";

    private final TracingHttpFilter tracingHttpFilter;

    public WorkerTracingFilter(TracingHttpFilter tracingHttpFilter) {
        this.tracingHttpFilter = checkNotNull(tracingHttpFilter,
                "tracingHttpFilter must not be null");
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        String operationName = requestContext.getMethod() + " "
                + requestContext.getUriInfo().getRequestUri().getPath();
        Span span = tracingHttpFilter.startServerSpan(
                operationName, requestContext, JAXRS_HEADER_GETTER);
        Scope scope = span.makeCurrent();
        requestContext.setProperty(SPAN_PROPERTY, span);
        requestContext.setProperty(SCOPE_PROPERTY, scope);
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) {
        Object scopeObj = requestContext.getProperty(SCOPE_PROPERTY);
        if (scopeObj instanceof Scope scope) {
            scope.close();
        }
        Object spanObj = requestContext.getProperty(SPAN_PROPERTY);
        if (spanObj instanceof Span span) {
            tracingHttpFilter.endSpan(span, responseContext.getStatus());
        }
    }

    private static final TextMapGetter<ContainerRequestContext> JAXRS_HEADER_GETTER =
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(ContainerRequestContext carrier) {
                    return carrier.getHeaders().keySet();
                }

                @Override
                public String get(ContainerRequestContext carrier, String key) {
                    return carrier.getHeaderString(key);
                }
            };
}
