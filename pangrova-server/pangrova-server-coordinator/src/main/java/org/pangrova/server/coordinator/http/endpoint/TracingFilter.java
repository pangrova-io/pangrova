package org.pangrova.server.coordinator.http.endpoint;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.observability.api.tracing.Scope;
import org.pangrova.observability.api.tracing.Span;
import org.pangrova.observability.api.tracing.TextMapGetter;

/**
 * JAX-RS ContainerRequestFilter/ContainerResponseFilter for OpenTelemetry trace propagation.
 * Replaces the former tracing filter.
 *
 * <p>Extracts W3C trace context from request headers, creates a server span on request,
 * activates it via {@code makeCurrent()}, and ends the span with appropriate status on response.
 * F-CHAIN-OBS-01 fix: the request filter must activate the span so that {@code Context.current()}
 * returns the server span for nested child spans within the request handler.</p>
 */
@Provider
public final class TracingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final String SPAN_PROPERTY = "pangrova.tracing.span";
    private static final String SCOPE_PROPERTY = "pangrova.tracing.scope";

    private final TracingHttpFilter tracingHttpFilter;

    public TracingFilter(TracingHttpFilter tracingHttpFilter) {
        this.tracingHttpFilter = checkNotNull(tracingHttpFilter,
                "tracingHttpFilter must not be null");
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        String operationName = requestContext.getMethod() + " "
                + requestContext.getUriInfo().getRequestUri().getPath();
        Span span = tracingHttpFilter.startServerSpan(
                operationName, requestContext, JAXRS_HEADER_GETTER);
        // F-CHAIN-OBS-01: activate the span so nested spans attach as children
        Scope scope = span.makeCurrent();
        requestContext.setProperty(SPAN_PROPERTY, span);
        requestContext.setProperty(SCOPE_PROPERTY, scope);
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext) {
        // F-CHAIN-OBS-01: close scope BEFORE ending span to deactivate the current context
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
