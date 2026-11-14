package org.pangrova.server.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;

import org.pangrova.observability.sdk.tracing.http.TracingHttpFilter;
import org.pangrova.testing.testkit.TestObservability;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link TracingFilter} correctly creates and ends spans
 * for HTTP request/response pairs without throwing exceptions.
 */
class TracingFilterMetricsTest {

    @Test
    @DisplayName("request and response filter create and end a span without error")
    void requestResponseFilterCreatesAndEndsSpan() {
        TracingFilter filter = new TracingFilter(new TracingHttpFilter(TestObservability.TRACING));

        Map<String, Object> props = new HashMap<>();
        ContainerRequestContext request = newRequest(props, "GET", "/v1/query");
        ContainerResponseContext response = newResponse(200);

        filter.filter(request);
        filter.filter(request, response);

        // The span was stored as a property during request filtering
        assertThat(props.get("pangrova.tracing.span"))
                .as("span must be stored during request filtering")
                .isNotNull();
    }

    @Test
    @DisplayName("response filter with no prior request span is a no-op (no NPE)")
    void responseFilterWithoutRequestSpanIsNoOp() {
        TracingFilter filter = new TracingFilter(new TracingHttpFilter(TestObservability.TRACING));

        Map<String, Object> props = new HashMap<>();
        ContainerRequestContext request = newRequest(props, "POST", "/v1/query");
        ContainerResponseContext response = newResponse(500);

        // Only call response filter without request filter first
        filter.filter(request, response);
        // No assertions beyond "did not throw"
    }

    private static ContainerRequestContext newRequest(Map<String, Object> props,
                                                      String method, String path) {
        UriInfo uriInfo = (UriInfo) Proxy.newProxyInstance(
                TracingFilterMetricsTest.class.getClassLoader(),
                new Class<?>[]{UriInfo.class},
                (proxy, m, args) -> "getRequestUri".equals(m.getName())
                        ? URI.create(path) : defaultValue(m));
        InvocationHandler handler = (proxy, m, args) -> {
            switch (m.getName()) {
                case "getMethod":
                    return method;
                case "getUriInfo":
                    return uriInfo;
                case "getHeaders":
                    return new MultivaluedHashMap<String, String>();
                case "getHeaderString":
                    return null;
                case "setProperty":
                    props.put((String) args[0], args[1]);
                    return null;
                case "getProperty":
                    return props.get(args[0]);
                default:
                    return defaultValue(m);
            }
        };
        return (ContainerRequestContext) Proxy.newProxyInstance(
                TracingFilterMetricsTest.class.getClassLoader(),
                new Class<?>[]{ContainerRequestContext.class}, handler);
    }

    private static ContainerResponseContext newResponse(int status) {
        InvocationHandler handler = (proxy, m, args) ->
                "getStatus".equals(m.getName()) ? status : defaultValue(m);
        return (ContainerResponseContext) Proxy.newProxyInstance(
                TracingFilterMetricsTest.class.getClassLoader(),
                new Class<?>[]{ContainerResponseContext.class}, handler);
    }

    private static Object defaultValue(Method m) {
        Class<?> rt = m.getReturnType();
        if (rt == boolean.class) return false;
        if (rt == int.class) return 0;
        if (rt == long.class) return 0L;
        if (rt == double.class) return 0.0;
        if (rt == void.class) return null;
        return null;
    }
}
