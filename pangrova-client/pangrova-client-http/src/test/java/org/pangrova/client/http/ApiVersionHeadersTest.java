package org.pangrova.client.http;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.session.ConnectionPoolConfig;
import org.pangrova.common.client.session.RetryPolicy;
import org.pangrova.common.client.constants.ApiVersion;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiVersionHeadersTest {
    private static final String VERSION = Integer.toString(ApiVersion.CURRENT);

    @Test
    void versionHeaderIsUniqueWhenAppliedTwice() {
        var request = HttpRequest.newBuilder(URI.create("http://localhost/"));
        ApiVersionHeaders.addVersionHeader(request);
        ApiVersionHeaders.addVersionHeader(request);
        assertThat(request.build().headers().allValues(ApiVersion.HEADER_NAME)).containsExactly(VERSION);
    }

    @Test
    void allRequestsSendOneVersionIncludingAfterFailover() throws Exception {
        var paths = new CopyOnWriteArrayList<String>();
        var headers = new CopyOnWriteArrayList<List<String>>();
        var first = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var next = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try {
            for (var server : List.of(first, next)) {
                server.createContext("/", exchange -> {
                    try (exchange) {
                        exchange.getRequestBody().readAllBytes();
                        var path = exchange.getRequestURI().getPath();
                        paths.add(path);
                        headers.add(exchange.getRequestHeaders().getOrDefault(ApiVersion.HEADER_NAME, List.of()));
                        boolean unavailable = server == first && path.endsWith("/state");
                        boolean unsupported = path.contains("/unsupported/");
                        int status = unavailable ? 503 : unsupported ? 422 : path.endsWith("/cancel") ? 202 : 200;
                        String payload = switch (path) {
                            case "/v1/query/q/cancel" -> """
                                    {"query_id":"q","outcome":"CANCEL_ACCEPTED","cancelled":true}""";
                            case "/v1/namespaces" -> """
                                    {"namespaces":[["test"]]}""";
                            case "/v1/query/unsupported/state" -> """
                                    {"error_code":"API_VERSION_UNSUPPORTED","message":"rejected"}""";
                            default -> """
                                    {"query_id":"q","state":"RUNNING"}""";
                        };
                        var body = payload.getBytes(StandardCharsets.UTF_8);
                        if (!path.equals("/v1/health")) {
                            // A newer server can accept this client's request. Error responses can also carry headers.
                            exchange.getResponseHeaders().set(ApiVersion.HEADER_NAME,
                                    unavailable ? "invalid" : path.endsWith("/cancel") ? "99" : VERSION);
                        }
                        exchange.sendResponseHeaders(status, body.length);
                        exchange.getResponseBody().write(body);
                    }
                });
                server.start();
            }
            var endpoints = List.of("127.0.0.1:" + first.getAddress().getPort(), "127.0.0.1:" + next.getAddress().getPort());
            try (var client = PangrovaRestClient.builder().connectionPoolConfig(ConnectionPoolConfig.withFailover(endpoints))
                    .retryPolicy(new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1)).build()) {
                client.ping(2);
                assertThat(client.submitQuery("SELECT 1", Map.of()).queryId()).isEqualTo("q");
                assertThat(client.getQueryStatus("q").state()).isEqualTo("RUNNING");
                assertThat(client.listCatalogs()).containsExactly("test");
                assertThat(client.cancelQuery("q").name()).isEqualTo("CANCEL_ACCEPTED");
                assertThatThrownBy(() -> client.getQueryStatus("unsupported"))
                        .isInstanceOfSatisfying(PangrovaClientException.class,
                                e -> assertThat(e.errorCode()).isEqualTo(ApiVersion.ERROR_CODE_UNSUPPORTED));
            }
            assertThat(paths).containsExactly("/v1/health", "/v1/query/submit", "/v1/query/q/state", "/v1/query/q/state",
                    "/v1/namespaces", "/v1/query/q/cancel", "/v1/query/unsupported/state");
            assertThat(headers).allSatisfy(values -> assertThat(values).containsExactly(VERSION));
        } finally {
            next.stop(0);
            first.stop(0);
        }
    }
}
