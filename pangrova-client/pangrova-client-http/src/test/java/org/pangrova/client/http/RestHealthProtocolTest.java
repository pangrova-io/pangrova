package org.pangrova.client.http;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.client.http.authentication.ClientAuthentication;
import org.pangrova.common.concurrency.ExecutorDrain;
import org.pangrova.common.concurrency.ManagedExecutorFactory;
import org.pangrova.common.client.exception.PangrovaClientException;
import org.pangrova.common.client.session.RetryPolicy;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Actual HTTP health protocol and credential transmission, not backend authentication or SQL execution. */
class RestHealthProtocolTest {
    @Test
    void exactReadyStatusSucceeds() throws Exception {
        try (var peer = new HealthPeer(200); var client = peer.client(null)) {
            assertThatCode(() -> client.ping(2)).doesNotThrowAnyException();
            assertThat(peer.path.get()).isEqualTo("/v1/health");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {201, 202, 204, 206, 301, 302, 307, 400, 401, 403, 404, 429, 500, 503})
    void everyNonReadyStatusIsRejected(int status) throws Exception {
        try (var peer = new HealthPeer(status); var client = peer.client(null)) {
            assertThatThrownBy(() -> client.ping(2)).isInstanceOf(PangrovaClientException.class);
        }
    }

    @Test
    void basicCredentialsReachTheHealthRequestEvenWhenRejected() throws Exception {
        verifyAuthenticatedRejection(ClientAuthentication.basic("读者", "密碼:a"));
    }

    @Test
    void bearerCredentialsReachTheHealthRequestEvenWhenRejected() throws Exception {
        verifyAuthenticatedRejection(ClientAuthentication.bearer("health-test-token"));
    }

    private static void verifyAuthenticatedRejection(ClientAuthentication authentication) throws Exception {
        try (var peer = new HealthPeer(401); var client = peer.client(authentication)) {
            Throwable failure = catchThrowable(() -> client.ping(2));
            assertThat(peer.authorization.get()).isEqualTo(authentication.authorizationHeader());
            assertThat(peer.path.get()).isEqualTo("/v1/health");
            assertThat(failure).isInstanceOf(PangrovaClientException.class);
        }
    }

    private static final class HealthPeer implements AutoCloseable {
        private final ExecutorService executor = ManagedExecutorFactory.newSingleThreadExecutor("rest-health-protocol");
        private final AtomicReference<String> authorization = new AtomicReference<>();
        private final AtomicReference<String> path = new AtomicReference<>();
        private HttpServer server;

        private HealthPeer(int status) throws Exception {
            try {
                server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
                server.setExecutor(executor);
                server.createContext("/v1/health", exchange -> {
                    try (exchange) {
                        exchange.getRequestBody().readAllBytes();
                        path.set(exchange.getRequestURI().getPath());
                        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                        byte[] body = (status == 200 ? "{\"ready\":true}" : "{\"ready\":false}")
                                .getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(status, status == 204 ? -1 : body.length);
                        if (status != 204) exchange.getResponseBody().write(body);
                    }
                });
                server.start();
            } catch (Exception | Error failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        private PangrovaRestClient client(ClientAuthentication authentication) {
            var builder = PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                    .coordinatorPort(server.getAddress().getPort()).httpExecutorThreads(1).retryPolicy(RetryPolicy.NONE);
            if (authentication != null) builder.authentication(authentication);
            return builder.build();
        }

        @Override public void close() {
            if (server != null) server.stop(0);
            ExecutorDrain.shutdownAndAwait(executor, 5_000);
        }
    }
}
