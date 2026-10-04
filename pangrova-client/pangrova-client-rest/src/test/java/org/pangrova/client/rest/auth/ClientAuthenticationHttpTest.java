package org.pangrova.client.rest.auth;

import org.pangrova.client.rest.PangrovaClientException;
import org.pangrova.client.rest.PangrovaRestClient;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real HTTP header transmission with a rejecting peer; no successful backend authentication or SQL. */
class ClientAuthenticationHttpTest {
    @Test
    void bothTypedSchemesReachTheActualHttpRequestAndDoNotIgnoreRejection() throws Exception {
        var header = new AtomicReference<String>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/not-admitted/state", exchange -> {
            try (exchange) {
                header.set(exchange.getRequestHeaders().getFirst("Authorization"));
                byte[] body = "{\"error_code\":\"UNAUTHENTICATED\",\"message\":\"Rejected\"}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(401, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
        try {
            for (var auth : new ClientAuthentication[]{ClientAuthentication.basic("读者", "密碼:a"),
                    ClientAuthentication.bearer("signed-token")}) {
                try (var client = PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                        .coordinatorPort(server.getAddress().getPort()).authentication(auth).build()) {
                    assertThatThrownBy(() -> client.getQueryStatus("not-admitted"))
                            .isInstanceOf(PangrovaClientException.class);
                    assertThat(header.get()).isEqualTo(auth.authorizationHeader());
                }
            }
        } finally {
            server.stop(0);
        }
    }
}
