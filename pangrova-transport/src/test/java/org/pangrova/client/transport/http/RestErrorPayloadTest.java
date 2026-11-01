package org.pangrova.client.transport.http;

import com.sun.net.httpserver.HttpServer;
import org.pangrova.client.error.PangrovaClientException;
import org.pangrova.client.error.PangrovaProtocolException;
import org.pangrova.client.error.PangrovaServerException;
import org.pangrova.client.session.RetryPolicy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestErrorPayloadTest {
    @ParameterizedTest
    @ValueSource(strings = {"not json", "null", "{}", "{\"error_code\":\"\",\"message\":\"invalid\"}"})
    void malformedServerErrorRetainsHttpClassificationAndCause(String body) throws Exception {
        assertMalformedError(body, 503, PangrovaServerException.class, "SERVER_ERROR");
    }

    @ParameterizedTest
    @ValueSource(strings = {"not json", "null", "{}", "{\"error_code\":\"INVALID\"}"})
    void malformedClientErrorRetainsHttpClassificationAndCause(String body) throws Exception {
        assertMalformedError(body, 400, PangrovaProtocolException.class, "HTTP_400");
    }

    private void assertMalformedError(String body, int status,
                                      Class<? extends PangrovaClientException> expectedType,
                                      String expectedCode) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/query/submit", exchange -> {
            try (exchange) {
                byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, payload.length);
                exchange.getResponseBody().write(payload);
            }
        });
        server.start();
        try (var client = PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                .coordinatorPort(server.getAddress().getPort()).retryPolicy(RetryPolicy.NONE).build()) {
            assertThatThrownBy(() -> client.submitQuery("SELECT 1", Map.of()))
                    .isInstanceOf(expectedType).hasCauseInstanceOf(Exception.class)
                    .satisfies(error -> {
                        var typed = (PangrovaClientException) error;
                        assertThat(typed.errorCode()).isEqualTo(expectedCode);
                        if (typed instanceof PangrovaServerException serverError) {
                            assertThat(serverError.httpStatusCode()).isEqualTo(status);
                        } else {
                            assertThat(((PangrovaProtocolException) typed).httpStatusCode()).isEqualTo(status);
                        }
                    });
        } finally {
            server.stop(0);
        }
    }
}
