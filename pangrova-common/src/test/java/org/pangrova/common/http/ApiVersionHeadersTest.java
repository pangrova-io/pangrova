package org.pangrova.common.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ApiVersionHeaders}.
 * Verifies client-side API version header injection and response checking.
 */
class ApiVersionHeadersTest {

    @Test
    void addVersionHeaderSetsCorrectHeader() {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/v1/query/submit"))
                .GET();

        ApiVersionHeaders.addVersionHeader(builder);
        HttpRequest request = builder.build();

        assertThat(request.headers().firstValue(ApiVersionHeaders.HEADER_NAME))
                .isPresent()
                .hasValue(String.valueOf(ApiVersionHeaders.CLIENT_VERSION));
    }

    @Test
    void headerNameMatchesProtocolConstant() {
        // Ensures the client constant stays in sync with the protocol definition
        assertThat(ApiVersionHeaders.HEADER_NAME).isEqualTo("X-Pangrova-Api-Version");
    }

    @Test
    void clientVersionIsPositive() {
        assertThat(ApiVersionHeaders.CLIENT_VERSION).isGreaterThan(0);
    }

    @Test
    void addVersionHeaderReturnsBuilderForChaining() {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:8080/v1/query"))
                .GET();

        HttpRequest.Builder result = ApiVersionHeaders.addVersionHeader(builder);

        assertThat(result).isSameAs(builder);
    }
}
