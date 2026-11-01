package org.pangrova.filesystem.s3.object;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.filesystem.api.object.ObjectLocation;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration isolation tested through actual local writes and SDK-signed HTTP requests. */
class S3ObjectStoreAccessorProviderIdentityTest {

    @Test
    void rotatingSessionCredentialsChangeBothSdkRequestsAndPresignedUrls() throws Exception {
        var receivedTokens = java.util.Collections.synchronizedList(new ArrayList<String>());
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                receivedTokens.add(exchange.getRequestHeaders().getFirst("X-Amz-Security-Token"));
                exchange.getResponseHeaders().set("Content-Length", "1");
                exchange.getResponseHeaders().set("Last-Modified", "Wed, 01 Jan 2020 00:00:00 GMT");
                exchange.getResponseHeaders().set("ETag", "\"stored\"");
                exchange.sendResponseHeaders(200, -1);
            }
        });
        server.start();
        try (var factory = new S3ObjectStoreAccessorProvider(AnonymousCredentialsProvider.create())) {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            var first = s3(endpoint, "secret-one", "token-one");
            var second = s3(endpoint, "secret-two", "token-two");
            factory.accessor(first).head(new ObjectLocation(first, "stage"), "item");
            factory.accessor(second).head(new ObjectLocation(second, "stage"), "item");
            assertThat(receivedTokens).containsExactly("token-one", "token-two");
            String signed = factory.accessor(second).presign(new ObjectLocation(second, "stage"), "item",
                    Duration.ofMinutes(1), true).url();
            assertThat(java.net.URI.create(signed).getRawQuery()).contains("X-Amz-Security-Token=token-two");
        } finally {
            server.stop(0);
        }
    }

    @Test
    void secretOnlyRotationDoesNotReuseAClientWithThePreviousSecret() {
        try (var factory = new S3ObjectStoreAccessorProvider(AnonymousCredentialsProvider.create())) {
            var first = factory.accessor(s3("http://127.0.0.1", "secret-one", "same-token"));
            var second = factory.accessor(s3("http://127.0.0.1", "secret-two", "same-token"));
            assertThat(second).isNotSameAs(first);
        }
    }

    @ParameterizedTest
    @MethodSource("invalidCredentials")
    void explicitIncompleteOrUnsupportedCredentialsNeverSelectTheDefaultChain(Map<String, String> credentials) {
        var profile = new StorageProfile("s3", StorageType.S3, "test-bucket", "", "", "us-east-1",
                credentials, Map.of());
        try (var factory = new S3ObjectStoreAccessorProvider(AnonymousCredentialsProvider.create())) {
            assertThatThrownBy(() -> factory.accessor(profile)).isInstanceOfSatisfying(PangrovaException.class,
                    failure -> assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
        }
    }

    private static Stream<Map<String, String>> invalidCredentials() {
        return Stream.of(Map.of("access-key-id", "id"), Map.of("secret-access-key", "secret"),
                Map.of("session-token", "token"), Map.of("role-arn", "arn:aws:iam::123456789012:role/test"),
                Map.of("access-key-id", "id", "secret-access-key", " "));
    }

    private static StorageProfile s3(String endpoint, String secret, String token) {
        return new StorageProfile("same-profile", StorageType.S3, "test-bucket", "", endpoint, "us-east-1",
                Map.of("access-key-id", "same-access-key", "secret-access-key", secret, "session-token", token),
                Map.of("path-style-access", "true"));
    }
}
