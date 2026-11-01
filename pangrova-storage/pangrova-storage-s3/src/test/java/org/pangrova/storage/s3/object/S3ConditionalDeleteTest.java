package org.pangrova.storage.s3.object;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.object.ObjectLocation;
import org.pangrova.file.api.profile.StorageProfile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK HTTP requests against a conditional-delete protocol fixture, not cloud qualification. */
class S3ConditionalDeleteTest {
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "null")
    void replacementAfterHeadSurvivesTheEarlierConditionalDelete(String version) throws Exception {
        try (var fixture = new Fixture(version, "\"before\"", true)) {
            assertThatThrownBy(() -> fixture.accessor.delete(fixture.location, "item", "\"before\""))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));
            assertThat(fixture.current.get()).isEqualTo("\"replacement\"");
            assertThat(fixture.condition.get()).isEqualTo("\"before\"");
            assertThat(fixture.authorization.get()).contains("if-match");
            assertThat(fixture.deletes).hasValue(1);
        }
    }

    @Test
    void unchangedUnversionedObjectIsConditionallyDeleted() throws Exception {
        try (var fixture = new Fixture(null, "\"before\"", false)) {
            fixture.accessor.delete(fixture.location, "item", null);
            assertThat(fixture.current.get()).isNull();
            assertThat(fixture.condition.get()).isEqualTo("\"before\"");
            assertThat(fixture.authorization.get()).contains("if-match");
            assertThat(fixture.deletes).hasValue(1);
        }
    }

    @Test
    void missingImmutableIdentityAndEtagNeverIssueAnUnconditionalDelete() throws Exception {
        try (var fixture = new Fixture(null, null, false)) {
            assertThatThrownBy(() -> fixture.accessor.delete(fixture.location, "item", null))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
            assertThat(fixture.deletes).hasValue(0);
        }
    }

    @Test
    void immutableVersionDeleteDoesNotRemoveTheReplacementCurrentObject() throws Exception {
        try (var fixture = new Fixture("version-one", "\"before\"", true)) {
            fixture.accessor.delete(fixture.location, "item", "version-one");
            assertThat(fixture.current.get()).isEqualTo("\"replacement\"");
            assertThat(fixture.query.get()).isEqualTo("versionId=version-one");
            assertThat(fixture.condition.get()).isNull();
        }
    }

    @Test
    void unqualifiedCustomEndpointNeverReceivesMutableVersionDelete() throws Exception {
        try (var fixture = new Fixture(null, "\"before\"", false, false)) {
            assertThatThrownBy(() -> fixture.accessor.delete(fixture.location, "item", null))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
            assertThat(fixture.deletes).hasValue(0);
            assertThat(fixture.current.get()).isEqualTo("\"before\"");
        }
    }

    @Test
    void providerAssemblyUsesItsExplicitlyQualifiedCustomEndpointPolicy() throws Exception {
        try (var fixture = new Fixture(null, "\"before\"", false);
                var provider = new S3ObjectStoreAccessorProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test-secret")),
                        java.util.Set.of(fixture.location.profile().endpoint()))) {
            provider.accessor(fixture.location.profile()).delete(fixture.location, "item", null);
            assertThat(fixture.current.get()).isNull();
            assertThat(fixture.condition.get()).isEqualTo("\"before\"");
            assertThat(fixture.authorization.get()).contains("if-match");
            assertThat(fixture.deletes).hasValue(1);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final HttpServer server;
        private final S3Client client;
        private final S3ObjectStoreAccessor accessor;
        private final ObjectLocation location;
        private final AtomicReference<String> current = new AtomicReference<>("\"before\"");
        private final AtomicReference<String> condition = new AtomicReference<>();
        private final AtomicReference<String> query = new AtomicReference<>();
        private final AtomicReference<String> authorization = new AtomicReference<>();
        private final AtomicInteger deletes = new AtomicInteger();

        Fixture(String version, String etag, boolean replaceAfterHead) throws Exception {
            this(version, etag, replaceAfterHead, true);
        }

        Fixture(String version, String etag, boolean replaceAfterHead, boolean qualified) throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                try (exchange) {
                    if (exchange.getRequestMethod().equals("HEAD")) {
                        if (etag != null) exchange.getResponseHeaders().set("ETag", etag);
                        if (version != null && !version.isEmpty()) {
                            exchange.getResponseHeaders().set("x-amz-version-id", version);
                        }
                        exchange.getResponseHeaders().set("Last-Modified", "Wed, 01 Jan 2020 00:00:00 GMT");
                        exchange.getResponseHeaders().set("Content-Length", "3");
                        exchange.sendResponseHeaders(200, -1);
                        if (replaceAfterHead) current.set("\"replacement\"");
                    } else {
                        deletes.incrementAndGet();
                        condition.set(exchange.getRequestHeaders().getFirst("If-Match"));
                        authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                        query.set(exchange.getRequestURI().getRawQuery());
                        if ("versionId=version-one".equals(query.get())) {
                            exchange.sendResponseHeaders(204, -1);
                        } else if (condition.get() != null && !condition.get().equals(current.get())) {
                            byte[] body = "<Error><Code>PreconditionFailed</Code></Error>"
                                    .getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(412, body.length);
                            exchange.getResponseBody().write(body);
                        } else {
                            current.set(null);
                            exchange.sendResponseHeaders(204, -1);
                        }
                    }
                }
            });
            server.start();
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            client = S3Client.builder().endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1)
                    .forcePathStyle(true).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test-secret"))).build();
            accessor = new S3ObjectStoreAccessor(client, null,
                    qualified ? java.util.Set.of(endpoint) : java.util.Set.of());
            location = new ObjectLocation(new StorageProfile("http", StorageType.S3, "bucket", "", endpoint,
                    "us-east-1", Map.of(), Map.of("path-style-access", "true")), "");
        }

        @Override public void close() { client.close(); server.stop(0); }
    }
}
