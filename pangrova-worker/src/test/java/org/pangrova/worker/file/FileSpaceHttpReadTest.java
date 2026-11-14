package org.pangrova.worker.file;

import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.enums.StorageType;
import org.pangrova.worker.http.endpoint.InternalAuthFilter;
import org.pangrova.worker.http.endpoint.WorkerExceptionMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises real HTTP framing, authorization, storage and streaming, without JNI. */
class FileSpaceHttpReadTest {
    private static final String SECRET = "file-http-test-secret";
    // Loopback contract tests must not depend on the workstation's proxy configuration.
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .proxy(ProxySelector.of(null))
            .connectTimeout(Duration.ofSeconds(10)).build();

    @TempDir
    Path root;
    private Server server;
    private ObjectStoreAccessorFactory accessors;
    private String base;

    @BeforeEach
    void start() throws Exception {
        accessors = ObjectStoreAccessorFactory.discover();
        ResourceConfig resources = new ResourceConfig()
                .register(new FileSpaceFileResource(accessors, SECRET, event -> {}))
                .register(new InternalAuthFilter(SECRET))
                .register(WorkerExceptionMapper.class)
                .register(JacksonFeature.class);
        ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(resources)), "/*");
        server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        base = "http://127.0.0.1:" + EmbeddedHttpServerFactory.getPort(server)
                + "/v1/file-space/object";
        put("0123456789");
    }

    @AfterEach
    void stop() throws Exception {
        try {
            if (server != null) {
                server.stop();
            }
        } finally {
            if (accessors != null) {
                accessors.close();
            }
        }
    }

    @Test
    void noLengthDownloadsWholeObjectAndReturnsQuotedEntityTag() throws Exception {
        HttpResponse<String> response = get("", null, null);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("0123456789");
        assertThat(response.headers().firstValue("Content-Length")).contains("10");
        assertThat(response.headers().firstValue("ETag").orElseThrow())
                .startsWith("\"").endsWith("\"");
        assertThat(response.headers().firstValue("Content-Range")).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({"bytes=2-4,234,bytes 2-4/10", "bytes=7-,789,bytes 7-9/10",
            "bytes=-3,789,bytes 7-9/10", "bytes=8-100,89,bytes 8-9/10",
            "bytes=0-9,0123456789,bytes 0-9/10",
            "bytes=-99999999999999999999999,0123456789,bytes 0-9/10",
            "bytes=8-99999999999999999999999,89,bytes 8-9/10"})
    void streamsExactRange(String range, String expected, String contentRange) throws Exception {
        HttpResponse<String> response = get("", range, null);
        assertThat(response.statusCode()).isEqualTo(206);
        assertThat(response.body()).isEqualTo(expected);
        assertThat(response.headers().firstValue("Content-Length"))
                .contains(Integer.toString(expected.length()));
        assertThat(response.headers().firstValue("Content-Range")).contains(contentRange);
    }

    @Test
    void preconditionsUseStrongTagsAndPrecedeRangeEvaluation() throws Exception {
        String etag = get("", null, null).headers().firstValue("ETag").orElseThrow();
        assertThat(get("", "bytes=0-1", etag).body()).isEqualTo("01");
        assertThat(get("", null, "\"a,b\", " + etag).statusCode()).isEqualTo(200);
        assertThat(get("", null, "*").statusCode()).isEqualTo(200);
        assertThat(get("", null, "W/" + etag).statusCode()).isEqualTo(412);
        HttpResponse<String> mismatch = get("", "bytes=100-", "\"stale\"");
        assertThat(mismatch.statusCode()).isEqualTo(412);
        assertThat(mismatch.body()).contains("FILE-0002");
        assertThat(get("&version=stale", null, null).statusCode()).isEqualTo(412);
        assertThat(get("", null, "unquoted").statusCode()).isEqualTo(400);
        put("replacement");
        assertThat(get("", null, etag).statusCode()).isEqualTo(412);
    }

    @Test
    void headReturnsReadMetadataWithoutBody() throws Exception {
        HttpResponse<String> response = send(HttpRequest.newBuilder(uri("read", ""))
                .method("HEAD", HttpRequest.BodyPublishers.noBody()));
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEmpty();
        assertThat(response.headers().firstValue("Content-Length")).contains("10");
        assertThat(response.headers().firstValue("ETag"))
                .isEqualTo(get("", null, null).headers().firstValue("ETag"));
    }

    @Test
    void ifRangeResumesOnlyWhenStrongTagStillMatches() throws Exception {
        String etag = get("", null, null).headers().firstValue("ETag").orElseThrow();
        HttpResponse<String> current = send(HttpRequest.newBuilder(uri("read", ""))
                .header("Range", "bytes=2-4").header("If-Range", etag).GET());
        assertThat(current.statusCode()).isEqualTo(206);
        assertThat(current.body()).isEqualTo("234");
        for (String validator : new String[]{"\"stale\"", "W/" + etag,
                "Wed, 21 Oct 2015 07:28:00 GMT"}) {
            HttpResponse<String> response = send(HttpRequest.newBuilder(uri("read", ""))
                    .header("Range", "bytes=999-").header("If-Range", validator).GET());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("0123456789");
        }
    }

    @Test
    void unsatisfiableRangesCarryObjectSize() throws Exception {
        for (String range : new String[]{"bytes=10-", "bytes=-0", "bytes=5-4",
                "bytes=99999999999999999999999-"}) {
            HttpResponse<String> response = get("", range, null);
            assertThat(response.statusCode()).as(range).isEqualTo(416);
            assertThat(response.headers().firstValue("Content-Range")).contains("bytes */10");
        }
        assertThat(get("", "bytes=abc", null).statusCode()).isEqualTo(400);
        assertThat(get("&offset=1", "bytes=1-2", null).statusCode()).isEqualTo(400);
        assertThat(get("&offset=-1", null, null).statusCode()).isEqualTo(416);
        assertThat(get("&length=-2", null, null).statusCode()).isEqualTo(416);
        assertThat(get("&length=0", null, null).statusCode()).isEqualTo(416);
        assertThat(get("&offset=8", null, null).body()).isEqualTo("89");
        assertThat(get("&offset=2&length=3", null, null).body()).isEqualTo("234");
    }

    @Test
    void unsupportedRangeFormsReturnCompleteRepresentation() throws Exception {
        for (String range : new String[]{"items=0-1", "bytes=0-1,5-6"}) {
            HttpResponse<String> response = get("", range, null);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).isEqualTo("0123456789");
        }
    }

    @Test
    void emptyObjectHasNoInvalidContentRange() throws Exception {
        put("");
        HttpResponse<String> response = get("", null, null);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEmpty();
        assertThat(response.headers().firstValue("Content-Length")).contains("0");
        assertThat(response.headers().firstValue("Content-Range")).isEmpty();
        HttpResponse<String> range = get("", "bytes=0-", null);
        assertThat(range.statusCode()).isEqualTo(416);
        assertThat(range.headers().firstValue("Content-Range")).contains("bytes */0");
    }

    @Test
    void invalidSignatureAndWrongOperationAreForbidden() throws Exception {
        String encoded = descriptor("read");
        HttpResponse<String> forged = send(HttpRequest.newBuilder(URI.create(base
                + "?descriptor=" + encoded + "&signature=forged")).GET());
        assertThat(forged.statusCode()).isEqualTo(403);
        assertThat(forged.body()).doesNotContain(encoded);
        assertThat(send(HttpRequest.newBuilder(uri("write", "")).GET()).statusCode())
                .isEqualTo(403);
    }

    @Test
    void bodylessDeleteUsesTheAuthorizedObjectPath() throws Exception {
        assertThat(send(HttpRequest.newBuilder(uri("delete", "")).DELETE()).statusCode())
                .isEqualTo(204);
        assertThat(get("", null, null).statusCode()).isEqualTo(404);
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "PUT", "DELETE"})
    void signedObjectAccessCannotFollowDirectoryLinks(String method) throws Exception {
        Path privateDirectory = Files.createDirectory(root.resolve("private"));
        Path secret = Files.writeString(privateDirectory.resolve("secret.txt"), "secret");
        Files.createSymbolicLink(root.resolve("volumes/media/raw/landing/link"), privateDirectory);
        String operation = switch (method) {
            case "PUT" -> "write";
            case "DELETE" -> "delete";
            default -> "read";
        };
        String encoded = descriptor(operation, "link/secret.txt");
        String signature = FileAccessSignature.sign(SECRET, encoded);
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base
                + "?descriptor=" + encoded + "&signature=" + signature))
                .header("X-Pangrova-File-Descriptor", encoded)
                .header("X-Pangrova-File-Signature", signature)
                .method(method, method.equals("PUT") ? HttpRequest.BodyPublishers.ofString("changed")
                        : HttpRequest.BodyPublishers.noBody());

        assertThat(send(request).statusCode()).isEqualTo(400);
        assertThat(Files.readString(secret)).isEqualTo("secret");
    }

    private void put(String content) throws Exception {
        String descriptor = descriptor("write");
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(base))
                .header("X-Pangrova-File-Descriptor", descriptor)
                .header("X-Pangrova-File-Signature", FileAccessSignature.sign(SECRET, descriptor))
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofString(content)));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    private HttpResponse<String> get(String query, String range, String ifMatch) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri("read", query)).GET();
        if (range != null) {
            request.header("Range", range);
        }
        if (ifMatch != null) {
            request.header("If-Match", ifMatch);
        }
        return send(request);
    }

    private URI uri(String operation, String query) throws Exception {
        String descriptor = descriptor(operation);
        return URI.create(base + "?descriptor=" + descriptor + "&signature="
                + FileAccessSignature.sign(SECRET, descriptor) + query);
    }

    private String descriptor(String operation) throws Exception {
        return descriptor(operation, "a.bin");
    }

    private String descriptor(String operation, String path) throws Exception {
        FileAccessDescriptor descriptor = new FileAccessDescriptor("local", StorageType.LOCAL,
                root.toString(), "", "", "", false, "volumes/media/raw/landing", path,
                operation, System.currentTimeMillis() + 60_000, "00000000-0000-0000-0000-000000000001", "alice", "tenant-a", "00000000-0000-0000-0000-000000000002", 1L);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                JsonMapperFactory.create().writeValueAsBytes(descriptor));
    }

    private static HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return CLIENT.send(request.timeout(Duration.ofSeconds(10)).build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
