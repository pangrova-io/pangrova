package org.pangrova.worker.file;

import java.io.IOException;
import java.io.InputStream;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.enums.StorageType;
import org.pangrova.worker.http.endpoint.InternalAuthFilter;
import org.pangrova.worker.http.endpoint.WorkerExceptionMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual 5 GiB LOCAL HTTP transfer; requires space for the complete object. */
@Tag("integration")
class FileSpaceLargeObjectIntegrationTest {
    private static final long SIZE = 5L * 1024 * 1024 * 1024;
    private static final int TEST_TIMEOUT_MINUTES = 10;
    private static final String SECRET = "large-object-test-secret";
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .proxy(ProxySelector.of(null)).connectTimeout(Duration.ofSeconds(10)).build();
    @TempDir
    Path root;
    private String base;

    @Test
    @Timeout(value = TEST_TIMEOUT_MINUTES, unit = TimeUnit.MINUTES)
    void streamsFiveGibibytesAndReadsAcrossFourGibibyteBoundary() throws Exception {
        try (var factory = ObjectStoreAccessorFactory.discover()) {
            Server server = server(factory);
            try {
                server.start();
                base = "http://localhost:" + EmbeddedHttpServerFactory.getPort(server)
                        + "/v1/file-space/object";
                String descriptor = descriptor("write");
                var upload = request(URI.create(base + "?length=" + SIZE))
                        .header("X-Pangrova-File-Descriptor", descriptor)
                        .header("X-Pangrova-File-Signature", FileAccessSignature.sign(SECRET, descriptor))
                        .header("Content-Type", "application/octet-stream")
                        .PUT(HttpRequest.BodyPublishers.ofInputStream(PatternInputStream::new));
                var uploaded = CLIENT.send(upload.build(), HttpResponse.BodyHandlers.ofString());
                assertThat(uploaded.statusCode()).as(uploaded.body()).isEqualTo(200);
                assertThat(JsonMapperFactory.create().readTree(uploaded.body()).get("size").asLong())
                        .isEqualTo(SIZE);

                var head = CLIENT.send(request(uri("read"))
                                .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
                        HttpResponse.BodyHandlers.discarding());
                assertThat(head.statusCode()).isEqualTo(200);
                assertThat(head.headers().firstValueAsLong("Content-Length").orElseThrow()).isEqualTo(SIZE);
                var download = CLIENT.send(request(uri("read")).GET().build(),
                        HttpResponse.BodyHandlers.ofInputStream());
                assertThat(download.statusCode()).isEqualTo(200);
                assertDownloadedObject(download.body());

                long offset = 4L * 1024 * 1024 * 1024 - 256;
                var range = CLIENT.send(request(uri("read"))
                                .header("Range", "bytes=" + offset + "-" + (offset + 511)).GET().build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                assertThat(range.statusCode()).isEqualTo(206);
                assertThat(range.body()).hasSize(512);
                for (int index = 0; index < 512; index++) {
                    assertThat(range.body()[index]).isEqualTo(pattern(offset + index));
                }
                var stale = CLIENT.send(request(uri("read")).header("If-Match", "\"stale\"")
                        .GET().build(), HttpResponse.BodyHandlers.discarding());
                assertThat(stale.statusCode()).isEqualTo(412);
            } finally {
                server.stop();
            }
        }
    }

    private static void assertDownloadedObject(InputStream body) throws IOException {
        byte[] actual = new byte[1024 * 1024];
        byte[] expected = new byte[actual.length];
        var pattern = new PatternInputStream();
        long transferred = 0;
        try (body) {
            int count;
            while ((count = body.readNBytes(actual, 0, actual.length)) > 0) {
                assertThat(pattern.read(expected, 0, count)).isEqualTo(count);
                assertThat(Arrays.mismatch(actual, 0, count, expected, 0, count))
                        .as("byte mismatch in chunk starting at %s", transferred)
                        .isEqualTo(-1);
                transferred += count;
            }
        }
        assertThat(transferred).isEqualTo(SIZE);
    }

    private Server server(ObjectStoreAccessorFactory factory) {
        var config = new ResourceConfig().register(new FileSpaceFileResource(factory, SECRET, event -> {}))
                .register(new InternalAuthFilter(SECRET)).register(WorkerExceptionMapper.class)
                .register(JacksonFeature.class);
        var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(config)), "/*");
        return EmbeddedHttpServerFactory.create(0, handler);
    }

    private String descriptor(String operation) throws Exception {
        long expiresAt = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(TEST_TIMEOUT_MINUTES);
        var grant = new FileAccessDescriptor("local", StorageType.LOCAL, root.toString(), "", "", "",
                false, "stage", "large.bin", operation, expiresAt,
                "00000000-0000-0000-0000-000000000001", "alice", "tenant-a",
                "00000000-0000-0000-0000-000000000002", 1L);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                JsonMapperFactory.create().writeValueAsBytes(grant));
    }

    private URI uri(String operation) throws Exception {
        String descriptor = descriptor(operation);
        return URI.create(base + "?descriptor=" + descriptor + "&signature="
                + FileAccessSignature.sign(SECRET, descriptor));
    }

    private static HttpRequest.Builder request(URI uri) {
        return HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(4));
    }

    private static byte pattern(long offset) {
        // Include high bits so a wrapped 32-bit file offset cannot return matching bytes.
        return (byte) ((offset * 31 + (offset >>> 8) * 17 + (offset >>> 16) * 7
                + (offset >>> 24) * 13 + (offset >>> 32) * 73 + 17) & 255);
    }

    private static final class PatternInputStream extends InputStream {
        private long position;

        @Override
        public int read() {
            return position < SIZE ? Byte.toUnsignedInt(pattern(position++)) : -1;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            java.util.Objects.checkFromIndexSize(offset, length, bytes.length);
            if (length == 0) return 0;
            if (position >= SIZE) return -1;
            int count = (int) Math.min(length, SIZE - position);
            for (int index = 0; index < count; index++) bytes[offset + index] = pattern(position + index);
            position += count;
            return count;
        }
    }
}
