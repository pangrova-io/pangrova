package org.pangrova.ai.worker.blob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.ai.worker.context.AiExecutionContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpsBlobImporterTest {

    private static final String HOST = "files.example.test";
    private static final Set<String> MIME_TYPES = Set.of("text/plain", "image/png");

    @TempDir
    Path temporaryDirectory;

    @Test
    void importsGzipWithinIndependentWireAndDecodedLimits() throws Exception {
        byte[] decoded = "governed remote content".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(decoded);
        GovernedBlobStore store = store(1024);
        HttpsBlobImporter importer = importer(
                store, 1024, 1024,
                response(200, Map.of(
                        "Content-Type", List.of("text/plain; charset=utf-8"),
                        "Content-Encoding", List.of("gzip"),
                        "Content-Length", List.of(Integer.toString(compressed.length))),
                        compressed),
                publicAddress());

        BlobReference reference = governed(() -> importer.importUrl(
                "https://files.example.test/content?id=1"));

        assertThat(reference.mimeType()).isEqualTo("text/plain");
        assertThat(reference.size()).isEqualTo(decoded.length);
        assertThat(governed(() -> store.read(reference))).isEqualTo(decoded);
    }

    @Test
    void rejectsNonHttpsAndNonAllowlistedHostsBeforeTransport() throws Exception {
        GovernedBlobStore store = store(1024);
        HttpsBlobImporter importer = importer(
                store, 1024, 1024,
                (uri, addresses, timeout) -> {
                    throw new AssertionError("transport must not be called");
                },
                publicAddress());

        assertThatThrownBy(() -> governed(() -> importer.importUrl(
                "http://files.example.test/content")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HTTPS");
        assertThatThrownBy(() -> governed(() -> importer.importUrl(
                "https://unlisted.example.test/content")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED));
    }

    @Test
    void rejectsEveryPrivateAndSpecialResolvedAddress() throws Exception {
        for (String address : List.of(
                "127.0.0.1", "10.0.0.1", "100.64.0.1", "169.254.169.254",
                "172.16.0.1", "192.168.1.1", "198.18.0.1", "224.0.0.1",
                "::1", "fc00::1", "fe80::1", "2001:db8::1")) {
            GovernedBlobStore store = store(1024);
            HttpsBlobImporter importer = importer(
                    store, 1024, 1024,
                    (uri, addresses, timeout) -> {
                        throw new AssertionError("transport must not be called");
                    },
                    InetAddress.getByName(address));

            assertThatThrownBy(() -> governed(() -> importer.importUrl(
                    "https://files.example.test/content")))
                    .as("address %s", address)
                    .isInstanceOf(PangrovaException.class)
                    .satisfies(failure -> assertThat(
                            ((PangrovaException) failure).getErrorCode())
                            .isEqualTo(PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED));
        }
    }

    @Test
    void rejectsRedirectWithoutFollowingLocation() throws Exception {
        GovernedBlobStore store = store(1024);
        HttpsBlobImporter importer = importer(
                store, 1024, 1024,
                response(302, Map.of(
                        "Location", List.of("https://127.0.0.1/metadata")), new byte[0]),
                publicAddress());

        assertThatThrownBy(() -> governed(() -> importer.importUrl(
                "https://files.example.test/content")))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("redirects are disabled");
    }

    @Test
    void passesTheValidatedAddressToThePinnedTransport() throws Exception {
        InetAddress first = publicAddress();
        InetAddress second = InetAddress.getByAddress(
                new byte[] {1, 1, 1, 1});
        AtomicReference<List<InetAddress>> observed = new AtomicReference<>();
        HttpsBlobImporter importer = new HttpsBlobImporter(
                store(1024), settings(1024, 1024),
                ignored -> new InetAddress[] {first, second},
                (uri, addresses, timeout) -> {
                    observed.set(addresses);
                    return response(200, Map.of(
                            "Content-Type", List.of("text/plain")),
                            new byte[] {1}).fetch(uri, addresses, timeout);
                });

        governed(() -> importer.importUrl("https://files.example.test/content"));

        assertThat(observed.get()).containsExactly(first, second);
        var target = PinnedHttpsTransport.pinnedTarget(
                URI.create("https://files.example.test/content"), first);
        assertThat(target.getAddress()).isEqualTo(first);
        assertThat(target.getHostName()).isEqualTo(HOST);
        assertThat(target.getPort()).isEqualTo(443);
    }

    @Test
    void clipsTransportReadTimeoutToTaskDeadline() throws Exception {
        Instant now = Instant.parse("2026-09-24T00:00:00Z");
        InetAddress address = publicAddress();
        AtomicReference<Duration> observed = new AtomicReference<>();
        HttpsBlobImporter importer = new HttpsBlobImporter(
                store(1024), settings(1024, 1024),
                ignored -> new InetAddress[] {address},
                (uri, addresses, timeout) -> {
                    observed.set(timeout);
                    throw new java.io.IOException("stop before content transfer");
                }, Clock.fixed(now, ZoneOffset.UTC));
        AiExecutionContext context = org.pangrova.ai.worker.context.AiTestTaskAuthority.context(
                "tenant-a", "alice", "query-1", "task-1", "trace-1",
                now.plusMillis(75).toEpochMilli(), 1024);

        assertThatThrownBy(() -> AiExecutionContext.callWith(context,
                () -> importer.importUrl("https://files.example.test/content")))
                .isInstanceOf(PangrovaException.class);
        assertThat(observed.get()).isEqualTo(Duration.ofMillis(75));
    }

    @Test
    void expiredTaskDeadlineRejectsBeforeTransport() throws Exception {
        Instant now = Instant.parse("2026-09-24T00:00:00Z");
        InetAddress address = publicAddress();
        HttpsBlobImporter importer = new HttpsBlobImporter(
                store(1024), settings(1024, 1024),
                ignored -> new InetAddress[] {address},
                (uri, addresses, timeout) -> {
                    throw new AssertionError("transport must not be called");
                }, Clock.fixed(now, ZoneOffset.UTC));
        AiExecutionContext context = org.pangrova.ai.worker.context.AiTestTaskAuthority.context(
                "tenant-a", "alice", "query-1", "task-1", "trace-1",
                now.minusMillis(1).toEpochMilli(), 1024);

        assertThatThrownBy(() -> AiExecutionContext.callWith(context,
                () -> importer.importUrl("https://files.example.test/content")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deadline has expired");
    }

    @Test
    void enforcesWireDecodedAndMimeLimitsBeforePublication() throws Exception {
        GovernedBlobStore wireStore = store(1024);
        HttpsBlobImporter wireImporter = importer(
                wireStore, 3, 1024,
                response(200, Map.of(
                        "Content-Type", List.of("text/plain"),
                        "Content-Length", List.of("4")), new byte[] {1, 2, 3, 4}),
                publicAddress());
        assertSizeFailure(wireImporter);

        byte[] compressed = gzip("decoded-too-large".getBytes(StandardCharsets.UTF_8));
        GovernedBlobStore decodedStore = store(1024);
        HttpsBlobImporter decodedImporter = importer(
                decodedStore, compressed.length, 4,
                response(200, Map.of(
                        "Content-Type", List.of("text/plain"),
                        "Content-Encoding", List.of("gzip")), compressed),
                publicAddress());
        assertSizeFailure(decodedImporter);

        GovernedBlobStore mimeStore = store(1024);
        HttpsBlobImporter mimeImporter = importer(
                mimeStore, 1024, 1024,
                response(200, Map.of(
                        "Content-Type", List.of("application/x-executable")),
                        new byte[] {1}),
                publicAddress());
        assertThatThrownBy(() -> governed(() -> mimeImporter.importUrl(
                "https://files.example.test/content")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_INVOCATION_AUTHORIZATION_DENIED));
    }

    private GovernedBlobStore store(long maximum) {
        return BlobTestStores.create(temporaryDirectory, maximum, MIME_TYPES);
    }

    private static HttpsBlobImporter importer(
            GovernedBlobStore store,
            long wireLimit,
            long decodedLimit,
            HttpsBlobImporter.HttpsTransport transport,
            InetAddress address) {
        return new HttpsBlobImporter(
                store, settings(wireLimit, decodedLimit),
                ignored -> new InetAddress[] {address}, transport);
    }

    private static HttpsBlobImporter.Settings settings(
            long wireLimit, long decodedLimit) {
        return new HttpsBlobImporter.Settings(
                Set.of(HOST), MIME_TYPES, Duration.ofSeconds(1), Duration.ofSeconds(1),
                wireLimit, decodedLimit,
                new HttpsBlobImporter.PoolSettings(
                        8, 4, Duration.ofMinutes(1), Duration.ofSeconds(10)));
    }

    private static HttpsBlobImporter.HttpsTransport response(
            int status, Map<String, List<String>> headers, byte[] content) {
        return (uri, addresses, timeout) -> new HttpsBlobImporter.DownloadResponse(
                status, headers, new ByteArrayInputStream(content));
    }

    private static InetAddress publicAddress() throws Exception {
        return InetAddress.getByAddress(new byte[] {93, (byte) 184, (byte) 216, 34});
    }

    private static byte[] gzip(byte[] content) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
            gzip.write(content);
        }
        return bytes.toByteArray();
    }

    private static void assertSizeFailure(HttpsBlobImporter importer) {
        assertThatThrownBy(() -> governed(() -> importer.importUrl(
                "https://files.example.test/content")))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(
                        ((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.AI_BLOB_SIZE_EXCEEDED));
    }

    private static <T> T governed(Supplier<T> action) {
        return org.pangrova.ai.worker.context.AiTestTaskAuthority.callWithRegisteredTask(
                "tenant-a", "alice", Instant.now().plus(Duration.ofMinutes(1)).toEpochMilli(),
                1024, action);
    }
}
