package org.pangrova.worker.file;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.common.storage.StorageType;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK/HTTP fault injection; this does not qualify an S3 provider or a blocked-I/O watchdog. */
class FileSpaceReadAdmissionDeadlineTest {
    private static final String SECRET = "read-admission-deadline-test";
    private static final long EXPIRY = 2_000;
    private static final byte[] CONTENT = "data".getBytes(StandardCharsets.UTF_8);
    private final AtomicLong now = new AtomicLong(1_000);
    private final AtomicInteger heads = new AtomicInteger();
    private final AtomicInteger gets = new AtomicInteger();
    private HttpServer provider;
    private FileSpaceFileResource worker;
    private volatile String expireAt;

    @BeforeEach
    void start() throws IOException {
        provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", this::respond);
        provider.start();
        worker = new FileSpaceFileResource(new ObjectStoreAccessorFactory(java.util.List.of(new org.pangrova.filesystem.s3.object.S3ObjectStoreAccessorProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access", "test-secret"))))),
                SECRET, JsonMapperFactory.create(), now::get);
    }

    @AfterEach
    void close() {
        try {
            if (worker != null) worker.close();
        } finally {
            if (provider != null) provider.stop(0);
        }
    }

    @Test
    void expiryDuringEndpointHeadDoesNotStartAnotherProviderOperation() {
        expireAt = "HEAD";
        assertDenied(() -> worker.read(descriptor(), 0, -1, null));
        assertThat(heads.get()).isEqualTo(1);
        assertThat(gets.get()).isZero();
    }

    @Test
    void expiryDuringGetRejectsBeforeReturningAnHttpSuccessResponse() throws IOException {
        expireAt = "GET";
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(JsonMapperFactory.create().writeValueAsBytes(descriptor()));
        assertDenied(() -> worker.get(encoded, FileAccessSignature.sign(SECRET, encoded),
                null, null, null, null, null, null));
        assertThat(gets.get()).isEqualTo(1);
    }

    @Test
    void expiryDuringHeadCannotReturnSuccessfulHeadMetadata() throws IOException {
        expireAt = "HEAD";
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(JsonMapperFactory.create().writeValueAsBytes(descriptor()));
        assertDenied(() -> worker.headObject(encoded, FileAccessSignature.sign(SECRET, encoded), null, null));
        assertThat(heads.get()).isEqualTo(1);
        assertThat(gets.get()).isZero();
    }

    @Test
    void rejectedOpenedStreamIsClosedAndCleanupFailureDoesNotReplaceTheDenial() {
        now.set(EXPIRY);
        var closes = new AtomicInteger();
        var stream = new ByteArrayInputStream(CONTENT) {
            @Override public void close() throws IOException {
                closes.incrementAndGet();
                throw new IOException("storage close failed");
            }
        };
        assertThatThrownBy(() -> worker.admitRead(descriptor(), stream))
                .isInstanceOfSatisfying(PangrovaException.class, denied -> {
                    assertThat(denied.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED);
                    assertThat(denied.getSuppressed()).singleElement().satisfies(failure ->
                            assertThat(failure).isInstanceOf(IOException.class).hasMessage("storage close failed"));
                });
        assertThat(closes.get()).isEqualTo(1);
    }

    @Test
    void liveReceiptKeepsExactProviderBytesAndTheOriginalDeadline() throws IOException {
        try (var input = worker.read(descriptor(), 0, -1, null).stream()) {
            assertThat(input.readAllBytes()).isEqualTo(CONTENT);
            now.set(EXPIRY);
            assertThatThrownBy(input::read).isInstanceOf(IOException.class).hasMessageContaining("expired");
        }
        assertThat(gets.get()).isEqualTo(1);
    }

    private static void assertDenied(org.assertj.core.api.ThrowableAssert.ThrowingCallable operation) {
        assertThatThrownBy(operation).isInstanceOfSatisfying(PangrovaException.class, denied ->
                assertThat(denied.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    private FileAccessDescriptor descriptor() {
        return new FileAccessDescriptor("deadline", StorageType.S3, "bucket", "",
                "http://127.0.0.1:" + provider.getAddress().getPort(), "us-east-1", true,
                "space", "item", "read", EXPIRY, "00000000-0000-0000-0000-000000000001",
                "alice", "tenant-a", "00000000-0000-0000-0000-000000000002", 1);
    }

    private void respond(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            if (method.equals("HEAD")) heads.incrementAndGet();
            else gets.incrementAndGet();
            if (method.equals(expireAt)) now.set(EXPIRY);
            var headers = exchange.getResponseHeaders();
            headers.set("Content-Length", Integer.toString(CONTENT.length));
            headers.set("ETag", "\"opaque-etag\"");
            headers.set("Last-Modified", "Tue, 1 Jan 2030 00:00:00 GMT");
            if (method.equals("HEAD")) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                headers.set("Content-Range", "bytes 0-3/4");
                exchange.sendResponseHeaders(206, CONTENT.length);
                exchange.getResponseBody().write(CONTENT);
            }
        }
    }
}
