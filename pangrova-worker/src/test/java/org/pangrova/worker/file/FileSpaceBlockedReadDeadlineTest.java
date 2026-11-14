package org.pangrova.worker.file;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.common.enums.StorageType;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real SDK/socket fault injection; this does not qualify an external storage provider. */
class FileSpaceBlockedReadDeadlineTest {
    @Test
    void originalDeadlineActivelyAbortsAStalledHttpBodyWithoutAnotherGet() throws Exception {
        var releaseProvider = new CountDownLatch(1);
        var gets = new AtomicInteger();
        var provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var serverThreads = Executors.newCachedThreadPool();
        provider.setExecutor(serverThreads);
        provider.createContext("/", exchange -> respond(exchange, releaseProvider, gets));
        provider.start();
        var readerThread = new AtomicReference<Thread>();
        var reader = Executors.newSingleThreadExecutor(task -> {
            var thread = new Thread(task, "blocked-file-read-test");
            readerThread.set(thread);
            return thread;
        });
        try (var worker = new FileSpaceFileResource(new ObjectStoreAccessorFactory(java.util.List.of(new org.pangrova.filesystem.s3.object.S3ObjectStoreAccessorProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test-secret"))))),
                "blocked-read-test", JsonMapperFactory.create(), System::currentTimeMillis)) {
            // Warm provider/client construction before starting the receipt's original deadline.
            worker.head(descriptor(provider, "head", Long.MAX_VALUE));
            long deadline = System.currentTimeMillis() + 2_000;
            try (var input = worker.read(descriptor(provider, "read", deadline), 0, -1, null).stream()) {
                assertThat(input.read()).isEqualTo('a');
                var result = reader.submit(() -> input.read());
                await().atMost(Duration.ofSeconds(1)).until(() -> {
                    var thread = readerThread.get();
                    return thread != null && Arrays.stream(thread.getStackTrace()).anyMatch(frame ->
                            frame.getClassName().equals("sun.nio.ch.NioSocketImpl")
                                    && frame.getMethodName().equals("implRead"));
                });
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).cause()
                        .isInstanceOf(IOException.class).hasMessageContaining("expired");
                assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(deadline);
                assertThat(gets.get()).isEqualTo(1);
            }
        } finally {
            releaseProvider.countDown();
            provider.stop(0);
            reader.shutdownNow();
            serverThreads.shutdownNow();
            assertThat(reader.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(serverThreads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static FileAccessDescriptor descriptor(HttpServer provider, String operation, long deadline) {
        return new FileAccessDescriptor("stalled", StorageType.S3, "bucket", "",
                "http://127.0.0.1:" + provider.getAddress().getPort(), "us-east-1", true,
                "space", "item", operation, deadline, "00000000-0000-0000-0000-000000000001",
                "alice", "tenant-a", "00000000-0000-0000-0000-000000000002", 1);
    }

    private static void respond(HttpExchange exchange, CountDownLatch release, AtomicInteger gets)
            throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Length", "4");
            exchange.getResponseHeaders().set("ETag", "\"opaque-etag\"");
            exchange.getResponseHeaders().set("Last-Modified", "Tue, 1 Jan 2030 00:00:00 GMT");
            if (exchange.getRequestMethod().equals("HEAD")) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            gets.incrementAndGet();
            exchange.getResponseHeaders().set("Content-Range", "bytes 0-3/4");
            exchange.sendResponseHeaders(206, 4);
            exchange.getResponseBody().write('a');
            exchange.getResponseBody().flush();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) throw new IOException("Provider was not released");
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IOException(failure);
            }
        }
    }
}
