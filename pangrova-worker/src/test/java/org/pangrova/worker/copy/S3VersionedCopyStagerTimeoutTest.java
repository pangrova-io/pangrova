package org.pangrova.worker.copy;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.s3.blob.S3BlobStorage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real HTTP response bodies exercising the SDK stream and local draft cleanup. */
class S3VersionedCopyStagerTimeoutTest {
    @TempDir Path temporaryDirectory;

    @Test
    void stalledGetBodyTimesOutAndLeavesNoDraft() throws Exception {
        assertFaultIsBounded(false, false, Duration.ofMillis(200),
                Duration.ofMinutes(10), Duration.ofSeconds(3));
    }

    @Test
    void rejectedGetVersionAbortsUnreadBodyWithoutDrainingIt() throws Exception {
        assertFaultIsBounded(true, false, Duration.ofSeconds(3),
                Duration.ofMinutes(10), Duration.ofSeconds(2));
    }

    @Test
    void tricklingGetBodyHitsWholeStageWallLimit() throws Exception {
        assertFaultIsBounded(false, true, Duration.ofSeconds(3),
                Duration.ofMillis(350), Duration.ofSeconds(2));
    }

    private void assertFaultIsBounded(boolean wrongGetVersion, boolean trickle,
                                      Duration socketTimeout, Duration stageWallTime,
                                      Duration allowed) throws Exception {
        CountDownLatch releaseBody = new CountDownLatch(1);
        AtomicInteger bodyBytesSent = new AtomicInteger();
        var executor = Executors.newCachedThreadPool();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            try (exchange) {
                exchange.getResponseHeaders().set("x-amz-version-id",
                        wrongGetVersion && "GET".equals(exchange.getRequestMethod())
                                ? "v2" : "v1");
                int contentLength = trickle ? 100 : 2;
                exchange.getResponseHeaders().set("Content-Length", Integer.toString(contentLength));
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(200, -1);
                    return;
                }
                exchange.getResponseHeaders().set("Content-Range", "bytes 0-" + (contentLength - 1) + "/" + contentLength);
                exchange.sendResponseHeaders(206, contentLength);
                exchange.getResponseBody().write('a');
                exchange.getResponseBody().flush();
                bodyBytesSent.incrementAndGet();
                if (trickle) {
                    for (int i = 1; i < contentLength; i++) {
                        try {
                            if (releaseBody.await(60, TimeUnit.MILLISECONDS)) return;
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        exchange.getResponseBody().write('a');
                        exchange.getResponseBody().flush();
                        bodyBytesSent.incrementAndGet();
                    }
                    return;
                }
                try {
                    releaseBody.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        server.start();
        long started = System.nanoTime();
        try (S3Client source = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("copy-timeout", "copy-timeout-secret")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .overrideConfiguration(config -> config
                        .apiCallTimeout(Duration.ofSeconds(4))
                        .apiCallAttemptTimeout(Duration.ofSeconds(3)))
                .httpClientBuilder(ApacheHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(1))
                        .socketTimeout(socketTimeout))
                .build()) {
            var managed = new S3BlobStorage(source, new S3BlobStorage.Settings(
                    "managed-copy", "us-east-1", null, "copy",
                    new S3BlobStorage.Encryption(ServerSideEncryption.AES256, ""), "", true));
            Path drafts = temporaryDirectory.resolve("drafts");
            var stager = new S3VersionedCopyStager(source, managed, drafts, 1024,
                    stageWallTime);
            var profile = new StorageProfile("incoming", StorageType.S3, "source-copy",
                    "data/", "", "us-east-1", Map.of(), Map.of());
            assertThatThrownBy(() -> stager.stage(profile,
                    "s3://source-copy/data/part.parquet", "tenant-one", budget()))
                    .isInstanceOf(IOException.class);
            if (trickle) assertThat(bodyBytesSent.get()).isGreaterThan(1);
            try (var files = Files.list(drafts)) {
                assertThat(files).isEmpty();
            }
            managed.close();
        } finally {
            releaseBody.countDown();
            server.stop(0);
            executor.shutdownNow();
        }
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(allowed);
    }
    private static CopyPreparation budget() {
        return new CopyPreparation(org.pangrova.protocol.v2.StageResourceGrant.newBuilder()
                .setGrantId("stager-test").setCpuSlots(1).setJvmHeapBytes(1024 * 1024)
                .setObjectStoreReadBytes(2 * 1024 * 1024).setObjectStoreWriteBytes(1024 * 1024)
                .setSpillBytes(1024 * 1024).build(), System.currentTimeMillis() + 30_000,
                java.time.Clock.systemUTC());
    }

}
