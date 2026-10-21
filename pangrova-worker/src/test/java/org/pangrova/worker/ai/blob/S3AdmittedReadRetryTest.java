package org.pangrova.worker.ai.blob;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.pangrova.worker.copy.CopyTaskReadBudget;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK HTTP retries must never create uncharged admitted object reads. */
class S3AdmittedReadRetryTest {
    @Test
    void admittedGetCannotRetryOutsideItsGrantOrChangeOrdinaryClientRetryPolicy() throws Exception {
        var requests = new AtomicInteger();
        var ranged = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (exchange) {
                requests.incrementAndGet();
                if ("bytes=0-3".equals(exchange.getRequestHeaders().getFirst("Range"))) ranged.incrementAndGet();
                byte[] body = "<Error><Code>InternalError</Code><Message>retry oracle</Message></Error>"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/xml");
                exchange.sendResponseHeaders(500, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
        try (S3Client client = S3Client.builder().region(Region.US_EAST_1)
                .endpointOverride(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .overrideConfiguration(config -> config.retryPolicy(RetryPolicy.builder().numRetries(2)
                        .backoffStrategy(BackoffStrategy.none()).throttlingBackoffStrategy(BackoffStrategy.none()).build()))
                .build()) {
            var storage = new S3BlobStorage(client, new S3BlobStorage.Settings("managed-copy", "us-east-1", null,
                    "copy", new S3BlobStorage.Encryption(ServerSideEncryption.AES256, ""), "", true));
            var budget = new CopyTaskReadBudget(4);
            assertThatThrownBy(() -> storage.open("a".repeat(64), "b".repeat(64), 4, budget::reserve))
                    .isInstanceOf(IOException.class);
            assertThat(requests.get()).isEqualTo(1);
            assertThat(ranged.get()).isEqualTo(1);
            assertThat(budget.requestedBytes()).isEqualTo(4);
            assertThatThrownBy(() -> storage.open("a".repeat(64), "b".repeat(64), 4))
                    .isInstanceOf(IOException.class);
            assertThat(requests.get()).isEqualTo(4);
            assertThat(ranged.get()).isEqualTo(1);
        } finally {
            server.stop(0);
        }
    }
}
