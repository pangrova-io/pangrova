package org.pangrova.shuffle.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3ShuffleObjectStorageTest {

    private static final String SUCCESS =
            "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>";
    private static final String PARTIAL_FAILURE = """
            <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Error><Key>test/shuffle/batch</Key><Code>AccessDenied</Code>
                <Message>Cannot delete object</Message></Error>
            </DeleteResult>
            """;

    @Test
    void rejectsPerObjectErrorInSuccessfulHttpResponse() throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startServer(requests, ignored -> PARTIAL_FAILURE);
        try (S3ShuffleObjectStorage objects = storage(server)) {
            assertThatThrownBy(() -> objects.delete(List.of("shuffle/batch")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("key=test/shuffle/batch")
                    .hasMessageContaining("code=AccessDenied");
            assertThat(requests).hasValue(1);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsLaterBatchErrorAfterEarlierBatchSucceeded() throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startServer(requests, index -> index == 0 ? SUCCESS : PARTIAL_FAILURE);
        try (S3ShuffleObjectStorage objects = storage(server)) {
            List<String> keys = IntStream.range(0, 1_001)
                    .mapToObj(index -> "shuffle/batch-" + index).toList();
            assertThatThrownBy(() -> objects.delete(keys))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("code=AccessDenied");
            assertThat(requests).hasValue(2);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void acceptsSuccessfulDeleteAndSkipsEmptyRequest() throws IOException {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = startServer(requests, ignored -> SUCCESS);
        try (S3ShuffleObjectStorage objects = storage(server)) {
            objects.delete(List.of());
            assertThat(requests).hasValue(0);
            objects.delete(List.of("shuffle/batch"));
            assertThat(requests).hasValue(1);
        } finally {
            server.stop(0);
        }
    }

    private static HttpServer startServer(
            AtomicInteger requests,
            IntFunction<String> response) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = response.apply(requests.getAndIncrement())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/xml");
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        server.start();
        return server;
    }

    private static S3ShuffleObjectStorage storage(HttpServer server) {
        S3Client client = S3Client.builder()
                .endpointOverride(URI.create("http://127.0.0.1:"
                        + server.getAddress().getPort()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("access-key", "secret-key")))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true).build())
                .build();
        return S3ShuffleObjectStorage.testing(client, "test", "test");
    }
}
