package org.pangrova.worker.ai.embedding.provider;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link OpenAiCompatibleEmbeddingProvider} using an in-process HTTP server.
 */
class OpenAiCompatibleEmbeddingProviderTest {

    private HttpServer server;
    private String endpoint;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        int port = server.getAddress().getPort();
        endpoint = "http://localhost:" + port + "/v1/embeddings";
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void embedSingleText() {
        server.createContext("/v1/embeddings", exchange -> {
            String requestBody = readRequestBody(exchange);
            assertThat(requestBody).contains("\"model\":\"test-model\"");
            assertThat(requestBody).contains("\"input\":[\"query: hello\"]");

            String response = buildEmbeddingResponse(new float[][]{{0.1f, 0.2f, 0.3f}});
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        float[] result = provider.embed("hello");

        assertThat(result).hasSize(3);
        assertThat(result[0]).isEqualTo(0.1f);
        assertThat(result[1]).isEqualTo(0.2f);
        assertThat(result[2]).isEqualTo(0.3f);
    }

    @Test
    void embedBatchTexts() {
        server.createContext("/v1/embeddings", exchange -> {
            String requestBody = readRequestBody(exchange);
            assertThat(requestBody).contains("\"model\":\"test-model\"");
            assertThat(requestBody).contains("doc: text1");
            assertThat(requestBody).contains("doc: text2");

            String response = buildEmbeddingResponse(new float[][]{
                    {1.0f, 2.0f, 3.0f},
                    {4.0f, 5.0f, 6.0f}
            });
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        List<float[]> results = provider.embedBatch(List.of("text1", "text2"));

        assertThat(results).hasSize(2);
        assertThat(results.get(0)).containsExactly(1.0f, 2.0f, 3.0f);
        assertThat(results.get(1)).containsExactly(4.0f, 5.0f, 6.0f);
    }

    @Test
    void retryOn429() {
        AtomicInteger callCount = new AtomicInteger(0);
        server.createContext("/v1/embeddings", exchange -> {
            int count = callCount.incrementAndGet();
            if (count <= 2) {
                sendResponse(exchange, 429, "rate limited");
            } else {
                String response = buildEmbeddingResponse(new float[][]{{0.5f, 0.6f, 0.7f}});
                sendResponse(exchange, 200, response);
            }
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        float[] result = provider.embed("retry test");

        assertThat(result).containsExactly(0.5f, 0.6f, 0.7f);
        assertThat(callCount.get()).isEqualTo(3);
    }

    @Test
    void retryOn503() {
        AtomicInteger callCount = new AtomicInteger(0);
        server.createContext("/v1/embeddings", exchange -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                sendResponse(exchange, 503, "service unavailable");
            } else {
                String response = buildEmbeddingResponse(new float[][]{{0.1f, 0.2f, 0.3f}});
                sendResponse(exchange, 200, response);
            }
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        float[] result = provider.embed("503 test");

        assertThat(result).hasSize(3);
        assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void failsAfterMaxRetries() {
        server.createContext("/v1/embeddings", exchange ->
                sendResponse(exchange, 429, "always rate limited"));
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);

        assertThatThrownBy(() -> provider.embed("fail test"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("HTTP 429");
    }

    @Test
    void timeoutHandling() {
        server.createContext("/v1/embeddings", exchange -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            sendResponse(exchange, 200, "{}");
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = new OpenAiCompatibleEmbeddingProvider(
                new OpenAiEmbeddingConfig(endpoint, "test-model", "test-key", 3, 10,
                        1000.0, "query: ", "doc: ", 1));

        assertThatThrownBy(() -> provider.embed("timeout test"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void invalidResponseHandling() {
        server.createContext("/v1/embeddings", exchange ->
                sendResponse(exchange, 200, "{\"data\": []}"));
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);

        assertThatThrownBy(() -> provider.embed("invalid"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("expected 1 embeddings");
    }

    @Test
    void dimensionMismatchThrows() {
        server.createContext("/v1/embeddings", exchange -> {
            String response = buildEmbeddingResponse(new float[][]{{0.1f, 0.2f}});
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);

        assertThatThrownBy(() -> provider.embed("dim mismatch"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Dimension mismatch");
    }

    @Test
    void queryPrefixPrepended() {
        server.createContext("/v1/embeddings", exchange -> {
            String requestBody = readRequestBody(exchange);
            assertThat(requestBody).contains("query: search text");

            String response = buildEmbeddingResponse(new float[][]{{0.1f, 0.2f, 0.3f}});
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        provider.embed("search text");
    }

    @Test
    void documentPrefixPrepended() {
        server.createContext("/v1/embeddings", exchange -> {
            String requestBody = readRequestBody(exchange);
            assertThat(requestBody).contains("doc: document text");

            String response = buildEmbeddingResponse(new float[][]{
                    {0.1f, 0.2f, 0.3f}
            });
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        provider.embedBatch(List.of("document text"));
    }

    @Test
    void nullTextThrows() {
        server.start();
        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);

        assertThatThrownBy(() -> provider.embed(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null");
    }

    @Test
    void emptyBatchThrows() {
        server.start();
        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);

        assertThatThrownBy(() -> provider.embedBatch(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void batchExceedingMaxSizeThrows() {
        server.start();
        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 2);

        assertThatThrownBy(() -> provider.embedBatch(List.of("a", "b", "c")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maxBatchSize");
    }

    @Test
    void jsonSpecialCharactersEscaped() {
        server.createContext("/v1/embeddings", exchange -> {
            String requestBody = readRequestBody(exchange);
            assertThat(requestBody).contains("\\\"");
            assertThat(requestBody).contains("\\n");

            String response = buildEmbeddingResponse(new float[][]{{0.1f, 0.2f, 0.3f}});
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        provider.embed("text with \"quotes\" and \nnewlines");
    }

    @Test
    void authorizationHeaderSent() {
        server.createContext("/v1/embeddings", exchange -> {
            String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
            assertThat(authHeader).isEqualTo("Bearer test-key");

            String response = buildEmbeddingResponse(new float[][]{{0.1f, 0.2f, 0.3f}});
            sendResponse(exchange, 200, response);
        });
        server.start();

        OpenAiCompatibleEmbeddingProvider provider = createProvider(3, 10);
        provider.embed("auth test");
    }

    @Test
    void modelNameAndDimensionsAccessors() {
        server.start();
        OpenAiCompatibleEmbeddingProvider provider = createProvider(768, 32);

        assertThat(provider.modelName()).isEqualTo("test-model");
        assertThat(provider.dimensions()).isEqualTo(768);
        assertThat(provider.maxBatchSize()).isEqualTo(32);
    }

    private OpenAiCompatibleEmbeddingProvider createProvider(int dimension, int maxBatchSize) {
        return new OpenAiCompatibleEmbeddingProvider(
                new OpenAiEmbeddingConfig(endpoint, "test-model", "test-key", dimension, maxBatchSize,
                        1000.0, "query: ", "doc: ", 30));
    }

    private static String buildEmbeddingResponse(float[][] embeddings) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"data\":[");
        for (int i = 0; i < embeddings.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"embedding\":[");
            for (int j = 0; j < embeddings[i].length; j++) {
                if (j > 0) {
                    sb.append(',');
                }
                sb.append(embeddings[i][j]);
            }
            sb.append("],\"index\":").append(i).append('}');
        }
        sb.append("]}");
        return sb.toString();
    }

    private static String readRequestBody(HttpExchange exchange) throws IOException {
        return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
