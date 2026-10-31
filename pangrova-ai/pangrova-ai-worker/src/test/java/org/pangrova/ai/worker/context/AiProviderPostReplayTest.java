package org.pangrova.ai.worker.context;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiEmbeddingConfig;
import org.pangrova.ai.worker.inference.provider.openai.OpenAiCompatibleInferenceProvider;
import org.pangrova.ai.worker.inference.provider.openai.OpenAiInferenceConfig;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;
import org.pangrova.ai.worker.rerank.provider.openai.OpenAiCompatibleRerankingProvider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real HTTP responses prove each model POST path refuses an unverified replay. */
class AiProviderPostReplayTest {
    private HttpServer server;
    private final AtomicInteger received = new AtomicInteger();

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void embeddingDoesNotReplayServiceUnavailable() throws IOException {
        String endpoint = unavailableEndpoint();
        var provider = new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                endpoint, "model", "", 3, 4, 100.0, "", "", 5));
        try (var budget = AiTransportTestAdmission.embedding(3, 1, 1)) {
            assertTerminal(() -> budget.call("embedding-post", () -> provider.embed(
                    EmbeddingBatchRequest.text(EmbeddingPurpose.RAW, List.of("hello")))));
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void embeddingSuccessfulResponseStillPublishesItsExactVector() throws IOException {
        String endpoint = successEndpoint(
                "{\"data\":[{\"index\":0,\"embedding\":[1,2,3]}]}");
        var provider = new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                endpoint, "model", "", 3, 4, 100.0, "", "", 5));
        try (var budget = AiTransportTestAdmission.embedding(3, 1, 1)) {
            var result = budget.call("embedding-success", () -> provider.embed(
                    EmbeddingBatchRequest.text(EmbeddingPurpose.RAW, List.of("hello"))));
            assertThat(result.vectors().get(0)).containsExactly(1.0f, 2.0f, 3.0f);
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void inferenceDoesNotReplayServiceUnavailable() throws IOException {
        String endpoint = unavailableEndpoint();
        try (var provider = new OpenAiCompatibleInferenceProvider(new OpenAiInferenceConfig(
                endpoint, "model", "", 4, 100.0, 2, 5), Optional.empty())) {
            assertTerminal(() -> AiTransportTestAdmission.inference(
                    () -> provider.complete("hello", GenerationParameters.DEFAULTS)));
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void rerankingDoesNotReplayServiceUnavailable() throws IOException {
        String endpoint = unavailableEndpoint();
        try (var provider = new OpenAiCompatibleRerankingProvider(
                URI.create(endpoint), "model", "", 4, 100.0, 5);
                var budget = new AiTransportTestAdmission(
                        AiCapabilityDescriptor.Operation.RERANKING, 0, 1, 1,
                        AiTransportTestAdmission.Limits.defaults(),
                        Instant.now().plusSeconds(30))) {
            assertTerminal(() -> budget.call("rerank-post",
                    () -> provider.score("query", List.of("document"))));
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void rerankingSuccessfulResponseStillPublishesItsExactScore() throws IOException {
        String endpoint = successEndpoint(
                "{\"id\":\"provider-1\",\"results\":[{\"index\":0,\"score\":0.75}]}");
        try (var provider = new OpenAiCompatibleRerankingProvider(
                URI.create(endpoint), "model", "", 4, 100.0, 5);
                var budget = new AiTransportTestAdmission(
                        AiCapabilityDescriptor.Operation.RERANKING, 0, 1, 1,
                        AiTransportTestAdmission.Limits.defaults(),
                        Instant.now().plusSeconds(30))) {
            var result = budget.call("rerank-success",
                    () -> provider.score("query", List.of("document")));
            assertThat(result.scores()).containsExactly(0.75);
            assertThat(result.providerRequestId()).isEqualTo("provider-1");
        }
        assertThat(received).hasValue(1);
    }

    @Test
    void independentLoopbackRerankingEndpointsEachReceiveExactlyOneUnavailablePost() throws IOException {
        for (int index = 0; index < 32; index++) {
            String endpoint = unavailableEndpoint();
            try (var provider = new OpenAiCompatibleRerankingProvider(
                    URI.create(endpoint), "model", "", 4, 100.0, 5);
                    var budget = new AiTransportTestAdmission(
                            AiCapabilityDescriptor.Operation.RERANKING, 0, 1, 1,
                            AiTransportTestAdmission.Limits.defaults(), Instant.now().plusSeconds(30))) {
                assertTerminal(() -> budget.call("rerank-independent-" + endpoint,
                        () -> provider.score("query", List.of("document"))));
                assertThat(received).as("POST delivered to %s", endpoint).hasValue(index + 1);
            } finally {
                server.stop(0);
            }
        }
    }

    private String unavailableEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/model", exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.incrementAndGet();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/model";
    }

    private String successEndpoint(String body) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/model", exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.incrementAndGet();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        });
        server.start();
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/model";
    }

    private static void assertTerminal(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOf(AiInvocationException.class)
                .hasMessageContaining("HTTP 503")
                .satisfies(failure -> {
                    AiInvocationException invocation = (AiInvocationException) failure;
                    assertThat(invocation.category())
                            .isEqualTo(AiInvocationException.Category.PROVIDER_UNAVAILABLE);
                    assertThat(invocation.retryable()).isFalse();
                });
    }
}
