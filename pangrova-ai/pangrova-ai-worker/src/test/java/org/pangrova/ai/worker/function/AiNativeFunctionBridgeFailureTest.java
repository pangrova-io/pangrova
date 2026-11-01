package org.pangrova.ai.worker.function;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiEmbeddingConfig;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;
import org.pangrova.spi.connector.data.value.ColumnValue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the production SQL function with an actual governed provider send. */
class AiNativeFunctionBridgeFailureTest {
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void preservesUnknownProviderOutcomeAcrossWorkerFunctionBoundary() throws IOException {
        AtomicInteger received = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/embeddings", exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.incrementAndGet();
            exchange.close();
        });
        server.start();

        var provider = new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                "http://localhost:" + server.getAddress().getPort() + "/v1/embeddings",
                "transport-model", "", 3, 4, 100.0, "", "", 5));
        var function = new AiEmbedFunction(model ->
                "transport-model".equals(model) ? provider : null);

        try (var budget = AiTransportTestAdmission.embedding(3, 1, 1)) {
            assertThatThrownBy(() -> budget.call("bridge-unknown", () -> function.invoke(
                    List.of(new ColumnValue.StringValue("transport-model"),
                            new ColumnValue.StringValue("hello"),
                            new ColumnValue.StringValue("RAW")))))
                    .isInstanceOf(AiInvocationException.class)
                    .satisfies(failure -> {
                        AiInvocationException invocation = (AiInvocationException) failure;
                        assertThat(invocation.category()).isEqualTo(
                                AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
                        assertThat(invocation.retryable()).isFalse();
                    });
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
        assertThat(received).hasValue(1);
    }
}
