package org.pangrova.ai.worker.inference.provider.openai;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.api.invocation.InferenceProvider;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Actual HTTP faults exercise both batch providers, without claiming model inference. */
class InferenceBatchHttpTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void knownFailureCannotHideAnotherItemsUnknownPostOutcome(boolean responses) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var threads = Executors.newFixedThreadPool(2);
        var sends = new AtomicInteger();
        server.setExecutor(threads);
        server.createContext("/model", exchange -> {
            try (exchange) {
                String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                sends.incrementAndGet();
                if (request.contains("known-failure")) {
                    exchange.sendResponseHeaders(503, -1);
                } else {
                    exchange.sendResponseHeaders(200, 100);
                    exchange.getResponseBody().write('{');
                    exchange.getResponseBody().flush();
                    // Close after headers and an incomplete entity: the POST outcome is unknown.
                }
            }
        });
        server.start();
        var config = new OpenAiInferenceConfig("http://127.0.0.1:" + server.getAddress().getPort()
                + "/model", "transport-contract", "", 2, 100, 2, 5);
        InferenceProvider provider = responses
                ? new OpenAiResponsesInferenceProvider(config, Optional.empty())
                : new OpenAiCompatibleInferenceProvider(config, Optional.empty());
        try (var closeable = (java.io.Closeable) provider;
                var budget = new AiTransportTestAdmission(AiCapabilityDescriptor.Operation.TEXT_GENERATION,
                        0, 2, 2, AiTransportTestAdmission.Limits.defaults(), Instant.now().plusSeconds(15))) {
            Throwable failure = catchThrowable(() -> budget.call("mixed-batch", () -> provider.completeBatch(
                    List.of("known-failure", "unknown-outcome"), GenerationParameters.DEFAULTS)));
            while (failure instanceof CompletionException && failure.getCause() != null) {
                failure = failure.getCause();
            }
            assertThat(failure).isInstanceOf(AiInvocationException.class);
            var typed = (AiInvocationException) failure;
            assertThat(typed.category()).isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
            assertThat(typed.retryable()).isFalse();
            assertThat(typed.getSuppressed()).hasSize(1);
            assertThat(typed.getSuppressed()[0]).isInstanceOf(AiInvocationException.class)
                    .hasMessageContaining("503");
            assertThat(sends).hasValue(2);
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
        } finally {
            server.stop(0);
            threads.shutdownNow();
            assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
