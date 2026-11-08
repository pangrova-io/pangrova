package org.pangrova.ai.worker.embedding.openai;

import org.pangrova.ai.provider.openai.embedding.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.provider.openai.embedding.OpenAiEmbeddingConfig;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.ai.worker.OllamaIntegrationTestModels;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real provider transport acceptance; not SQL/JNI or distributed E2E coverage. */
class OpenAiCompatibleEmbeddingProviderIntegrationTest extends ContainerTestBase {

    private static final String MODEL = "all-minilm:latest";
    private static Network network;
    private static GenericContainer<?> ollama;
    private static ToxiproxyContainer toxiproxy;
    private static Proxy proxy;

    @BeforeAll
    static void startRealProvider() throws Exception {
        isDockerReady();
        network = Network.newNetwork();
        ollama = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withNetwork(network).withNetworkAliases("embedding-provider")
                .withExposedPorts(11434)
                .waitingFor(Wait.forHttp("/api/tags").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        OllamaIntegrationTestModels.mountCache(ollama);
        ollama.start();
        OllamaIntegrationTestModels.requireModel(ollama, MODEL);
        toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0"))
                .withNetwork(network);
        toxiproxy.start();
        proxy = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort())
                .createProxy("embeddings", "0.0.0.0:8666", "embedding-provider:11434");
        assertThat(embed(false, request(EmbeddingPurpose.RAW, List.of("ready")))
                .vectors().get(0)).hasSize(384);
    }

    @AfterEach
    void restoreTransport() throws IOException {
        for (var toxic : proxy.toxics().getAll()) {
            toxic.remove();
        }
        proxy.enable();
    }

    @AfterAll
    static void stopRealProvider() {
        try {
            if (toxiproxy != null) {
                toxiproxy.stop();
            }
        } finally {
            try {
                if (ollama != null) {
                    ollama.stop();
                }
            } finally {
                if (network != null) {
                    network.close();
                }
            }
        }
    }

    @Test
    void realScalarBatchAndTailKeepPurposeAndInputOrder() {
        List<String> texts = List.of("first", "second", "第三", "fourth", "fifth",
                "sixth", "seventh", "eighth", "tail");
        for (EmbeddingPurpose purpose : EmbeddingPurpose.values()) {
            List<float[]> batched = new ArrayList<>();
            batched.addAll(embed(true, request(purpose, texts.subList(0, 8))).vectors());
            batched.addAll(embed(true, request(purpose, texts.subList(8, 9))).vectors());
            assertThat(batched).hasSize(9);
            for (int i = 0; i < texts.size(); i++) {
                String prefix = switch (purpose) {
                    case RAW -> "";
                    case QUERY -> "query: ";
                    case DOCUMENT -> "doc: ";
                };
                var expected = embed(false, request(EmbeddingPurpose.RAW,
                        List.of(prefix + texts.get(i)))).vectors().get(0);
                assertCoordinates(batched.get(i), expected);
            }
        }
    }

    @Test
    void twoFailedInvocationsRequireANewInvocationAfterRecovery() throws IOException {
        assertNewInvocationAfterRecovery(2);
    }

    @Test
    void oneFailedInvocationRequiresANewInvocationAfterRecovery() throws IOException {
        assertNewInvocationAfterRecovery(1);
    }

    @Test
    void singleAttemptGrantDoesNotReplayAnUnknownPost() throws IOException {
        proxy.disable();
        var defaults = AiTransportTestAdmission.Limits.defaults();
        var limits = new AiTransportTestAdmission.Limits(defaults.concurrency(), defaults.items(), 1,
                defaults.requestBytes(), defaults.responseBytes(), defaults.mediaBytes(), defaults.constraints());
        try (var budget = new AiTransportTestAdmission(org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation.EMBEDDING,
                384, 1, 1, limits, Instant.now().plusSeconds(20))) {
            assertThatThrownBy(() -> budget.call("deny-retry", () -> provider(true).embed(
                    request(EmbeddingPurpose.RAW, List.of("denied")))))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                        assertThat(failure.category())
                                .isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
                        assertThat(failure.retryable()).isFalse();
                    });
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
        assertThat(proxy.isEnabled()).isFalse();
    }

    @Test
    void largerAttemptGrantDoesNotAuthorizeReplayOfAnUnknownPost() throws IOException {
        proxy.disable();
        try (var budget = AiTransportTestAdmission.embedding(384, 1, 1)) {
            assertThatThrownBy(() -> budget.call("exhaust-retries",
                    () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("exhausted")))))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                        assertThat(failure.category())
                                .isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
                        assertThat(failure.retryable()).isFalse();
                    });
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
    }

    @Test
    void absoluteTaskDeadlineDoesNotReplayAnUnknownPost() throws IOException {
        proxy.toxics().timeout("blocked-response", ToxicDirection.DOWNSTREAM, 0);
        try (var budget = new AiTransportTestAdmission(org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation.EMBEDDING,
                384, 1, 1, AiTransportTestAdmission.Limits.defaults(), Instant.now().plusSeconds(2))) {
            assertThatThrownBy(() -> budget.call("deadline",
                    () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("deadline")))))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                        assertThat(failure.category())
                                .isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
                        assertThat(failure.retryable()).isFalse();
                        assertThat(failure).hasCauseInstanceOf(HttpTimeoutException.class);
                    });
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
    }

    @Test
    void realModelBodyStallIsBoundedAndNextExplicitInvocationRecovers() throws Exception {
        assertRealModelBodyTermination(false);
    }

    @Test
    void realModelTaskCancellationJoinsItsClientAndANewTaskCanRun() throws Exception {
        assertRealModelBodyTermination(true);
    }

    private void assertRealModelBodyTermination(boolean cancelTask) throws Exception {
        var input = request(EmbeddingPurpose.RAW, List.of("real response body deadline"));
        float[] expected = embed(false, input).vectors().get(0);
        var release = new java.util.concurrent.CountDownLatch(1);
        var bodyStarted = new java.util.concurrent.CountDownLatch(1);
        var forwarded = new java.util.concurrent.atomic.AtomicInteger();
        var relay = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        var threads = java.util.concurrent.Executors.newFixedThreadPool(2);
        var caller = java.util.concurrent.Executors.newSingleThreadExecutor();
        var upstream = java.net.http.HttpClient.newHttpClient();
        relay.setExecutor(threads);
        relay.createContext("/v1/embeddings", exchange -> {
            try (exchange) {
                byte[] requestBody = exchange.getRequestBody().readAllBytes();
                int ordinal = forwarded.incrementAndGet();
                var actual = upstream.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(
                                "http://" + ollama.getHost() + ":" + ollama.getMappedPort(11434) + "/v1/embeddings"))
                        .timeout(Duration.ofSeconds(20)).header("Content-Type", "application/json")
                        .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(requestBody)).build(),
                        java.net.http.HttpResponse.BodyHandlers.ofByteArray());
                byte[] body = actual.body();
                exchange.sendResponseHeaders(actual.statusCode(), body.length);
                exchange.getResponseBody().write(body, 0, 1);
                exchange.getResponseBody().flush();
                bodyStarted.countDown();
                if (ordinal == 1 && !release.await(20, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new IllegalStateException("body fault relay was not released");
                }
                exchange.getResponseBody().write(body, 1, body.length - 1);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        relay.start();
        var stalled = new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                "http://127.0.0.1:" + relay.getAddress().getPort() + "/v1/embeddings", MODEL,
                "", 384, 8, 1000, "", "", cancelTask ? 20 : 3));
        var defaults = AiTransportTestAdmission.Limits.defaults();
        var singleSlot = new AiTransportTestAdmission.Limits(1, defaults.items(), defaults.attempts(),
                defaults.requestBytes(), defaults.responseBytes(), defaults.mediaBytes(), defaults.constraints());
        try (var budget = new AiTransportTestAdmission(
                org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation.EMBEDDING, 384, 1, 1,
                singleSlot, java.time.Instant.now().plusSeconds(30))) {
            var outcome = caller.submit(() -> {
                try {
                    budget.call("body-fault", () -> stalled.embed(input));
                    throw new AssertionError("incomplete model response was published");
                } catch (AiInvocationException failure) {
                    return failure;
                }
            });
            assertThat(bodyStarted.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            if (cancelTask) {
                long started = System.nanoTime();
                budget.cancel();
                assertThat(System.nanoTime() - started).isLessThan(java.util.concurrent.TimeUnit.SECONDS.toNanos(5));
            }
            var failure = outcome.get(8, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(failure.category()).isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
            assertThat(failure.retryable()).isFalse();
            if (!cancelTask) {
                assertThat(failure).hasCauseInstanceOf(HttpTimeoutException.class);
            }
            assertThat(forwarded).hasValue(1);
            if (cancelTask) {
                try (var next = AiTransportTestAdmission.embedding(384, 1, 1)) {
                    var recovered = next.call("new-task-invocation", () -> stalled.embed(input));
                    assertCoordinates(recovered.vectors().get(0), expected);
                    assertThat(next.usage().getProviderAttempts()).isEqualTo(1);
                }
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
            } else {
                var recovered = budget.call("new-explicit-invocation", () -> stalled.embed(input));
                assertCoordinates(recovered.vectors().get(0), expected);
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
            }
            assertThat(forwarded).hasValue(2);
        } finally {
            release.countDown();
            caller.shutdownNow();
            relay.stop(0);
            threads.shutdownNow();
            assertThat(caller.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(threads.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
    }

    private void assertNewInvocationAfterRecovery(int failedInvocations) throws IOException {
        proxy.disable();
        for (int ordinal = 0; ordinal < failedInvocations; ordinal++) {
            try (var budget = AiTransportTestAdmission.embedding(384, 1, 1)) {
                int invocation = ordinal;
                assertThatThrownBy(() -> budget.call("failed-" + invocation,
                        () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("recovered")))))
                        .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                            assertThat(failure.category())
                                    .isEqualTo(AiInvocationException.Category.PROVIDER_OUTCOME_UNCERTAIN);
                            assertThat(failure.retryable()).isFalse();
                        });
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
            }
        }
        proxy.enable();
        try (var budget = AiTransportTestAdmission.embedding(384, 1, 1)) {
            var result = budget.call("recovered-" + failedInvocations,
                    () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("recovered"))));
            var expected = embed(false, request(EmbeddingPurpose.RAW, List.of("recovered")));
            assertCoordinates(result.vectors().get(0), expected.vectors().get(0));
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
    }

    private static org.pangrova.ai.api.embedding.EmbeddingBatchResult embed(boolean proxied, EmbeddingBatchRequest request) {
        try (var budget = AiTransportTestAdmission.embedding(384, request.inputs().size(), 1)) {
            return budget.call("embedding-reference", () -> provider(proxied).embed(request));
        }
    }

    private static void assertCoordinates(float[] actual, float[] expected) {
        assertThat(actual).hasSize(384);
        assertThat(expected).hasSize(384);
        for (int i = 0; i < expected.length; i++) {
            assertThat(actual[i]).isFinite().isCloseTo(expected[i],
                    org.assertj.core.data.Offset.offset(0.00001f));
        }
    }

    private static EmbeddingBatchRequest request(EmbeddingPurpose purpose, List<String> texts) {
        return EmbeddingBatchRequest.text(purpose, texts);
    }

    private static OpenAiCompatibleEmbeddingProvider provider(boolean proxied) {
        String endpoint = proxied
                ? "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666)
                : "http://" + ollama.getHost() + ":" + ollama.getMappedPort(11434);
        return new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                endpoint + "/v1/embeddings", MODEL, "", 384, 8, 1000.0,
                proxied ? "query: " : "", proxied ? "doc: " : "", 30));
    }
}
