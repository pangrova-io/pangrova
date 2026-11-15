package org.pangrova.testing.integration.e2e.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Offline real model deployment, immutable inventory binding, and timeout cleanup. */
class SharedAiProviderDeploymentIntegrationTest extends ContainerTestBase {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void realModelsStartAndInferWithoutRegistryAccess() throws Exception {
        var selected = AiProviderDeployment.configured();
        selected.container().withExtraHost("registry.ollama.ai", "127.0.0.1");
        try (var provider = new SharedAiProvider(selected, Duration.ofMinutes(3))) {
            var config = provider.prepare();
            assertThat(selected.container().execInContainer("getent", "ahostsv4", "registry.ollama.ai")
                    .getStdout()).contains("127.0.0.1");
            HttpRequest request = HttpRequest.newBuilder(URI.create(provider.endpoint("/api/tags")))
                    .timeout(Duration.ofSeconds(10)).GET().build();
            HttpResponse<String> inventory = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                    .build().send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(inventory.statusCode()).isEqualTo(200);
            JsonNode models = MAPPER.readTree(inventory.body()).path("models");
            Map<String, String> deployed = config.configForPlugin("ai-inference");
            for (var binding : Map.of("local", SharedAiProvider.PROVIDER_MODEL,
                    "embedding", SharedAiProvider.EMBEDDING_PROVIDER_MODEL).entrySet()) {
                String digest = null;
                for (JsonNode model : models) {
                    if (binding.getValue().equals(model.path("name").asText())) {
                        assertThat(digest).as("inventory contains one exact model identity").isNull();
                        digest = model.path("digest").asText();
                    }
                }
                assertThat(digest).matches("(?:sha256:)?[a-fA-F0-9]{64}");
                assertThat(deployed.get("models." + binding.getKey() + ".provider-model"))
                        .isEqualTo(binding.getValue());
                assertThat(deployed.get("models." + binding.getKey() + ".version")).isEqualTo(digest);
                assertThat(deployed.get("models." + binding.getKey() + ".max-input-tokens"))
                        .isEqualTo(Integer.toString(AiProviderDeployment.MAX_INPUT_TOKENS));
            }
            assertThat(provider.prepare()).isSameAs(config);
            assertThat(provider.requestCount("/api/pull")).isZero();
            JsonNode completion = infer(provider, "/api/generate", Map.of(
                    "model", SharedAiProvider.PROVIDER_MODEL, "prompt", "Say hello", "stream", false,
                    "options", Map.of("num_predict", 4, "num_ctx", 512)));
            assertThat(completion.path("response").asText()).isNotBlank();
            JsonNode embedding = infer(provider, "/api/embeddings", Map.of(
                    "model", SharedAiProvider.EMBEDDING_PROVIDER_MODEL, "prompt", "hello"));
            assertThat(embedding.path("embedding").size()).isEqualTo(SharedAiProvider.EMBEDDING_DIMENSION);
            assertThat(provider.requestCount("/api/pull")).isZero();
        }
    }

    @Test
    void wrongPreparedDigestFailsAndRemovesTheOwnedContainer() {
        var selected = AiProviderDeployment.configured();
        var wrongModels = new HashMap<>(selected.expectedModels());
        wrongModels.put(SharedAiProvider.PROVIDER_MODEL, "0".repeat(64));
        try (var provider = new SharedAiProvider(new AiProviderDeployment(
                selected.container(), wrongModels), Duration.ofMinutes(3))) {
            assertThatThrownBy(provider::prepare).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Prepared AI model identity changed");
            assertContainerRemoved(selected.container());
        }
    }

    @Test
    void missingModelFailsLocallyWithoutAttemptingDownload() {
        var empty = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withExposedPorts(11434)
                .waitingFor(new HttpWaitStrategy().forPath("/api/tags").forStatusCode(200));
        try (var provider = new SharedAiProvider(empty, Duration.ofMinutes(3))) {
            assertThatThrownBy(provider::prepare).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Prepared Ollama fixture is missing model");
            assertContainerRemoved(empty);
        }
    }

    private static void assertContainerRemoved(GenericContainer<?> container) {
        assertThat(container.isRunning()).isFalse();
        assertThat(container.getContainerId()).isNull();
    }

    private static JsonNode infer(SharedAiProvider provider, String path, Map<String, Object> body)
            throws Exception {
        var request = HttpRequest.newBuilder(URI.create(provider.endpoint(path)))
                .timeout(Duration.ofSeconds(60)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(MAPPER.writeValueAsBytes(body))).build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        var result = MAPPER.readTree(response.body());
        assertThat(result.hasNonNull("error")).isFalse();
        return result;
    }

    @Test
    void pausedRealOllamaExpiresTheSharedBudgetAndDeletesOnlyItsOwnedContainer() {
        var pausedId = new AtomicReference<String>();
        var container = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withExposedPorts(11434)
                .waitingFor(new PauseAfterReadiness(pausedId));
        try (var provider = new SharedAiProvider(container, Duration.ofSeconds(20))) {
            long started = System.nanoTime();
            assertThatThrownBy(provider::prepare)
                    .isInstanceOfAny(TimeoutException.class, HttpTimeoutException.class)
                    .satisfies(failure -> assertThat(failure.getSuppressed())
                            .anySatisfy(diagnostic -> assertThat(diagnostic)
                                    .hasMessageContaining("immutable model inventory")
                                    .hasMessageContaining("container diagnostics")));
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofSeconds(30));
            assertThat(pausedId.get()).as("the actual ready Ollama process was paused").isNotBlank();
            assertThatThrownBy(() -> DockerClientFactory.instance().client()
                    .inspectContainerCmd(pausedId.get()).exec()).isInstanceOf(NotFoundException.class);
            assertThatThrownBy(() -> provider.endpoint("/api/tags"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not running");
        }
    }

    /** Pauses the actual Docker process only after the real inventory endpoint has returned HTTP 200. */
    private static final class PauseAfterReadiness extends HttpWaitStrategy {
        private final AtomicReference<String> pausedId;

        private PauseAfterReadiness(AtomicReference<String> pausedId) {
            this.pausedId = pausedId;
            forPath("/api/tags");
            forStatusCode(200);
        }

        @Override
        protected void waitUntilReady() {
            super.waitUntilReady();
            String id = waitStrategyTarget.getContainerId();
            DockerClientFactory.instance().client().pauseContainerCmd(id).exec();
            assertThat(DockerClientFactory.instance().client().inspectContainerCmd(id).exec()
                    .getState().getPaused()).isTrue();
            pausedId.set(id);
        }
    }
}
