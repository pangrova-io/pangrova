package org.pangrova.testing.e2e.ai;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.ContainerTestBase;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real model deployment, immutable inventory binding, and paused-server timeout cleanup. */
class SharedAiProviderDeploymentIntegrationTest extends ContainerTestBase {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void realHttpPullFreezesBothExactInventoryDigestsBeforePublishingConfiguration() throws Exception {
        try (var provider = new SharedAiProvider(true)) {
            var config = provider.prepare();
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
            }
            assertThat(provider.prepare()).isSameAs(config);
            await("two actual completed non-streaming model pulls")
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(provider.requestCount("/api/pull")).isEqualTo(2));
        }
    }

    @Test
    void pausedRealOllamaExpiresTheSharedBudgetAndDeletesOnlyItsOwnedContainer() {
        var pausedId = new AtomicReference<String>();
        var container = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withExposedPorts(11434)
                .waitingFor(Wait.forHttp("/api/tags").forStatusCode(200));
        Runnable pauseBeforePull = () -> {
            String id = container.getContainerId();
            DockerClientFactory.instance().client().pauseContainerCmd(id).exec();
            assertThat(DockerClientFactory.instance().client().inspectContainerCmd(id).exec()
                    .getState().getPaused()).isTrue();
            pausedId.set(id);
        };
        try (var provider = new SharedAiProvider(container, Duration.ofSeconds(20), pauseBeforePull)) {
            long started = System.nanoTime();
            assertThatThrownBy(provider::prepare)
                    .isInstanceOfAny(TimeoutException.class, HttpTimeoutException.class)
                    .satisfies(failure -> assertThat(failure.getSuppressed())
                            .anySatisfy(diagnostic -> assertThat(diagnostic)
                                    .hasMessageContaining("pull " + SharedAiProvider.PROVIDER_MODEL)
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
}
