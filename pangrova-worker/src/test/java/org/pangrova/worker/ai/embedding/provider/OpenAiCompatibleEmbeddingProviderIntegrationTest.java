package org.pangrova.worker.ai.embedding.provider;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.embedding.EmbeddingBatchRequest;
import org.pangrova.spi.ai.embedding.EmbeddingPurpose;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.worker.ai.context.AiProviderCallContext;
import org.pangrova.worker.ai.invocation.AiTransportTestAdmission;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
        ollama.start();
        var pulled = ollama.execInContainer("ollama", "pull", MODEL);
        assertThat(pulled.getExitCode()).as("Actual model pull: %s", pulled.getStderr()).isZero();
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
        var actualProvider = provider(true);
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
    void retriesTwoRealNetworkFailuresThenSucceedsOnThirdSend() throws IOException {
        assertRecoverySendCount(3);
    }

    @Test
    void retriesOneRealNetworkFailureThenSucceedsOnSecondSend() throws IOException {
        assertRecoverySendCount(2);
    }

    @Test
    void deniedRetryStopsBeforeSecondTransportSend() throws IOException {
        proxy.disable();
        var defaults = AiTransportTestAdmission.Limits.defaults();
        var limits = new AiTransportTestAdmission.Limits(defaults.concurrency(), defaults.items(), 1,
                defaults.requestBytes(), defaults.responseBytes(), defaults.mediaBytes(), defaults.constraints());
        try (var budget = new AiTransportTestAdmission(org.pangrova.spi.ai.AiCapabilityDescriptor.Operation.EMBEDDING,
                384, 1, 1, limits, Instant.now().plusSeconds(20))) {
            assertThatThrownBy(() -> budget.call("deny-retry", () -> provider(true).embed(
                    request(EmbeddingPurpose.RAW, List.of("denied")))))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("remaining exact resource grant");
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
        assertThat(proxy.isEnabled()).isFalse();
    }

    @Test
    void exhaustedNetworkRetriesStopAtFourSends() throws IOException {
        proxy.disable();
        try (var budget = AiTransportTestAdmission.embedding(384, 1, 1)) {
            assertThatThrownBy(() -> budget.call("exhaust-retries",
                    () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("exhausted")))))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("failed after 4 attempts");
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(4);
        }
    }

    @Test
    void absoluteTaskDeadlineStopsBlockedTransportAndPreventsRetry() throws IOException {
        proxy.toxics().timeout("blocked-response", ToxicDirection.DOWNSTREAM, 0);
        try (var budget = new AiTransportTestAdmission(org.pangrova.spi.ai.AiCapabilityDescriptor.Operation.EMBEDDING,
                384, 1, 1, AiTransportTestAdmission.Limits.defaults(), Instant.now().plusSeconds(2))) {
            assertThatThrownBy(() -> budget.call("deadline",
                    () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("deadline")))))
                    .isInstanceOf(AiInvocationException.class).extracting("category")
                    .isEqualTo(AiInvocationException.Category.TIMEOUT);
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
        }
    }

    private void assertRecoverySendCount(int successfulSend) throws IOException {
        proxy.disable();
        try (var budget = AiTransportTestAdmission.embedding(384, 1, 1)) {
            var result = budget.call("recover-" + successfulSend, () -> AiProviderCallContext.callWith(
                    "recover-" + successfulSend, Instant.now().plusSeconds(20), bytes -> {
                        var attempt = budget.openAttempt(bytes);
                        try {
                            if (budget.usage().getProviderAttempts() == successfulSend) proxy.enable();
                            return attempt;
                        } catch (IOException exception) {
                            attempt.close();
                            throw new UncheckedIOException(exception);
                        }
                    }, () -> provider(true).embed(request(EmbeddingPurpose.RAW, List.of("recovered")))));
            var expected = embed(false, request(EmbeddingPurpose.RAW, List.of("recovered")));
            assertCoordinates(result.vectors().get(0), expected.vectors().get(0));
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(successfulSend);
        }
    }

    private static org.pangrova.spi.ai.embedding.EmbeddingBatchResult embed(boolean proxied, EmbeddingBatchRequest request) {
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
