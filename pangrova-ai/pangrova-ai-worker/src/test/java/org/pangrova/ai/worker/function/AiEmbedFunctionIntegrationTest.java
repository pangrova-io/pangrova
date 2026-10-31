package org.pangrova.ai.worker.function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.capability.AiCapabilityDescriptor;
import org.pangrova.ai.api.error.AiInvocationException;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.embedding.EmbeddingPurpose;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.ai.worker.OllamaIntegrationTestModels;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.AiProviderCallContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.ai.worker.invocation.AiTransportTestAdmission;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiEmbeddingConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Worker function/provider values, not public SQL/JNI E2E acceptance. */
class AiEmbedFunctionIntegrationTest extends ContainerTestBase {

    private static final String MODEL = "all-minilm:latest";
    private static GenericContainer<?> ollama;

    @BeforeAll
    static void startRealEmbeddingModel() throws Exception {
        isDockerReady();
        ollama = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withExposedPorts(11434)
                .waitingFor(Wait.forHttp("/api/tags").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        OllamaIntegrationTestModels.mountCache(ollama);
        ollama.start();
        OllamaIntegrationTestModels.requireModel(ollama, MODEL);
        assertThat(embed(provider(384), EmbeddingPurpose.RAW, List.of("ready")).get(0)).hasSize(384);
    }

    @AfterAll
    static void stopRealEmbeddingModel() {
        if (ollama != null) {
            ollama.stop();
        }
    }

    @Test
    void scalarReturnsFirstClassVectorForEveryExplicitPurpose() {
        var provider = provider(384);
        var function = new AiEmbedFunction(name -> MODEL.equals(name) ? provider : null);
        List<float[]> vectors = new ArrayList<>();
        for (EmbeddingPurpose purpose : EmbeddingPurpose.values()) {
            ColumnValue actual;
            try (var budget = AiTransportTestAdmission.embedding(384, 1, 1)) {
                actual = budget.call("scalar-" + purpose, () -> function.invoke(arguments("hello world", purpose)));
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
            }
            assertThat(actual).isInstanceOf(ColumnValue.VectorValue.class);
            float[] vector = ((ColumnValue.VectorValue) actual).value();
            var expected = embed(provider, purpose, List.of("hello world")).get(0);
            assertCoordinates(vector, expected);
            vectors.add(vector);
        }
        assertThat(vectors.get(0)).isNotEqualTo(vectors.get(1)).isNotEqualTo(vectors.get(2));
        assertThat(vectors.get(1)).isNotEqualTo(vectors.get(2));
    }

    @Test
    void batchesEightPlusOneWithoutChangingPurposeAndPreservesNullPosition() {
        var provider = provider(384);
        var function = new AiEmbedFunction(name -> MODEL.equals(name) ? provider : null);
        List<String> texts = List.of("first", "second", "第三", "fourth", "fifth",
                "sixth", "seventh", "eighth", "tail");
        for (EmbeddingPurpose purpose : EmbeddingPurpose.values()) {
            List<List<ColumnValue>> rows = new ArrayList<>();
            rows.add(arguments(texts.get(0), purpose));
            rows.add(List.of(new ColumnValue.StringValue(MODEL), new ColumnValue.NullValue(),
                    new ColumnValue.StringValue(purpose.name())));
            texts.subList(1, texts.size()).forEach(text -> rows.add(arguments(text, purpose)));
            List<ColumnValue> result;
            try (var budget = AiTransportTestAdmission.embedding(384, 9, 2)) {
                result = budget.call("function-" + purpose, () -> function.invokeBatch(rows));
                assertThat(budget.usage().getProviderAttempts()).isEqualTo(2);
                assertThat(budget.usage().getProviderItems()).isEqualTo(9);
            }
            assertThat(result).hasSize(10);
            assertThat(result.get(1)).isInstanceOf(ColumnValue.NullValue.class);
            assertBatchValues(provider, result, texts, purpose);
        }
    }

    @Test
    void actualModelDimensionMismatchRejectsCompleteResponse() {
        var provider = provider(383);
        var function = new AiEmbedFunction(name -> provider);
        try (var budget = AiTransportTestAdmission.embedding(383, 1, 1)) {
            assertThatThrownBy(() -> budget.call("dimension", () -> function.invoke(arguments("dimension", EmbeddingPurpose.RAW))))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("Dimension mismatch")
                    .hasMessageContaining("expected 383, got 384");
        }
    }

    @Test
    void rejectionOfTailGrantDoesNotPublishSuccessfulFirstChunk() {
        var provider = provider(384);
        var function = new AiEmbedFunction(name -> provider);
        var rows = java.util.stream.IntStream.range(0, 9)
                .mapToObj(index -> arguments("row " + index, EmbeddingPurpose.DOCUMENT)).toList();
        var defaults = AiTransportTestAdmission.Limits.defaults();
        var limits = new AiTransportTestAdmission.Limits(defaults.concurrency(), defaults.items(), 1,
                defaults.requestBytes(), defaults.responseBytes(), defaults.mediaBytes(), defaults.constraints());
        try (var budget = new AiTransportTestAdmission(AiCapabilityDescriptor.Operation.EMBEDDING, 384,
                9, 2, limits, Instant.now().plusSeconds(30))) {
            assertThatThrownBy(() -> budget.call("reject-tail", () -> function.invokeBatch(rows)))
                    .isInstanceOf(AiInvocationException.class).hasMessageContaining("remaining exact resource grant");
            assertThat(budget.usage().getProviderAttempts()).isEqualTo(1);
            assertThat(budget.usage().getProviderResponseBytes()).isPositive();
        }
    }

    private static void assertBatchValues(OpenAiCompatibleEmbeddingProvider provider,
            List<ColumnValue> results, List<String> texts, EmbeddingPurpose purpose) {
        for (int index = 0; index < texts.size(); index++) {
            int resultIndex = index == 0 ? 0 : index + 1;
            assertThat(results.get(resultIndex)).isInstanceOf(ColumnValue.VectorValue.class);
            var expected = embed(provider, purpose, List.of(texts.get(index))).get(0);
            assertCoordinates(((ColumnValue.VectorValue) results.get(resultIndex)).value(), expected);
        }
    }

    private static void assertCoordinates(float[] actual, float[] expected) {
        assertThat(actual).hasSize(384);
        assertThat(expected).hasSize(384);
        for (int index = 0; index < actual.length; index++) {
            assertThat(actual[index]).isFinite().isCloseTo(expected[index],
                    org.assertj.core.data.Offset.offset(0.00001f));
        }
    }

    private static List<ColumnValue> arguments(String text, EmbeddingPurpose purpose) {
        return List.of(new ColumnValue.StringValue(MODEL), new ColumnValue.StringValue(text),
                new ColumnValue.StringValue(purpose.name()));
    }

    private static List<float[]> embed(OpenAiCompatibleEmbeddingProvider provider, EmbeddingPurpose purpose, List<String> texts) {
        try (var budget = AiTransportTestAdmission.embedding(provider.dimensions(), texts.size(), 1)) {
            return budget.call("reference", () -> provider.embed(EmbeddingBatchRequest.text(purpose, texts))).vectors();
        }
    }

    private static OpenAiCompatibleEmbeddingProvider provider(int dimension) {
        String endpoint = "http://" + ollama.getHost() + ":" + ollama.getMappedPort(11434);
        return new OpenAiCompatibleEmbeddingProvider(new OpenAiEmbeddingConfig(
                endpoint + "/v1/embeddings", MODEL, "", dimension, 8, 1000.0,
                "query: ", "doc: ", 30));
    }
}
