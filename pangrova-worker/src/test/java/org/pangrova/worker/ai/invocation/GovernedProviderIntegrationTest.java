package org.pangrova.worker.ai.invocation;

import org.pangrova.worker.ai.governance.AiInvocationObserver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.pangrova.spi.ai.AiCapabilityDescriptor;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.AiInvocationRequest;
import org.pangrova.spi.ai.AiInvocationResult;
import org.pangrova.spi.ai.embedding.EmbeddingBatchRequest;
import org.pangrova.spi.ai.embedding.EmbeddingPurpose;
import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.worker.ai.provider.GovernedEmbeddingProvider;
import org.pangrova.worker.ai.provider.GovernedInferenceProvider;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual registry, service, governance, and Ollama HTTP; not SQL/JNI E2E. */
class GovernedProviderIntegrationTest extends ContainerTestBase {
    private static GenericContainer<?> ollama;
    private GovernedAiTestModels models;
    private String taskId;

    @BeforeAll
    static void startRealModels() throws Exception {
        isDockerReady();
        ollama = new GenericContainer<>(DockerImageName.parse("ollama/ollama:0.6.2"))
                .withExposedPorts(11434)
                .waitingFor(Wait.forHttp("/api/tags").forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(3)));
        ollama.start();
        for (String model : List.of(GovernedAiTestModels.EMBEDDING_MODEL,
                GovernedAiTestModels.INFERENCE_MODEL)) {
            var pull = ollama.execInContainer("ollama", "pull", model);
            assertThat(pull.getExitCode()).as("Actual model pull: %s", pull.getStderr()).isZero();
        }
    }

    @AfterAll
    static void stopModels() { if (ollama != null) ollama.stop(); }

    @BeforeEach
    void configureRealProviders(TestInfo info) {
        models = new GovernedAiTestModels("http://" + ollama.getHost() + ":" + ollama.getMappedPort(11434));
        taskId = info.getTestMethod().orElseThrow().getName();
    }

    @AfterEach
    void releaseOwnedResources() {
        if (models != null) models.close();
    }

    @Test
    void inferenceUsesExactTrustedIdentityAndReportsActualCompletion() {
        List<AiInvocationRequest> authorized = new ArrayList<>();
        List<AiInvocationResult> completed = new ArrayList<>();
        var governed = new GovernedInferenceProvider(models.inference(),
                models.service((request, capability) -> authorized.add(request), observer(completed)));
        var parameters = new GenerationParameters(OptionalDouble.of(0), OptionalInt.of(16), null);
        var result = models.invoke(AiCapabilityDescriptor.Operation.TEXT_GENERATION, taskId, 8,
                () -> governed.complete("Say hello in one sentence.", parameters));
        assertThat(result.text()).isNotBlank();
        assertThat(completed).singleElement().satisfies(value ->
                assertThat(value.outputs()).containsExactly(new AiInvocationResult.TextOutput(result.text())));
        assertIdentity(authorized, AiCapabilityDescriptor.Operation.TEXT_GENERATION);
    }

    @Test
    void embeddingBatchUsesOneAuthorizationAndPreservesPurposeAndActualOrder() {
        List<AiInvocationRequest> authorized = new ArrayList<>();
        List<AiInvocationResult> completed = new ArrayList<>();
        var raw = models.embedding();
        var governed = new GovernedEmbeddingProvider(raw,
                models.service((request, capability) -> authorized.add(request), observer(completed)));
        List<String> texts = List.of("first distinct text", "第二个文本");
        for (EmbeddingPurpose purpose : EmbeddingPurpose.values()) {
            authorized.clear();
            completed.clear();
            var request = EmbeddingBatchRequest.text(purpose, texts);
            var actual = models.invoke(AiCapabilityDescriptor.Operation.EMBEDDING, taskId, 8,
                    () -> governed.embed(request));
            String prefix = purpose == EmbeddingPurpose.QUERY ? "query: "
                    : purpose == EmbeddingPurpose.DOCUMENT ? "doc: " : "";
            var oracle = new GovernedEmbeddingProvider(raw,
                    models.service((value, capability) -> {}, AiInvocationObserver.noop()));
            var expected = models.invoke(AiCapabilityDescriptor.Operation.EMBEDDING, taskId + "-oracle", 8,
                    () -> oracle.embed(EmbeddingBatchRequest.text(EmbeddingPurpose.RAW,
                            texts.stream().map(text -> prefix + text).toList()))).vectors();
            assertThat(actual.vectors()).hasSize(2);
            for (int row = 0; row < texts.size(); row++) {
                assertVector(actual.vectors().get(row), expected.get(row));
            }
            assertIdentity(authorized, AiCapabilityDescriptor.Operation.EMBEDDING);
            assertThat(authorized).singleElement().satisfies(value -> {
                assertThat(value.embeddingPurpose()).contains(purpose);
                assertThat(value.executionMode()).isEqualTo(AiCapabilityDescriptor.ExecutionMode.BATCH);
                assertThat(value.inputs()).containsExactlyElementsOf(texts.stream()
                        .map(AiInvocationRequest.TextInput::new).toList());
            });
            assertThat(completed).singleElement().satisfies(value -> {
                assertThat(value.outputs()).hasSize(2).allSatisfy(output ->
                        assertThat(output).isInstanceOf(AiInvocationResult.VectorOutput.class));
            });
        }
    }

    @Test
    void consumedOperatorGrantRejectsSecondInvocationAfterRealFirstCompletion() {
        var governed = new GovernedEmbeddingProvider(models.embedding(),
                models.service((request, capability) -> {}, AiInvocationObserver.noop()));
        var request = EmbeddingBatchRequest.text(EmbeddingPurpose.RAW, List.of("first"));
        var first = models.invoke(AiCapabilityDescriptor.Operation.EMBEDDING, taskId, 1,
                () -> governed.embed(request));
        assertThat(first.vectors()).singleElement().satisfies(vector -> assertThat(vector).hasSize(384));
        assertThatThrownBy(() -> models.invoke(AiCapabilityDescriptor.Operation.EMBEDDING, taskId, 1,
                () -> governed.embed(request)))
                .isInstanceOfSatisfying(AiInvocationException.class, failure -> {
                    assertThat(failure.category()).isEqualTo(AiInvocationException.Category.BUDGET_EXCEEDED);
                    assertThat(failure.retryable()).isFalse();
                }).hasMessageContaining("remaining exact resource grant");
    }

    private void assertIdentity(List<AiInvocationRequest> requests, AiCapabilityDescriptor.Operation operation) {
        var binding = models.binding(operation, taskId, 8);
        assertThat(requests).singleElement().satisfies(request -> {
            assertThat(request.tenantId()).isEqualTo("tenant-a");
            assertThat(request.principal()).isEqualTo("alice");
            assertThat(request.modelName()).isEqualTo(binding.modelId());
            assertThat(request.modelVersion()).isEqualTo(binding.modelVersion());
            assertThat(request.modelSignatureSha256()).containsExactly(binding.modelSignatureSha256());
            assertThat(request.deadline().toEpochMilli()).isEqualTo(models.context(taskId).deadlineEpochMs());
        });
    }

    private static void assertVector(float[] actual, float[] expected) {
        assertThat(actual).hasSize(384);
        assertThat(expected).hasSize(384);
        for (int index = 0; index < actual.length; index++) {
            assertThat(actual[index]).isFinite().isCloseTo(expected[index],
                    org.assertj.core.data.Offset.offset(0.00001f));
        }
    }

    private static AiInvocationObserver observer(List<AiInvocationResult> results) {
        return new AiInvocationObserver() {
            @Override
            public void onSuccess(AiInvocationRequest request, AiInvocationResult result) { results.add(result); }
            @Override
            public void onFailure(AiInvocationRequest request, AiInvocationException failure) {}
        };
    }
}
