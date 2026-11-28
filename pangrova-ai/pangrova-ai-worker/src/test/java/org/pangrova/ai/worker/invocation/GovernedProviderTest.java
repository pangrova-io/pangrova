package org.pangrova.ai.worker.invocation;

import org.pangrova.ai.api.enums.AiFailureCategory;
import org.pangrova.ai.api.enums.AiOperation;
import org.pangrova.ai.worker.governance.observer.AiInvocationObserver;

import org.junit.jupiter.api.Test;
import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.embedding.EmbeddingBatchRequest;
import org.pangrova.ai.api.enums.EmbeddingPurpose;
import org.pangrova.ai.api.invocation.GenerationParameters;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.provider.governance.GovernedEmbeddingProvider;
import org.pangrova.ai.worker.provider.governance.GovernedInferenceProvider;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pre-I/O security checks use real configured providers, never a fake response. */
class GovernedProviderTest {

    @Test
    void invocationWithoutNativeTaskContextFailsClosedBeforeProviderCall() {
        try (var models = new GovernedAiTestModels("http://127.0.0.1:1")) {
            var governed = new GovernedInferenceProvider(models.inference(),
                    models.service((request, capability) -> {}, AiInvocationObserver.noop()));
            assertThatThrownBy(() -> governed.complete("hello", GenerationParameters.DEFAULTS))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                            assertThat(failure.category()).isEqualTo(AiFailureCategory.AUTHORIZATION))
                    .hasMessageContaining("no trusted native task context");
        }
    }

    @Test
    void invocationWithoutExactOperatorBindingFailsClosed() {
        try (var models = new GovernedAiTestModels("http://127.0.0.1:1")) {
            var governed = new GovernedInferenceProvider(models.inference(),
                    models.service((request, capability) -> {}, AiInvocationObserver.noop()));
            assertThatThrownBy(() -> AiExecutionContext.callWith(models.context("no-binding"),
                    () -> governed.complete("hello", GenerationParameters.DEFAULTS)))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                            assertThat(failure.category()).isEqualTo(AiFailureCategory.AUTHORIZATION))
                    .hasMessageContaining("no exact Native OperatorBinding");
            assertThat(AiExecutionContext.current()).isEmpty();
            assertThat(AiExecutionContext.boundInvocation()).isEmpty();
        }
    }

    @Test
    void inferenceCannotBorrowEmbeddingOperatorsExactIdentity() {
        try (var models = new GovernedAiTestModels("http://127.0.0.1:1")) {
            var governed = new GovernedInferenceProvider(models.inference(),
                    models.service((request, capability) -> {}, AiInvocationObserver.noop()));
            assertThatThrownBy(() -> models.invoke(AiOperation.EMBEDDING,
                    "wrong-operation", 1, () -> governed.complete("hello", GenerationParameters.DEFAULTS)))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                            assertThat(failure.category()).isEqualTo(AiFailureCategory.CAPABILITY_MISMATCH))
                    .hasMessageContaining("exact OperatorBinding");
            assertThat(AiExecutionContext.boundInvocation()).isEmpty();
            assertThat(AiExecutionContext.providerIdempotencyBase()).isEmpty();
        }
    }

    @Test
    void embeddingRequiresSameTrustedContextAsInference() {
        try (var models = new GovernedAiTestModels("http://127.0.0.1:1")) {
            var governed = new GovernedEmbeddingProvider(models.embedding(),
                    models.service((request, capability) -> {}, AiInvocationObserver.noop()));
            assertThatThrownBy(() -> governed.embed(
                    EmbeddingBatchRequest.text(EmbeddingPurpose.DOCUMENT, List.of("hello"))))
                    .isInstanceOfSatisfying(AiInvocationException.class, failure ->
                            assertThat(failure.category()).isEqualTo(AiFailureCategory.AUTHORIZATION))
                    .hasMessageContaining("no trusted native task context");
        }
    }
}
