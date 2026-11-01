package org.pangrova.ai.worker.function;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.capability.AiCapabilityDescriptor;
import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiCompatibleEmbeddingProvider;
import org.pangrova.ai.worker.embedding.provider.openai.OpenAiEmbeddingConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pre-I/O validation uses the actual provider; value acceptance is real-provider integration. */
class AiEmbedFunctionTest {

    private final OpenAiCompatibleEmbeddingProvider provider = new OpenAiCompatibleEmbeddingProvider(
            new OpenAiEmbeddingConfig("http://localhost:11434/v1/embeddings", "local", "",
                    384, 8, 1000.0, "query: ", "doc: ", 30));
    private final AiEmbedFunction function = new AiEmbedFunction(
            name -> "local".equals(name) ? provider : null);

    @Test
    void embedReturnsNullOnNullText() {
        assertThat(function.invoke(arguments(new ColumnValue.StringValue("local"),
                new ColumnValue.NullValue(), "RAW"))).isInstanceOf(ColumnValue.NullValue.class);
    }

    @Test
    void embedReturnsNullOnNullModel() {
        assertThat(function.invoke(arguments(new ColumnValue.NullValue(),
                new ColumnValue.StringValue("text"), "RAW"))).isInstanceOf(ColumnValue.NullValue.class);
    }

    @Test
    void embedThrowsOnUnknownModel() {
        assertThatThrownBy(() -> function.invoke(arguments(new ColumnValue.StringValue("unknown"),
                new ColumnValue.StringValue("text"), "RAW")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("unknown");
    }

    @Test
    void embedThrowsOnWrongNormalizedArity() {
        for (var arguments : List.of(List.<ColumnValue>of(new ColumnValue.StringValue("local")),
                List.<ColumnValue>of(new ColumnValue.StringValue("local"),
                        new ColumnValue.StringValue("text")))) {
            assertThatThrownBy(() -> function.invoke(arguments))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("normalized");
        }
    }

    @Test
    void embedRejectsProviderDimensionThatDiffersFromExactModelBindingBeforeIo() {
        var binding = new BoundAiInvocation("ai-embed-test", "local", "v1", new byte[32],
                AiCapabilityDescriptor.Operation.EMBEDDING, "query:stage:task:ai-embed-test",
                new org.pangrova.spi.ai.capability.AiRequestConstraints(1000, 0, 262144, 1048576),
                8, 100, 400, 1048576, 4194304, 0, 383);
        assertThatThrownBy(() -> AiExecutionContext.callWithBoundInvocation(binding,
                () -> function.invoke(arguments(new ColumnValue.StringValue("local"),
                        new ColumnValue.StringValue("text"), "RAW"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("differs from exact model binding 383");
    }

    @Test
    void requiresExactModelBindingBeforeIo() {
        assertThatThrownBy(() -> function.invoke(arguments(new ColumnValue.StringValue("local"),
                new ColumnValue.StringValue("text"), "RAW")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("exact model output binding");
    }

    @Test
    void rejectsUnknownOrNullPurposeBeforeIo() {
        assertThatThrownBy(() -> function.invoke(arguments(new ColumnValue.StringValue("local"),
                new ColumnValue.StringValue("text"), "UNKNOWN")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> function.invoke(List.of(new ColumnValue.StringValue("local"),
                new ColumnValue.StringValue("text"), new ColumnValue.NullValue())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("purpose must not be NULL");
    }

    @Test
    void metadataDeclaresOneNormalizedPurposeSignatureForTextAndFile() {
        assertThat(function.getArgumentSignatures()).containsExactly(
                List.of("VARCHAR", "VARCHAR", "VARCHAR"), List.of("VARCHAR", "ROW", "VARCHAR"));
        assertThat(function.getMetadata().returnType()).isEqualTo("VECTOR");
    }

    private static List<ColumnValue> arguments(ColumnValue model, ColumnValue input, String purpose) {
        return List.of(model, input, new ColumnValue.StringValue(purpose));
    }
}
