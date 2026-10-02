package org.pangrova.worker.ai.context;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.ai.AiCapabilityDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundAiInvocationTest {

    @Test
    void requiresFixedDimensionForEmbeddingOutput() {
        assertThatThrownBy(() -> binding(
                AiCapabilityDescriptor.Operation.EMBEDDING, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputVectorDimension");
        assertThatThrownBy(() -> binding(
                AiCapabilityDescriptor.Operation.EMBEDDING, 65_536))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputVectorDimension");
    }

    @Test
    void rejectsVectorDimensionForNonEmbeddingOperation() {
        assertThatThrownBy(() -> binding(
                AiCapabilityDescriptor.Operation.TEXT_GENERATION, 384))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not declare a vector dimension");
    }

    private static BoundAiInvocation binding(
            AiCapabilityDescriptor.Operation operation, int dimension) {
        return new BoundAiInvocation(
                "ai-test", "model-a", "v1", new byte[32], operation,
                "query:stage:task:ai-test", new org.pangrova.spi.ai.AiRequestConstraints(1000, 500, 262144, 1048576),
                2, 100, 400, 1048576, 4194304, 0,
                dimension);
    }
}
