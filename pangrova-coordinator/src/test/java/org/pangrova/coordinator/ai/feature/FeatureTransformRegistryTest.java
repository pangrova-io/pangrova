package org.pangrova.coordinator.ai.feature;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FeatureTransformRegistry}.
 */
@DisplayName("FeatureTransformRegistry")
class FeatureTransformRegistryTest {

    private FeatureTransformRegistry registry;

    @BeforeEach
    void setUp() {
        registry = FeatureTransformRegistry.createWithBuiltins();
    }

    @Test
    @DisplayName("createWithBuiltins registers all four built-in transforms")
    void registersAllBuiltinTransforms() {
        assertThat(registry.size()).isEqualTo(4);
        assertThat(registry.registeredTypes()).containsExactlyInAnyOrder(
                FeatureTransformType.TOKENIZE,
                FeatureTransformType.NORMALIZE,
                FeatureTransformType.ONE_HOT_ENCODE,
                FeatureTransformType.BUCKETIZE);
    }

    @Test
    @DisplayName("resolve returns correct descriptor for TOKENIZE")
    void resolveTokenize() {
        FeatureTransformDescriptor desc = registry.resolve(FeatureTransformType.TOKENIZE);
        assertThat(desc.type()).isEqualTo(FeatureTransformType.TOKENIZE);
        assertThat(desc.inputTypeFamily()).isEqualTo("VARCHAR");
        assertThat(desc.outputType()).isEqualTo("ARRAY<VARCHAR>");
        assertThat(desc.argumentCount()).isEqualTo(2);
        assertThat(desc.functionName()).isEqualTo("TOKENIZE");
    }

    @Test
    @DisplayName("resolve returns correct descriptor for NORMALIZE")
    void resolveNormalize() {
        FeatureTransformDescriptor desc = registry.resolve(FeatureTransformType.NORMALIZE);
        assertThat(desc.type()).isEqualTo(FeatureTransformType.NORMALIZE);
        assertThat(desc.inputTypeFamily()).isEqualTo("NUMERIC");
        assertThat(desc.outputType()).isEqualTo("DOUBLE");
    }

    @Test
    @DisplayName("resolve returns correct descriptor for ONE_HOT_ENCODE")
    void resolveOneHotEncode() {
        FeatureTransformDescriptor desc = registry.resolve(FeatureTransformType.ONE_HOT_ENCODE);
        assertThat(desc.type()).isEqualTo(FeatureTransformType.ONE_HOT_ENCODE);
        assertThat(desc.inputTypeFamily()).isEqualTo("VARCHAR");
        assertThat(desc.outputType()).isEqualTo("ARRAY<INT>");
    }

    @Test
    @DisplayName("resolve returns correct descriptor for BUCKETIZE")
    void resolveBucketize() {
        FeatureTransformDescriptor desc = registry.resolve(FeatureTransformType.BUCKETIZE);
        assertThat(desc.type()).isEqualTo(FeatureTransformType.BUCKETIZE);
        assertThat(desc.inputTypeFamily()).isEqualTo("NUMERIC");
        assertThat(desc.outputType()).isEqualTo("INT");
    }

    @Test
    @DisplayName("resolve throws on null type")
    void resolveNullThrows() {
        assertThatThrownBy(() -> registry.resolve(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("type must not be null");
    }

    @Test
    @DisplayName("resolveByFunctionName resolves case-insensitively")
    void resolveByFunctionNameCaseInsensitive() {
        assertThat(registry.resolveByFunctionName("tokenize").type())
                .isEqualTo(FeatureTransformType.TOKENIZE);
        assertThat(registry.resolveByFunctionName("NORMALIZE").type())
                .isEqualTo(FeatureTransformType.NORMALIZE);
        assertThat(registry.resolveByFunctionName("One_Hot_Encode").type())
                .isEqualTo(FeatureTransformType.ONE_HOT_ENCODE);
    }

    @Test
    @DisplayName("resolveByFunctionName throws on unknown function")
    void resolveByFunctionNameUnknownThrows() {
        assertThatThrownBy(() -> registry.resolveByFunctionName("UNKNOWN_FUNC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown feature transform function");
    }

    @Test
    @DisplayName("isFeatureTransform returns true for registered transforms")
    void isFeatureTransformTrue() {
        assertThat(registry.isFeatureTransform("TOKENIZE")).isTrue();
        assertThat(registry.isFeatureTransform("normalize")).isTrue();
        assertThat(registry.isFeatureTransform("ONE_HOT_ENCODE")).isTrue();
        assertThat(registry.isFeatureTransform("bucketize")).isTrue();
    }

    @Test
    @DisplayName("isFeatureTransform returns false for non-transforms")
    void isFeatureTransformFalse() {
        assertThat(registry.isFeatureTransform("ai_embed")).isFalse();
        assertThat(registry.isFeatureTransform("SUM")).isFalse();
        assertThat(registry.isFeatureTransform("")).isFalse();
        assertThat(registry.isFeatureTransform(null)).isFalse();
    }
}
