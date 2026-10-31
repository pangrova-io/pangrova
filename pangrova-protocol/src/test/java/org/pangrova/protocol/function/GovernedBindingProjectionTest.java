package org.pangrova.protocol.function;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.ExecutionReusePolicy;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.VersionedRef;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Authority projection components, not JNI authentication or provider end-to-end tests. */
class GovernedBindingProjectionTest {
    @Test
    void completeBindingAndEveryAuthorityDimensionAreCheckedAgainstTheAdmittedValues() {
        var binding = binding();
        var layout = FunctionBindingDeclarations.governedLayout("AI_MODEL");
        var encoded = encoded("AI_MODEL", binding);
        assertThatCode(() -> GovernedBindingProjection.validateValues("AI_MODEL", binding, encoded))
                .doesNotThrowAnyException();
        var values = GovernedBindingProjection.values("AI_MODEL", binding);
        assertThat(values.get("invocation_id")).isEqualTo(new GovernedScalarValue.Text("ai-invoke-0"));
        assertThat(values.get("output_vector_dimension")).isEqualTo(new GovernedScalarValue.Unsigned(384));
        for (var argument : layout.arguments()) {
            List<String> changed = new ArrayList<>(encoded);
            changed.set(layout.ordinal(argument.name()), tamper(argument.encoding()));
            assertThatThrownBy(() -> GovernedBindingProjection.validateValues("AI_MODEL", binding, changed))
                    .as(argument.name()).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void exactFloatComparisonUsesBitsNotCrossLanguageStringFormatting() {
        var binding = binding().toBuilder().setReusePolicy(ExecutionReusePolicy.newBuilder()
                .setSimilarityThreshold(0.1)).build();
        var encoded = new ArrayList<>(encoded("AI_MODEL", binding));
        int threshold = FunctionBindingDeclarations.governedLayout("AI_MODEL").ordinal("reuse_similarity_threshold");
        encoded.set(threshold, "0.10000000000000001");
        assertThatCode(() -> GovernedBindingProjection.validateValues("AI_MODEL", binding, encoded))
                .doesNotThrowAnyException();
        encoded.set(threshold, "0.10000000000000002");
        assertThatThrownBy(() -> GovernedBindingProjection.validateValues("AI_MODEL", binding, encoded))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reuse_similarity_threshold");
    }

    @Test
    void rerankDataExpressionsAreExcludedButAllRemainingAuthorityIsBound() {
        var binding = binding();
        var values = GovernedBindingProjection.values("AI_RERANK", binding);
        assertThat(values).doesNotContainKeys("query", "document");
        var encoded = new ArrayList<>(encoded("AI_RERANK", binding));
        var layout = FunctionBindingDeclarations.governedLayout("AI_RERANK");
        encoded.set(layout.ordinal("query"), null);
        encoded.set(layout.ordinal("document"), "actual row document");
        assertThatCode(() -> GovernedBindingProjection.validateValues("AI_RERANK", binding, encoded))
                .doesNotThrowAnyException();
        encoded.set(layout.ordinal("provider_attempts"), "12");
        assertThatThrownBy(() -> GovernedBindingProjection.validateValues("AI_RERANK", binding, encoded))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("provider_attempts");
    }

    @Test
    void absentMessagesUnrepresentableUnsignedAndNoncanonicalNumbersFailClosed() {
        assertThatThrownBy(() -> GovernedBindingProjection.values("AI_MODEL", binding().toBuilder()
                .setModel(binding().getModel().toBuilder().clearRequestLimits()).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> GovernedBindingProjection.values("AI_MODEL", binding().toBuilder()
                .setResources(binding().getResources().toBuilder().setProviderAttempts(-1)).build()))
                .isInstanceOf(IllegalArgumentException.class);
        for (String value : List.of("01", "+1", "-1", "9223372036854775808")) {
            assertThatThrownBy(() -> GovernedScalarEncoding.decode(value, "UNSIGNED_DECIMAL"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (String value : List.of("NaN", "Infinity", "0x1.0p0", "1.0suffix")) {
            assertThatThrownBy(() -> GovernedScalarEncoding.decode(value, "EXACT_FLOAT"))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static OperatorBinding binding() {
        return OperatorBinding.newBuilder().setPlanNodeId("ai-invoke-0")
                .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                .setModel(ModelBinding.newBuilder().setModel(VersionedRef.newBuilder().setId("model")
                        .setVersion("v1").setSignatureSha256(ByteString.copyFrom(new byte[32])))
                        .setOutputType(VectorLogicalType.create(384)).setRequestLimits(ModelRequestLimits.newBuilder()
                                .setMaxInputTokens(900).setMaxOutputTokens(100).setMaxRequestBytes(128).setMaxResponseBytes(256)))
                .setResources(OperatorResourceRequirement.newBuilder().setProviderConcurrency(3)
                        .setProviderItems(8).setProviderAttempts(11).setProviderRequestBytes(1024)
                        .setProviderResponseBytes(2048).setMediaBytes(0))
                .setExecutionPolicy(ExecutionPolicy.newBuilder().setIdempotencyKey("task:ai-invoke-0")).build();
    }

    private static List<String> encoded(String layoutId, OperatorBinding binding) {
        var projected = GovernedBindingProjection.values(layoutId, binding);
        return FunctionBindingDeclarations.governedLayout(layoutId).arguments().stream().map(argument -> {
            var value = projected.get(argument.name());
            if (value == null) return "data";
            if (value instanceof GovernedScalarValue.Text text) return text.value();
            if (value instanceof GovernedScalarValue.Signature signature) return Base64.getEncoder().encodeToString(signature.value().toByteArray());
            if (value instanceof GovernedScalarValue.Unsigned unsigned) return Long.toString(unsigned.value());
            if (value instanceof GovernedScalarValue.Flag flag) return Boolean.toString(flag.value());
            return Double.toString(((GovernedScalarValue.Floating) value).value());
        }).toList();
    }

    private static String tamper(String encoding) {
        return switch (encoding) {
            case "STRING" -> "tampered";
            case "BASE64" -> Base64.getEncoder().encodeToString(new byte[32]).replace('A', 'B');
            case "UNSIGNED_DECIMAL" -> "999";
            case "BOOLEAN" -> "true";
            case "EXACT_FLOAT" -> "0.75";
            default -> throw new IllegalArgumentException(encoding);
        };
    }
}
