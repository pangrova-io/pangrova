package org.pangrova.worker.ai.context;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelRequestLimits;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorResourceRequirement;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.spi.ai.AiCapabilityDescriptor.Operation;
import org.pangrova.spi.ai.AiInvocationException;
import org.pangrova.spi.ai.AiRequestConstraints;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exact canonical projection unit tests; no provider implementation or authorization stub. */
class GovernedAiBindingsTest {
    @Test
    void embeddingBindingHasOneCompleteCanonicalProjectionAndNoCallerSelectedOperation() {
        var expected = new BoundAiInvocation("invocation", "model", "v1", new byte[32], Operation.EMBEDDING,
                "task:invocation", new AiRequestConstraints(900, 100, 128, 256), 3, 8, 11, 1024, 2048, 0, 384);
        assertThat(GovernedAiBindings.fromAdmitted(binding())).isEqualTo(expected).hasSameHashCodeAs(expected);
        assertThat(GovernedAiBindings.fromAdmitted(binding().toBuilder().setResources(binding().getResources()
                .toBuilder().setProviderAttempts(12)).build())).isNotEqualTo(expected);
    }

    @Test
    void capabilityVersionOrSignatureCannotBeReclassifiedByTheCaller() {
        var binding = binding();
        assertThatThrownBy(() -> GovernedAiBindings.fromAdmitted(binding.toBuilder().setCapability(
                binding.getCapability().toBuilder().setSemanticVersion("wrong")).build()))
                .isInstanceOf(AiInvocationException.class);
        assertThatThrownBy(() -> GovernedAiBindings.fromAdmitted(binding.toBuilder().setCapability(
                binding.getCapability().toBuilder().setSignatureSha256(ByteString.copyFrom(new byte[32]))).build()))
                .isInstanceOf(AiInvocationException.class);
    }

    @Test
    void rerankUsesItsActualInternalCapabilityAndNeverInheritsAnEmbeddingDimension() {
        var binding = binding().toBuilder().setCapability(BuiltInCapabilityCatalog.crossEncoderRerankPlanRef())
                .setModel(binding().getModel().toBuilder().setOutputType(LogicalType.newBuilder()
                        .setCanonicalSql("DOUBLE").setSubstraitType(io.substrait.proto.Type.newBuilder()
                                .setFp64(io.substrait.proto.Type.FP64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))))
                .build();
        var actual = GovernedAiBindings.fromAdmitted(binding);
        assertThat(actual.operation()).isEqualTo(Operation.RERANKING);
        assertThat(actual.outputVectorDimension()).isZero();
        assertThat(actual.maxProviderItems()).isEqualTo(8);
        assertThat(actual.maxProviderAttempts()).isEqualTo(11);
    }

    private static OperatorBinding binding() {
        return OperatorBinding.newBuilder().setPlanNodeId("invocation")
                .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                .setModel(ModelBinding.newBuilder().setModel(VersionedRef.newBuilder().setId("model")
                        .setVersion("v1").setSignatureSha256(ByteString.copyFrom(new byte[32])))
                        .setOutputType(VectorLogicalType.create(384)).setRequestLimits(ModelRequestLimits.newBuilder()
                                .setMaxInputTokens(900).setMaxOutputTokens(100).setMaxRequestBytes(128).setMaxResponseBytes(256)))
                .setResources(OperatorResourceRequirement.newBuilder().setProviderConcurrency(3).setProviderItems(8)
                        .setProviderAttempts(11).setProviderRequestBytes(1024).setProviderResponseBytes(2048))
                .setExecutionPolicy(ExecutionPolicy.newBuilder().setIdempotencyKey("task:invocation")).build();
    }
}
