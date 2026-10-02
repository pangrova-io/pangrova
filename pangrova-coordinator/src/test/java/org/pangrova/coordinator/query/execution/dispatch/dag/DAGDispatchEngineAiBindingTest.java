package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.execution.dispatch.dag.binding.AiInvocationOperatorBinder;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.planner.AiInvocationPlanHint;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.spi.security.UserIdentity;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DAGDispatchEngineAiBindingTest {

    private static final UserIdentity USER = new UserIdentity(
            "ai-user", Set.of("ai_users"), null, Instant.EPOCH);

    @Test
    void appendsExactModelAndBoundedProviderResources() {
        ModelAvailability model = model(limits(5, 10, 6, 0, 2));
        OperatorBinding bound = AiInvocationOperatorBinder.bindQuery(
                fragment(AiInvocationPlanHint.Operation.TEXT_GENERATION, false)
                        .aiInvocationHints().get(0),
                selector(model), USER);

        assertThat(bound).satisfies(operator -> {
            assertThat(operator.getPlanNodeId()).isEqualTo("ai-invoke-0");
            assertThat(operator.getModel().getModel()).isEqualTo(model.getModel());
            assertThat(operator.getModel().getOutputType()).isEqualTo(varcharOutput());
            assertThat(operator.getResources().getProviderItems()).isEqualTo(5);
            assertThat(operator.getResources().getProviderConcurrency()).isEqualTo(2);
            assertThat(operator.getResources().getProviderAttempts()).isEqualTo(5);
            assertThat(operator.getResources().getProviderRequestBytes()).isEqualTo(5 * 1_048_576L);
            assertThat(operator.getResources().getProviderResponseBytes()).isEqualTo(5 * 2_097_152L);
            assertThat(operator.getModel().getRequestLimits().getMaxInputTokens()).isEqualTo(10);
            assertThat(operator.getModel().getRequestLimits().getMaxOutputTokens()).isEqualTo(6);
            assertThat(operator.getResources().getMediaBytes()).isZero();
        });
    }

    @Test
    void requiresOperationSpecificGenerationAndMediaLimits() {
        assertThatThrownBy(() -> AiInvocationOperatorBinder.bindQuery(
                fragment(AiInvocationPlanHint.Operation.TEXT_GENERATION, false)
                        .aiInvocationHints().get(0),
                selector(model(limits(5, 10, 0, 0, 2))), USER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("output-token");

        assertThatThrownBy(() -> AiInvocationOperatorBinder.bindQuery(
                fragment(AiInvocationPlanHint.Operation.MULTIMODAL_GENERATION, true)
                        .aiInvocationHints().get(0),
                selector(model(limits(5, 10, 6, 0, 2))), USER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("media-byte");
    }

    private static SerializedPlanFragment fragment(
            AiInvocationPlanHint.Operation operation, boolean media) {
        return new SerializedPlanFragment(
                new byte[] {1},
                List.of(),
                List.of(new AiInvocationPlanHint(
                        "ai-invoke-0", "ai_complete", "catalog.chat",
                        operation, media)));
    }

    private static WorkerSelector selector(ModelAvailability model) {
        return new WorkerSelector() {
            @Override
            public WorkerInfo select(String tenantId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public StageResourceGrant computeResourceLimit(
                    int parallelism, int concurrentStages, WorkerInfo selectedWorker) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ModelAvailability resolveExactModel(
                    String modelName,
                    CapabilityRef capability,
                    UserIdentity user) {
                assertThat(modelName).isEqualTo("catalog.chat");
                assertThat(user).isEqualTo(USER);
                return model;
            }
        };
    }

    private static ModelAvailability model(CapabilityLimits limits) {
        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId("catalog.chat")
                        .setVersion("v7")
                        .setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(org.pangrova.protocol.BuiltInCapabilityCatalog
                                .aiTextGenerationPlanRef())
                        .setOutputType(varcharOutput())
                        .setBindingContractSha256(ByteString.copyFrom(new byte[32])))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(org.pangrova.protocol.BuiltInCapabilityCatalog
                                .aiMultimodalGenerationPlanRef())
                        .setOutputType(varcharOutput())
                        .setBindingContractSha256(ByteString.copyFrom(new byte[32])))
                .setLimits(limits)
                .build();
    }

    private static LogicalType varcharOutput() {
        return LogicalType.newBuilder()
                .setCanonicalSql("VARCHAR")
                .setSubstraitType(io.substrait.proto.Type.newBuilder()
                        .setString(io.substrait.proto.Type.String.newBuilder()
                                .setNullability(io.substrait.proto.Type.Nullability
                                        .NULLABILITY_NULLABLE)))
                .build();
    }

    private static CapabilityLimits limits(
            long invocations,
            long inputTokens,
            long outputTokens,
            long mediaBytes,
            int concurrency) {
        return CapabilityLimits.newBuilder()
                .setMaxBatchItems(16)
                .setMaxProviderItemsPerTask(invocations)
                                .setMaxProviderAttemptsPerTask(invocations)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152)
                .setMaxInputTokens(inputTokens)
                .setMaxOutputTokens(outputTokens)
                .setMaxMediaBytes(mediaBytes)
                .setMaxConcurrency(concurrency)
                .build();
    }
}
