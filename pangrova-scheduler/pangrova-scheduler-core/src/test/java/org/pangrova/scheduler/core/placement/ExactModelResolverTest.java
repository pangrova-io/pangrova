package org.pangrova.scheduler.core.placement;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.scheduler.api.error.NoAvailableWorkerException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExactModelResolverTest {
    private static final CapabilityRef REQUIRED = BuiltInCapabilityCatalog.crossEncoderRerankPlanRef();

    @Test
    void replicasResolveOnceAndPreserveTheExactServingEnvelope() {
        var model = model();
        assertThat(ExactModelResolver.resolve(List.of(inventory(model), inventory(model)), "reranker", REQUIRED))
                .isEqualTo(model);
    }

    @Test
    void differentLimitsAreAmbiguousEvenWithTheSameModelIdentity() {
        var model = model();
        var other = model.toBuilder().setLimits(model.getLimits().toBuilder().setMaxConcurrency(2)).build();
        assertThatThrownBy(() -> ExactModelResolver.resolve(
                List.of(inventory(model), inventory(other)), "reranker", REQUIRED))
                .isInstanceOf(NoAvailableWorkerException.class).hasMessageContaining("multiple active");
    }

    @Test
    void absentOrPinnedBuildMismatchCannotResolveAnotherImplementation() {
        assertThatThrownBy(() -> ExactModelResolver.resolve(List.of(), "reranker", REQUIRED))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThatThrownBy(() -> ExactModelResolver.resolve(List.of(inventory(model())), "reranker",
                REQUIRED.toBuilder().setImplementationBuildId("different-build").build()))
                .isInstanceOf(NoAvailableWorkerException.class);
        assertThatThrownBy(() -> ExactModelResolver.resolve(List.of(inventory(model())), "other", REQUIRED))
                .isInstanceOf(NoAvailableWorkerException.class);
    }

    @Test
    void incompleteLimitsOrVersionAreRejectedBeforeBinding() {
        var model = model();
        for (var invalid : List.of(
                model.toBuilder().setLimits(model.getLimits().toBuilder().clearMaxProviderItemsPerTask()).build(),
                model.toBuilder().setLimits(model.getLimits().toBuilder().clearMaxRequestBytes()).build(),
                model.toBuilder().setModel(model.getModel().toBuilder().clearVersion()).build())) {
            assertThatThrownBy(() -> ExactModelResolver.resolve(List.of(inventory(invalid)), "reranker", REQUIRED))
                    .isInstanceOf(NoAvailableWorkerException.class).hasMessageContaining("incomplete serving envelope");
        }
    }

    @Test
    void unspecifiedIdentityCannotMeanAnyModel() {
        assertThatThrownBy(() -> ExactModelResolver.resolve(List.of(inventory(model())), " ", REQUIRED))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ExactModelResolver.resolve(List.of(inventory(model())), "reranker",
                REQUIRED.toBuilder().clearSignatureSha256().build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static CapabilityInventory inventory(ModelAvailability model) {
        return CapabilityInventory.newBuilder().addModels(model).build();
    }

    private static ModelAvailability model() {
        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder().setId("reranker").setVersion("v1")
                        .setSignatureSha256(ByteString.copyFrom(new byte[32])))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(REQUIRED.toBuilder().setImplementationBuildId("build-a"))
                        .setBindingContractSha256(ByteString.copyFrom(new byte[32]))
                        .setOutputType(org.pangrova.protocol.v2.LogicalType.newBuilder().setCanonicalSql("DOUBLE")
                                .setSubstraitType(io.substrait.proto.Type.newBuilder()
                                        .setFp64(io.substrait.proto.Type.FP64.newBuilder()
                                                .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED)))))
                .setLimits(CapabilityLimits.newBuilder().setMaxBatchItems(16).setMaxInputTokens(8192)
                        .setMaxOutputTokens(1024).setMaxConcurrency(4).setMaxProviderItemsPerTask(100)
                        .setMaxProviderAttemptsPerTask(100).setMaxRequestBytes(1048576).setMaxResponseBytes(2097152))
                .build();
    }
}
