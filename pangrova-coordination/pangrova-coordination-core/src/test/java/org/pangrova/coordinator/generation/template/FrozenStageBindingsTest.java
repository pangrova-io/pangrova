package org.pangrova.coordinator.generation.template;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.execution.GenerationStageGraph.StagePlan;
import org.pangrova.generation.api.execution.template.GenerationStageTemplate;
import org.pangrova.generation.api.GenerationPageKind;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.security.api.authz.DeferredAuthorizationScope.ModelSnapshot;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrozenStageBindingsTest {
    private static final ModelSnapshot MODEL = new ModelSnapshot("lake.embed", "actual-model-incarnation", 7,
            "deployment-v7", "0".repeat(64));

    @Test
    void freezesActualModelContractAndLogicalOutputWithoutRuntimeAttemptIdentity() {
        var live = model().toBuilder().setExecutionPolicy(ExecutionPolicy.newBuilder()
                .setDeadlineEpochMs(12345).setIdempotencyKey("live-attempt")).build();
        var type = ResultColumnLogicalType.newBuilder().setColumnIndex(2)
                .setLogicalType(VectorLogicalType.create(768)).build();
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(new byte[]{1}, List.of(live), List.of(type)),
                List.of(MODEL));
        var replay = FrozenStageBindings.read(stage(template), List.of(MODEL));
        assertThat(replay.operatorBindings().get(0).getModel()).isEqualTo(live.getModel());
        assertThat(replay.operatorBindings().get(0).getExecutionPolicy().getDeadlineEpochMs()).isZero();
        assertThat(replay.operatorBindings().get(0).getExecutionPolicy().getIdempotencyKey()).isEmpty();
        assertThat(replay.resultColumnTypes()).containsExactly(type);
    }

    @Test
    void storageDoubleVectorReplaysThroughExistingTemplateWithoutModelAuthority() {
        var type = ResultColumnLogicalType.newBuilder().setColumnIndex(2)
                .setLogicalType(VectorLogicalType.create(3, VectorLogicalType.FLOAT64, true, false)).build();
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(new byte[]{1}, List.of(), List.of(type)), List.of());
        assertThat(FrozenStageBindings.resultColumnTypes(stage(template))).containsExactly(type);
        assertThat(FrozenStageBindings.read(stage(template), List.of()).resultColumnTypes()).containsExactly(type);
        var narrow = StagePlan.of(1, 1, GenerationPageKind.RELATIONAL_COMPUTE, new byte[]{1}, template,
                List.of("lake.raw.documents"), List.of("embedding"), false);
        assertThatThrownBy(() -> FrozenStageBindings.resultColumnTypes(narrow))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("stage width");
    }

    @Test
    void rejectsVersionSignatureOrMissingCurrentModelObservation() {
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(new byte[]{1}, List.of(model())), List.of(MODEL));
        assertThatThrownBy(() -> FrozenStageBindings.read(stage(template), List.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("model authority");
        assertThatThrownBy(() -> FrozenStageBindings.read(stage(template), List.of(new ModelSnapshot(
                MODEL.logicalName(), MODEL.incarnation(), 7, "deployment-v8", MODEL.signature()))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("model authority");
        assertThatThrownBy(() -> FrozenStageBindings.read(stage(template), List.of(new ModelSnapshot(
                MODEL.logicalName(), MODEL.incarnation(), 7, MODEL.deploymentVersion(), "f".repeat(64)))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("model authority");
    }

    @Test
    void rejectsCredentialsUnknownNestedFieldsAndRepeatedOperatorIdentity() {
        var credential = model().toBuilder().setModel(model().getModel().toBuilder().setCredentialRef("provider-secret"));
        assertThatThrownBy(() -> freeze(credential.build())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credentials");
        var unknown = UnknownFieldSet.newBuilder().addField(123,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var extended = model().toBuilder().setModel(model().getModel().toBuilder().setUnknownFields(unknown));
        assertThatThrownBy(() -> freeze(extended.build())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown protocol");
        assertThatThrownBy(() -> FrozenStageBindings.freeze(new BoundExecutionPlan(new byte[]{1},
                List.of(model(), model())), List.of(MODEL))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }

    @Test
    void rejectsPersistedDeadlineAndMissingOrDuplicateLogicalOutputContracts() {
        var runtime = model().toBuilder().setExecutionPolicy(ExecutionPolicy.newBuilder().setDeadlineEpochMs(123)).build();
        var template = GenerationStageTemplate.of(OperatorBindingSet.newBuilder().addBindings(runtime).build().toByteArray(),
                List.of(), List.of());
        assertThatThrownBy(() -> FrozenStageBindings.read(stage(template), List.of(MODEL)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dispatched execution policy");
        var invalid = model().toBuilder().setModel(model().getModel().toBuilder().clearOutputType()).build();
        assertThatThrownBy(() -> freeze(invalid)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("logical type");
        var output = ResultColumnLogicalType.newBuilder().setLogicalType(VectorLogicalType.create(768)).build();
        assertThatThrownBy(() -> FrozenStageBindings.freeze(new BoundExecutionPlan(new byte[]{1},
                List.of(model()), List.of(output, output)), List.of(MODEL))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ordinals");
    }

    private static GenerationStageTemplate freeze(OperatorBinding binding) {
        return FrozenStageBindings.freeze(new BoundExecutionPlan(new byte[]{1}, List.of(binding)), List.of(MODEL));
    }

    private static OperatorBinding model() {
        return OperatorBinding.newBuilder().setPlanNodeId("embed-1")
                .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                .setModel(ModelBinding.newBuilder().setModel(VersionedRef.newBuilder().setId(MODEL.logicalName())
                        .setVersion(MODEL.deploymentVersion()).setSignatureSha256(ByteString.copyFrom(new byte[32])))
                        .setOutputType(VectorLogicalType.create(768))).build();
    }

    private static StagePlan stage(GenerationStageTemplate template) {
        return StagePlan.of(1, 1, GenerationPageKind.RELATIONAL_COMPUTE, new byte[]{1}, template,
                List.of("lake.raw.documents"), List.of("id", "content", "embedding"), false);
    }
}
