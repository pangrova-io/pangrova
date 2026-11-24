package org.pangrova.generation.execution.binding;

import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.generation.api.graph.GenerationStageGraph.StagePlan;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.protocol.substrait.SubstraitProtocolVersion;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DurableStageBindingsTest {
    @Test
    void equivalentClaimsReuseTheValidatedTemplateAndDifferentAuthorityIsRejected() {
        var original = stage(7);
        var bindings = new GenerationPartitionBindings.StageBindings(original, List.of());
        var first = bindings.requireStage(stage(7), List.of());
        assertThat(bindings.requireStage(stage(7), List.of())).isSameAs(first);
        assertThat(bindings.planBytes()).containsExactly(original.planBytes());

        assertThatThrownBy(() -> bindings.requireStage(stage(8), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("validated stage contract");
        var changedAuthority = new ModelSnapshot("model", "incarnation", 2, "v2", "a".repeat(64));
        assertThatThrownBy(() -> bindings.requireStage(stage(7), List.of(changedAuthority)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("validated stage contract");
    }

    private static StagePlan stage(long constant) {
        var read = ReadRel.newBuilder().setVirtualTable(ReadRel.VirtualTable.newBuilder()
                .addValues(Expression.Literal.Struct.newBuilder()
                        .addFields(Expression.Literal.newBuilder().setI64(constant))));
        var plan = Plan.newBuilder().setVersion(SubstraitProtocolVersion.build()).addRelations(
                PlanRel.newBuilder().setRoot(RelRoot.newBuilder().setInput(Rel.newBuilder().setRead(read))
                        .addNames("value")));
        return StagePlan.of(0, 2, GenerationPageKind.RELATIONAL_COMPUTE, plan.build().toByteArray(),
                GenerationStageTemplate.of(new byte[0], List.of(), List.of()), List.of(), List.of("value"), false);
    }
}
