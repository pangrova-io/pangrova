package org.pangrova.coordinator.query.execution.dispatch.dag.control;

import org.pangrova.databuild.protocol.DataBuildStageGraphCodec;

import com.google.protobuf.Any;
import io.substrait.proto.ExchangeRel;
import io.substrait.proto.Expression;
import io.substrait.proto.ExtensionLeafRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.databuild.template.FrozenStageBindings;
import org.pangrova.databuild.api.execution.DataBuildStageGraph;
import org.pangrova.databuild.api.execution.template.DataBuildStageTemplate;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Persisted actual canonical bytes are schema-checked before replay; no RPC/execution claim. */
class DurableStageExchangeContractsTest {
    @Test
    void checksDecodedImmutablePlansWithoutChangingTemplatesBindingsOrPresentation() {
        var original = graph(schema());
        var persisted = DataBuildStageGraphCodec.decode(DataBuildStageGraphCodec.encode(original), DataBuildStageGraphCodec.sha256(original));
        assertThatCode(() -> DurableStageRuntime.validateExchangeContracts(persisted)).doesNotThrowAnyException();
        assertThat(persisted).isEqualTo(original);
        assertThat(persisted.requireStage(0).planBytes()).isEqualTo(original.requireStage(0).planBytes());
        assertThat(persisted.requireStage(0).bindingSetBytes()).isEqualTo(original.requireStage(0).bindingSetBytes());
        assertThat(FrozenStageBindings.read(persisted.requireStage(0), List.of()).operatorBindings())
                .containsExactly(binding());
    }

    @Test
    void recomputedCanonicalChecksumsDoNotAdmitChangedWireNamesTypesOrNullability() {
        var schema = schema();
        var changedNullability = schema.toBuilder().setStruct(schema.getStruct().toBuilder().setTypes(0,
                schema.getStruct().getTypes(0).toBuilder().setI64(schema.getStruct().getTypes(0).getI64().toBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))).build();
        var changedType = schema.toBuilder().setStruct(schema.getStruct().toBuilder().setTypes(0,
                Type.newBuilder().setI32(Type.I32.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE)))).build();
        for (NamedStruct changed : List.of(schema.toBuilder().setNames(0, "different").build(),
                changedNullability, changedType)) {
            var malformed = graph(changed);
            var persisted = DataBuildStageGraphCodec.decode(DataBuildStageGraphCodec.encode(malformed), DataBuildStageGraphCodec.sha256(malformed));
            assertThatThrownBy(() -> DurableStageRuntime.validateExchangeContracts(persisted))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("rebuild required")
                    .hasRootCauseMessage("Exchange input differs from frozen producer schema: gather");
        }
    }

    private static DataBuildStageGraph graph(NamedStruct consumerSchema) {
        NamedStruct source = schema();
        Rel read = Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(source).setVirtualTable(
                ReadRel.VirtualTable.newBuilder().addValues(Expression.Literal.Struct.newBuilder().addFields(
                        Expression.Literal.newBuilder().setI64(7).setNullable(true))))).build();
        Rel producer = Rel.newBuilder().setExchange(ExchangeRel.newBuilder().setInput(read).setPartitionCount(1)
                .setRoundRobin(ExchangeRel.RoundRobin.newBuilder().setExact(true))).build();
        var input = ExchangeInputDetail.newBuilder().setPlanNodeId("input").setExchangeId("gather")
                .setUpstreamStageId(1).setDownstreamStageId(0).setPartitionCount(1)
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)
                .setBatchSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH).setRequireComplete(true).setOutputSchema(consumerSchema);
        Rel consumer = Rel.newBuilder().setExtensionLeaf(ExtensionLeafRel.newBuilder().setDetail(Any.pack(
                RelationDetailEnvelope.newBuilder().setSchemaVersion(1).setExchangeInput(input).build()))).build();
        byte[] output = plan(consumer);
        var template = FrozenStageBindings.freeze(new BoundExecutionPlan(output, List.of(binding())), List.of());
        return new DataBuildStageGraph(DataBuildStageGraph.FORMAT_VERSION, DataBuildStageGraph.Purpose.RELATIONAL_EXPORT,
                List.of(DataBuildStageGraph.StagePlan.of(1, 1, DataBuildPageKind.RELATIONAL_COMPUTE, plan(producer),
                                DataBuildStageTemplate.of(new byte[0], List.of(), List.of()), List.of(), List.of("display"), false),
                        DataBuildStageGraph.StagePlan.of(0, 1, DataBuildPageKind.RELATIONAL_PARTITION, output,
                                template, List.of(), List.of("display"), true)),
                List.of(new DataBuildStageGraph.Exchange("gather", 1, 0,
                        DataBuildStageGraph.Exchange.Distribution.SINGLETON, List.of(), 1,
                        "BATCH_SERDE_ARROW_BATCH", true)), List.of());
    }

    private static OperatorBinding binding() {
        return OperatorBinding.newBuilder().setPlanNodeId("export")
                .setCapability(BuiltInCapabilityCatalog.tabularExportPlanRef()).build();
    }

    private static byte[] plan(Rel input) {
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(input).addNames("display"))).build().toByteArray();
    }

    private static NamedStruct schema() {
        return NamedStruct.newBuilder().addNames("wire_id").setStruct(Type.Struct.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED).addTypes(Type.newBuilder().setI64(
                        Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE)))).build();
    }
}
