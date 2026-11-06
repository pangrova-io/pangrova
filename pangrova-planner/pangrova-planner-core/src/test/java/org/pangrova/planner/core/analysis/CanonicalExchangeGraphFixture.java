package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

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
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.List;

/** Actual canonical Values/exchange bytes; no simulated planner, connector or execution authority. */
final class CanonicalExchangeGraphFixture {
    private CanonicalExchangeGraphFixture() {}

    static NamedStruct schema() {
        Type decimal = Type.newBuilder().setDecimal(Type.Decimal.newBuilder().setPrecision(38).setScale(18)
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        Type timestamp = Type.newBuilder().setPrecisionTimestamp(Type.PrecisionTimestamp.newBuilder().setPrecision(6)
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        Type row = Type.newBuilder().setStruct(Type.Struct.newBuilder().addTypes(decimal).addTypes(timestamp)
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        Type array = Type.newBuilder().setList(Type.List.newBuilder().setType(row)
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        Type map = Type.newBuilder().setMap(Type.Map.newBuilder().setKey(Type.newBuilder().setString(
                        Type.String.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                .setValue(row).setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
        return NamedStruct.newBuilder().addAllNames(List.of("record", "amount", "created", "rows", "sum", "at",
                        "lookup", "value_amount", "value_created"))
                .setStruct(Type.Struct.newBuilder().addTypes(row).addTypes(array).addTypes(map)
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    static StageTransfer edge() {
        return new StageTransfer(1, 0, ShuffleType.SINGLETON, List.of(), "gather", 1,
                BatchSerde.BATCH_SERDE_ARROW_BATCH, true);
    }

    static Rel values(NamedStruct schema, int rows) {
        var values = ReadRel.VirtualTable.newBuilder();
        for (int i = 0; i < rows; i++) {
            var row = Expression.Literal.Struct.newBuilder();
            schema.getStruct().getTypesList().forEach(type -> row.addFields(
                    Expression.Literal.newBuilder().setNull(type)));
            values.addValues(row);
        }
        return Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema).setVirtualTable(values)).build();
    }

    static Rel producer(Rel input) {
        return Rel.newBuilder().setExchange(ExchangeRel.newBuilder().setInput(input).setPartitionCount(1)
                .setRoundRobin(ExchangeRel.RoundRobin.newBuilder().setExact(true))).build();
    }

    static Rel consumer(NamedStruct schema) {
        var detail = ExchangeInputDetail.newBuilder().setPlanNodeId("consumer-input").setExchangeId("gather")
                .setUpstreamStageId(1).setDownstreamStageId(0).setPartitionCount(1).setRequireComplete(true)
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)
                .setBatchSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH).setOutputSchema(schema);
        return Rel.newBuilder().setExtensionLeaf(ExtensionLeafRel.newBuilder().setDetail(Any.pack(
                RelationDetailEnvelope.newBuilder().setSchemaVersion(1).setExchangeInput(detail).build()))).build();
    }

    static SerializedPlanFragment plan(Rel input, List<String> presentation) {
        var plan = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(input).addAllNames(presentation))).build();
        return new SerializedPlanFragment(plan.toByteArray(), List.of(), List.of());
    }

    static AuthorizedStageGraph.SerializedStage stage(int id, Rel input, List<String> presentation) {
        return new AuthorizedStageGraph.SerializedStage(id, 1, id == 0 ? "OUTPUT" : "COMPUTE",
                plan(input, presentation), List.of(),
                SubstraitRelationLayout.topLevelNames(NamedStruct.newBuilder().addAllNames(presentation)
                        .setStruct(SubstraitRelationLayout.describe(input).outputSchema().getStruct()).build()),
                List.of(), List.of(), -1, -1);
    }

    static AuthorizedStageGraph graph(NamedStruct source, NamedStruct consumer) {
        return new AuthorizedStageGraph(AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT,
                List.of(stage(1, producer(values(source, 3)), source.getNamesList()),
                        stage(0, consumer(consumer), consumer.getNamesList())), List.of(edge()));
    }
}
