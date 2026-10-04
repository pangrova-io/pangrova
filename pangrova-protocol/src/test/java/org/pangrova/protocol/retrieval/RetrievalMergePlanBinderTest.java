package org.pangrova.protocol.retrieval;

import java.util.List;

import com.google.protobuf.Any;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.ScoreDirection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetrievalMergePlanBinderTest {

    @Test
    void bindsRrfAboveCompleteSingletonExchangeWithoutTouchingRows() throws Exception {
        NamedStruct schema = candidateSchema();
        byte[] bound = RetrievalMergePlanBinder.bind(
                exchangePlan(schema, true, ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON),
                binding(schema, RetrievalMergeBinding.Strategy.RRF),
                "query-1-stage-2-merge");

        Rel root = Plan.parseFrom(bound).getRelations(0).getRoot().getInput();
        assertThat(root.hasExtensionSingle()).isTrue();
        assertThat(root.getExtensionSingle().getInput().hasExtensionLeaf()).isTrue();
        RelationDetailEnvelope envelope = root.getExtensionSingle().getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(envelope.getRetrievalMerge().getPlanNodeId())
                .isEqualTo("query-1-stage-2-merge");
        assertThat(envelope.getRetrievalMerge().getInvocation().getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.rrfMergePlanRef());
        assertThat(envelope.getRetrievalMerge().getInputsList())
                .extracting(input -> input.getSourceId())
                .containsExactly("vector", "text");
        assertThat(envelope.getRetrievalMerge().getInvocation().getOptions(0)
                .getValue().getInt64Value()).isEqualTo(60);
    }

    @Test
    void bindsLinearWeightsByStableSourceId() throws Exception {
        NamedStruct schema = candidateSchema();
        byte[] bound = RetrievalMergePlanBinder.bind(
                exchangePlan(schema, true, ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON),
                binding(schema, RetrievalMergeBinding.Strategy.LINEAR),
                "query-1-stage-2-merge");

        RelationDetailEnvelope envelope = Plan.parseFrom(bound).getRelations(0)
                .getRoot().getInput().getExtensionSingle().getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(envelope.getRetrievalMerge().getInvocation().getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.linearMergePlanRef());
        assertThat(envelope.getRetrievalMerge().getInvocation().getOptionsList())
                .extracting(option -> option.getName())
                .containsExactly("weight.vector", "weight.text");
    }

    @Test
    void bindsMatchingOperatorExecutionContract() {
        NamedStruct schema = candidateSchema();
        RetrievalMergeBinding binding = binding(
                schema, RetrievalMergeBinding.Strategy.RRF);

        BoundExecutionPlan bound = RetrievalMergePlanBinder.bindExecution(
                exchangePlan(schema, true,
                        ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON),
                binding,
                "query-1-stage-2-merge");

        assertThat(bound.operatorBindings()).hasSize(1);
        var operator = bound.operatorBindings().get(0);
        assertThat(operator.getPlanNodeId()).isEqualTo("query-1-stage-2-merge");
        assertThat(operator.getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.rrfMergePlanRef());
        assertThat(operator.getExecutionPolicy().getCoveragePolicy())
                .isEqualTo(org.pangrova.protocol.v2.CoveragePolicy
                        .COVERAGE_POLICY_REQUIRE_COMPLETE);
        assertThat(operator.getExecutionPolicy().getApproximation().getCandidateLimit())
                .isEqualTo(binding.sources().stream()
                        .mapToLong(RetrievalMergeBinding.Source::candidateLimit)
                        .sum());
        assertThat(operator.getExecutionPolicy().getApproximation().getExactRescoreLimit())
                .isEqualTo(binding.outputLimit());
    }

    @Test
    void replacesGlobalTopKAndRemovesInternalSourceTag() throws Exception {
        NamedStruct schema = candidateSchema();
        BoundExecutionPlan bound = RetrievalMergePlanBinder.bindFinalTopKExecution(
                globalTopKPlan(visibleSchema()),
                binding(schema, RetrievalMergeBinding.Strategy.RRF),
                "query-1-stage-2-merge");

        Rel root = Plan.parseFrom(bound.planBytes()).getRelations(0)
                .getRoot().getInput();
        assertThat(root.hasProject()).isTrue();
        assertThat(root.getProject().getExpressionsCount()).isEqualTo(2);
        assertThat(root.getProject().getCommon().getEmit().getOutputMappingList()).containsExactly(3, 4);
        assertThat(org.pangrova.protocol.substrait.SubstraitRelationLayout.describe(root)
                .outputSchema()).isEqualTo(visibleSchema());
        assertThat(root.getProject().getExpressions(0).getSelection().hasRootReference()).isTrue();
        Rel merge = root.getProject().getInput();
        assertThat(merge.hasExtensionSingle()).isTrue();
        RelationDetailEnvelope mergeEnvelope = merge.getExtensionSingle()
                .getDetail().unpack(RelationDetailEnvelope.class);
        assertThat(mergeEnvelope.hasRetrievalMerge()).isTrue();
        RelationDetailEnvelope exchangeEnvelope = merge.getExtensionSingle()
                .getInput().getExtensionLeaf().getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(exchangeEnvelope.getExchangeInput().getOutputSchema())
                .isEqualTo(schema);
        assertThat(bound.operatorBindings()).hasSize(1);
    }

    @Test
    void rejectsPartialNonSingletonAndSchemaDrift() {
        NamedStruct schema = candidateSchema();
        RetrievalMergeBinding binding = binding(schema, RetrievalMergeBinding.Strategy.RRF);
        assertThatThrownBy(() -> RetrievalMergePlanBinder.bind(
                exchangePlan(schema, false, ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON),
                binding, "merge"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete singleton");
        assertThatThrownBy(() -> RetrievalMergePlanBinder.bind(
                exchangePlan(schema, true, ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH),
                binding, "merge"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete singleton");

        NamedStruct drifted = schema.toBuilder().setNames(0, "other_id").build();
        assertThatThrownBy(() -> RetrievalMergePlanBinder.bind(
                exchangePlan(drifted, true,
                        ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON),
                binding, "merge"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema differs");
    }

    private static RetrievalMergeBinding binding(
            NamedStruct schema, RetrievalMergeBinding.Strategy strategy) {
        return new RetrievalMergeBinding(
                strategy,
                1,
                2,
                List.of(0),
                10,
                2,
                60,
                List.of(
                        new RetrievalMergeBinding.Source(
                                "vector",
                                ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                                0.7,
                                40),
                        new RetrievalMergeBinding.Source(
                                "text",
                                ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                                0.3,
                                40)),
                schema.toByteArray());
    }

    private static byte[] exchangePlan(
            NamedStruct schema,
            boolean complete,
            ExchangeDistribution distribution) {
        int partitions = distribution == ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON
                ? 1 : 8;
        RelationDetailEnvelope envelope = RelationDetailEnvelope.newBuilder()
                .setSchemaVersion(1)
                .setExchangeInput(ExchangeInputDetail.newBuilder()
                        .setPlanNodeId("candidate-exchange")
                        .setExchangeId("exchange-1-to-2")
                        .setUpstreamStageId(1)
                        .setDownstreamStageId(2)
                        .setDistribution(distribution)
                        .setPartitionCount(partitions)
                        .setOutputSchema(schema)
                        .setBatchSerde(BatchSerde.BATCH_SERDE_ARROW_BATCH)
                        .setRequireComplete(complete))
                .build();
        Rel input = Rel.newBuilder().setExtensionLeaf(
                io.substrait.proto.ExtensionLeafRel.newBuilder()
                        .setDetail(Any.pack(envelope)))
                .build();
        return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder()
                .setRoot(io.substrait.proto.RelRoot.newBuilder()
                        .setInput(input)
                        .addAllNames(schema.getNamesList())))
                .build().toByteArray();
    }

    private static byte[] globalTopKPlan(NamedStruct visibleSchema) {
        try {
            Rel exchange = Plan.parseFrom(exchangePlan(
                    visibleSchema, true,
                    ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON))
                    .getRelations(0).getRoot().getInput();
            Rel sort = Rel.newBuilder().setSort(io.substrait.proto.SortRel
                    .newBuilder().setInput(exchange).addSorts(
                            SortField.newBuilder().setExpr(field(1))
                                    .setDirection(SortField.SortDirection
                                            .SORT_DIRECTION_ASC_NULLS_LAST)))
                    .build();
            Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel
                    .newBuilder().setInput(sort).setCountExpr(SubstraitFetchBounds.literal(10))).build();
            return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel
                    .newBuilder().setRoot(io.substrait.proto.RelRoot.newBuilder()
                            .setInput(fetch)
                            .addAllNames(visibleSchema.getNamesList())))
                    .build().toByteArray();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static io.substrait.proto.Expression field(int index) {
        return org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(index);
    }

    private static NamedStruct visibleSchema() {
        return NamedStruct.newBuilder()
                .addNames("id")
                .addNames("score")
                .setStruct(io.substrait.proto.Type.Struct.newBuilder()
                        .addTypes(io.substrait.proto.Type.newBuilder().setI64(
                                io.substrait.proto.Type.I64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(io.substrait.proto.Type.newBuilder().setFp64(
                                io.substrait.proto.Type.FP64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))))
                .build();
    }

    private static NamedStruct candidateSchema() {
        return NamedStruct.newBuilder()
                .addNames("id")
                .addNames("score")
                .addNames("source_id")
                .setStruct(io.substrait.proto.Type.Struct.newBuilder()
                        .addTypes(io.substrait.proto.Type.newBuilder().setI64(
                                io.substrait.proto.Type.I64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(io.substrait.proto.Type.newBuilder().setFp64(
                                io.substrait.proto.Type.FP64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))
                        .addTypes(io.substrait.proto.Type.newBuilder().setString(
                                io.substrait.proto.Type.String.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED))))
                .build();
    }
}
