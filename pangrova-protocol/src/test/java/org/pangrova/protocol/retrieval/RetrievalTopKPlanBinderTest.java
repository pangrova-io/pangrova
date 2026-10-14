package org.pangrova.protocol.retrieval;

import java.util.List;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExchangeInputDetail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetrievalTopKPlanBinderTest {

    @Test
    void replacesSortFetchWithTypedExactTopKRelation() throws Exception {
        NamedStruct schema = candidateSchema();
        byte[] bound = RetrievalTopKPlanBinder.bind(
                plan(schema).toByteArray(),
                List.of(RetrievalTopKBinding.exact(
                        1, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                        List.of(0), schema.toByteArray())),
                "query-1-stage-0");

        Rel root = Plan.parseFrom(bound).getRelations(0).getRoot().getInput();
        assertThat(root.hasExtensionSingle()).isTrue();
        assertThat(root.getExtensionSingle().getInput().hasRead()).isTrue();
        RelationDetailEnvelope envelope = root.getExtensionSingle().getDetail().unpack(
                RelationDetailEnvelope.class);
        assertThat(envelope.getSchemaVersion()).isEqualTo(1);
        assertThat(envelope.getRetrievalSource().getCandidateLimit()).isEqualTo(10);
        assertThat(envelope.getRetrievalSource().getInvocation().getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.exactTopKPlanRef());
        assertThat(envelope.getRetrievalSource().getOutputContract().getStableKeyFieldsList())
                .containsExactly("id");
    }

    @Test
    void exactTopKRetainsOrdinarySortWhenJoinColumnsShareAnIdentityName() throws Exception {
        NamedStruct original = candidateSchema();
        NamedStruct duplicated = original.toBuilder()
                .addNames("id")
                .setStruct(original.getStruct().toBuilder().addTypes(original.getStruct().getTypes(0)))
                .build();
        Plan sqlPlan = plan(duplicated);

        BoundExecutionPlan bound = RetrievalTopKPlanBinder.bindExecution(
                sqlPlan.toByteArray(),
                List.of(RetrievalTopKBinding.exact(
                        1, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                        List.of(0, 2), duplicated.toByteArray())),
                "duplicate-join-key");

        assertThat(Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput()
                .getFetch().getInput().hasSort()).isTrue();
        assertThat(bound.operatorBindings()).isEmpty();
    }

    @Test
    void exactTopKRetainsOrdinarySortWhenBindingNamesDriftFromInput() throws Exception {
        NamedStruct actual = candidateSchema();
        NamedStruct requested = actual.toBuilder().setNames(0, "renamed_id").build();

        BoundExecutionPlan bound = RetrievalTopKPlanBinder.bindExecution(
                plan(actual).toByteArray(),
                List.of(RetrievalTopKBinding.exact(
                        1, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                        List.of(0), requested.toByteArray())),
                "drifted-candidate-name");

        assertThat(Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput()
                .getFetch().getInput().hasSort()).isTrue();
        assertThat(bound.operatorBindings()).isEmpty();
    }

    @Test
    void bindsPlannerExpressionEncodedFetchLimit() throws Exception {
        NamedStruct schema = candidateSchema();
        Plan originalPlan = plan(schema);
        Rel fetch = originalPlan.getRelations(0).getRoot().getInput();
        var expressionFetch = fetch.getFetch().toBuilder()
                .setCountExpr(Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setI64(10)))
                .setOffsetExpr(Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setI64(0)))
                .build();
        Plan plannerPlan = originalPlan.toBuilder().setRelations(0,
                originalPlan.getRelations(0).toBuilder().setRoot(
                        originalPlan.getRelations(0).getRoot().toBuilder()
                                .setInput(fetch.toBuilder().setFetch(expressionFetch))))
                .build();

        byte[] bound = RetrievalTopKPlanBinder.bind(plannerPlan.toByteArray(),
                List.of(RetrievalTopKBinding.exact(
                        1, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                        List.of(0), schema.toByteArray())), "query-1-stage-0");

        assertThat(Plan.parseFrom(bound).getRelations(0).getRoot().getInput()
                .hasExtensionSingle()).isTrue();
    }

    @Test
    void unmatchedBindingFailsClosed() {
        NamedStruct schema = candidateSchema();
        RetrievalTopKBinding wrongLimit = RetrievalTopKBinding.exact(
                1, 11, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                List.of(0), schema.toByteArray());

        assertThatThrownBy(() -> RetrievalTopKPlanBinder.bind(
                plan(schema).toByteArray(), List.of(wrongLimit), "query-1-stage-0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unmatched");
    }

    @Test
    void bindsGlobalTopKDirectlyAboveCanonicalSingletonExchangeInput() throws Exception {
        NamedStruct schema = candidateSchema();
        RelationDetailEnvelope exchange = RelationDetailEnvelope.newBuilder()
                .setSchemaVersion(1)
                .setExchangeInput(ExchangeInputDetail.newBuilder()
                        .setPlanNodeId("exchange-input-global")
                        .setExchangeId("exchange-0-to-1")
                        .setUpstreamStageId(0)
                        .setDownstreamStageId(1)
                        .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)
                        .setPartitionCount(1)
                        .setOutputSchema(schema)
                        .setBatchSerde(org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH)
                        .setRequireComplete(true))
                .build();
        Rel input = Rel.newBuilder().setExtensionLeaf(
                io.substrait.proto.ExtensionLeafRel.newBuilder()
                        .setDetail(com.google.protobuf.Any.pack(exchange)))
                .build();
        byte[] bound = RetrievalTopKPlanBinder.bind(
                planWithInput(input).toByteArray(),
                List.of(RetrievalTopKBinding.exact(
                        1, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                        List.of(0), schema.toByteArray())),
                "query-1-stage-1");

        Rel root = Plan.parseFrom(bound).getRelations(0).getRoot().getInput();
        assertThat(root.hasExtensionSingle()).isTrue();
        assertThat(root.getExtensionSingle().getInput().hasExtensionLeaf()).isTrue();
        RelationDetailEnvelope detail = root.getExtensionSingle().getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(detail.getRetrievalSource().getPlanNodeId())
                .isEqualTo("query-1-stage-1-topk-0");
    }

    @Test
    void bindsBm25ScoreToNativeMaterializedScanColumn() throws Exception {
        NamedStruct schema = candidateSchema();
        RetrievalTopKBinding binding = RetrievalTopKBinding.artifact(
                1, 10, ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                List.of(0), schema.toByteArray(),
                new RetrievalTopKBinding.ArtifactInput(
                        "items", BuiltInCapabilityCatalog.bm25RetrievalPlanRef(),
                        bm25Options(), ArtifactBinding.newBuilder()
                                .setArtifactId("text-index")
                                .build(),
                        "bm25-shard-exact", 10, 1.0, 10));

        BoundExecutionPlan bound = RetrievalTopKPlanBinder.bindExecution(
                bm25Plan().toByteArray(), List.of(binding), "query-bm25-stage-0");

        Rel root = Plan.parseFrom(bound.planBytes()).getRelations(0)
                .getRoot().getInput();
        Rel input = root.getExtensionSingle().getInput();
        assertThat(input.hasProject()).isTrue();
        assertThat(input.getProject().getExpressions(1).hasCast()).isTrue();
        Rel filter = input.getProject().getInput();
        assertThat(filter.hasFilter()).isTrue();
        assertThat(filter.getFilter().getCondition().getLiteral().getBoolean()).isTrue();
        Rel read = filter.getFilter().getInput();
        assertThat(read.getRead().getBaseSchema().getNamesList())
                .endsWith(BuiltInCapabilityCatalog.MATERIALIZED_RETRIEVAL_SCORE_COLUMN);
        assertThat(read.getRead().getBaseSchema().getStruct().getTypesList())
                .last().satisfies(type -> assertThat(type.hasFp64()).isTrue());
        assertThat(bound.operatorBindings()).singleElement().satisfies(operator -> {
            assertThat(operator.getCapability())
                    .isEqualTo(BuiltInCapabilityCatalog.bm25RetrievalPlanRef());
            assertThat(operator.getExecutionPolicy().getApproximation().getProfileId())
                    .isEqualTo("bm25-shard-exact");
        });
    }

    @Test
    void rejectsBm25BindingAgainstAnUnrelatedScoreExpression() {
        NamedStruct schema = candidateSchema();
        RetrievalTopKBinding binding = RetrievalTopKBinding.artifact(
                1, 10, ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                List.of(0), schema.toByteArray(),
                new RetrievalTopKBinding.ArtifactInput(
                        "items", BuiltInCapabilityCatalog.bm25RetrievalPlanRef(),
                        bm25Options(), ArtifactBinding.newBuilder()
                                .setArtifactId("text-index")
                                .build(),
                        "bm25-shard-exact", 10, 1.0, 10));
        Plan unrelated = bm25Plan().toBuilder()
                .setExtensions(0, SimpleExtensionDeclaration.newBuilder()
                        .setExtensionFunction(
                                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                                        .setFunctionAnchor(7)
                                        .setName("l2_distance:list_list")))
                .build();

        assertThatThrownBy(() -> RetrievalTopKPlanBinder.bindExecution(
                unrelated.toByteArray(), List.of(binding), "query-bm25-stage-0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void composesSortAndFetchEmitsInsteadOfDroppingTheInnerProjection() throws Exception {
        Plan original = plan(candidateSchema());
        var fetch = original.getRelations(0).getRoot().getInput().getFetch();
        var sort = fetch.getInput().getSort().toBuilder().setCommon(emit(1, 0, 1));
        var root = original.getRelations(0).getRoot().toBuilder().setInput(Rel.newBuilder().setFetch(
                fetch.toBuilder().setInput(Rel.newBuilder().setSort(sort)).setCommon(emit(2, 1, 2))));
        Plan remapped = original.toBuilder().setRelations(0, original.getRelations(0).toBuilder().setRoot(root)).build();
        var bound = RetrievalTopKPlanBinder.bind(remapped.toByteArray(), List.of(RetrievalTopKBinding.exact(
                1, 10, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER, List.of(0), candidateSchema().toByteArray())), "remapped");
        var extension = Plan.parseFrom(bound).getRelations(0).getRoot().getInput().getExtensionSingle();
        assertThat(extension.getCommon().getEmit().getOutputMappingList()).containsExactly(1, 0, 1);
        assertThat(extension.getDetail().unpack(RelationDetailEnvelope.class).getRetrievalSource()
                .getOutputContract().getSchema().getNamesList()).containsExactly("id", "score");
    }

    @Test
    void materializedBm25TraversesReadFilterAndIntermediateProjectRemaps() throws Exception {
        Plan original = bm25Plan();
        var fetch = original.getRelations(0).getRoot().getInput().getFetch();
        var sort = fetch.getInput().getSort();
        var scoreProject = sort.getInput().getProject();
        var filter = scoreProject.getInput().getFilter();
        var remappedRead = filter.getInput().getRead().toBuilder().setCommon(emit(1, 0));
        var condition = filter.getCondition().toBuilder().setScalarFunction(filter.getCondition().getScalarFunction()
                .toBuilder().setArguments(0, io.substrait.proto.FunctionArgument.newBuilder().setValue(fieldReference(0))));
        Rel intermediate = Rel.newBuilder().setProject(io.substrait.proto.ProjectRel.newBuilder().setCommon(emit(1, 0))
                .setInput(Rel.newBuilder().setFilter(filter.toBuilder().setCondition(condition)
                        .setInput(Rel.newBuilder().setRead(remappedRead))))).build();
        var updatedSort = sort.toBuilder().setInput(Rel.newBuilder().setProject(scoreProject.toBuilder().setInput(intermediate)));
        var updatedRoot = original.getRelations(0).getRoot().toBuilder().setInput(Rel.newBuilder()
                .setFetch(fetch.toBuilder().setInput(Rel.newBuilder().setSort(updatedSort))));
        Plan remapped = original.toBuilder().setRelations(0, original.getRelations(0).toBuilder().setRoot(updatedRoot)).build();
        var binding = RetrievalTopKBinding.artifact(1, 10, ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                List.of(0), candidateSchema().toByteArray(), new RetrievalTopKBinding.ArtifactInput("items",
                        BuiltInCapabilityCatalog.bm25RetrievalPlanRef(), bm25Options(),
                        ArtifactBinding.newBuilder().setArtifactId("text-index").build(), "bm25-shard-exact", 10, 1.0, 10));
        var bound = RetrievalTopKPlanBinder.bind(remapped.toByteArray(), List.of(binding), "remapped-bm25");
        var top = Plan.parseFrom(bound).getRelations(0).getRoot().getInput().getExtensionSingle().getInput().getProject();
        assertThat(top.getCommon().getEmit().getOutputMappingList()).containsExactly(3, 4);
        assertThat(top.getExpressions(1).getCast().getInput().getSelection().getDirectReference().getStructField().getField())
                .isEqualTo(2);
        var middle = top.getInput().getProject();
        assertThat(middle.getExpressionsList()).isEmpty();
        assertThat(middle.getCommon().getEmit().getOutputMappingList()).containsExactly(1, 0, 2);
        assertThat(middle.getInput().getFilter().getCondition().getLiteral().getBoolean()).isTrue();
        assertThat(middle.getInput().getFilter().getInput().getRead().getCommon().getEmit().getOutputMappingList())
                .containsExactly(1, 0, 2);
    }

    private static Plan plan(NamedStruct schema) {
        Rel read = Rel.newBuilder().setRead(io.substrait.proto.ReadRel.newBuilder()
                .setBaseSchema(schema)
                .setNamedTable(io.substrait.proto.ReadRel.NamedTable.newBuilder()
                        .addNames("items"))).build();
        Rel sort = Rel.newBuilder().setSort(io.substrait.proto.SortRel.newBuilder()
                .setInput(read)
                .addSorts(SortField.newBuilder()
                        .setExpr(fieldReference(1))
                        .setDirection(SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST)))
                .build();
        Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel.newBuilder()
                .setInput(sort)
                .setOffsetExpr(integer(0))
                .setCountExpr(integer(10)))
                .build();
        return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder()
                .setRoot(io.substrait.proto.RelRoot.newBuilder()
                        .setInput(fetch)
                        .addNames("id")
                        .addNames("score")))
                .build();
    }

    private static Plan bm25Plan() {
        NamedStruct readSchema = NamedStruct.newBuilder()
                .addNames("id")
                .addNames("content")
                .setStruct(io.substrait.proto.Type.Struct.newBuilder()
                        .addTypes(io.substrait.proto.Type.newBuilder().setI64(
                                io.substrait.proto.Type.I64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(io.substrait.proto.Type.newBuilder().setString(
                                io.substrait.proto.Type.String.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE))))
                .build();
        Rel read = Rel.newBuilder().setRead(io.substrait.proto.ReadRel.newBuilder()
                .setBaseSchema(readSchema)
                .setNamedTable(io.substrait.proto.ReadRel.NamedTable.newBuilder()
                        .addNames("items"))).build();
        var match = Expression.newBuilder().setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(8)
                        .setOutputType(io.substrait.proto.Type.newBuilder().setBool(
                                io.substrait.proto.Type.Boolean.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))
                        .addArguments(io.substrait.proto.FunctionArgument.newBuilder()
                                .setValue(fieldReference(1)))
                        .addArguments(io.substrait.proto.FunctionArgument.newBuilder()
                                .setValue(Expression.newBuilder().setLiteral(
                                        io.substrait.proto.Expression.Literal.newBuilder()
                                                .setString("distributed search")))))
                .build();
        Rel filter = Rel.newBuilder().setFilter(io.substrait.proto.FilterRel.newBuilder()
                .setInput(read)
                .setCondition(match)).build();
        var score = Expression.newBuilder().setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(7)
                        .setOutputType(io.substrait.proto.Type.newBuilder().setFp64(
                                io.substrait.proto.Type.FP64.newBuilder()
                                        .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))
                        .addArguments(io.substrait.proto.FunctionArgument.newBuilder()
                                .setValue(fieldReference(1)))
                        .addArguments(io.substrait.proto.FunctionArgument.newBuilder()
                                .setValue(Expression.newBuilder().setLiteral(
                                        io.substrait.proto.Expression.Literal.newBuilder()
                                                .setString("distributed search")))))
                .build();
        Rel project = Rel.newBuilder().setProject(io.substrait.proto.ProjectRel.newBuilder()
                .setInput(filter)
                .setCommon(io.substrait.proto.RelCommon.newBuilder().setEmit(
                        io.substrait.proto.RelCommon.Emit.newBuilder().addOutputMapping(2).addOutputMapping(3)))
                .addExpressions(fieldReference(0))
                .addExpressions(score)).build();
        Rel sort = Rel.newBuilder().setSort(io.substrait.proto.SortRel.newBuilder()
                .setInput(project)
                .addSorts(SortField.newBuilder()
                        .setExpr(fieldReference(1))
                        .setDirection(SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST)))
                .build();
        Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel.newBuilder()
                .setInput(sort).setCountExpr(integer(10))).build();
        return Plan.newBuilder()
                .addExtensions(SimpleExtensionDeclaration.newBuilder()
                        .setExtensionFunction(
                                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                                        .setFunctionAnchor(7)
                                        .setName("bm25_score:string_string")))
                .addExtensions(SimpleExtensionDeclaration.newBuilder()
                        .setExtensionFunction(
                                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                                        .setFunctionAnchor(8)
                                        .setName("match:string_string")))
                .addRelations(io.substrait.proto.PlanRel.newBuilder()
                        .setRoot(io.substrait.proto.RelRoot.newBuilder()
                                .setInput(fetch).addNames("id").addNames("score")))
                .build();
    }

    private static List<NamedValue> bm25Options() {
        return List.of(
                namedString("query_text", "distributed search"),
                namedString("analyzer", "standard-v1"),
                namedDouble("k1", 1.2),
                namedDouble("b", 0.75));
    }

    private static NamedValue namedString(String name, String value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                        .setStringValue(value)).build();
    }

    private static NamedValue namedDouble(String name, double value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("DOUBLE"))
                        .setDoubleValue(value)).build();
    }

    private static Plan planWithInput(Rel input) {
        Rel sort = Rel.newBuilder().setSort(io.substrait.proto.SortRel.newBuilder()
                .setInput(input)
                .addSorts(SortField.newBuilder()
                        .setExpr(fieldReference(1))
                        .setDirection(SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST)))
                .build();
        Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel.newBuilder()
                .setInput(sort)
                .setCountExpr(integer(10)))
                .build();
        return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder()
                .setRoot(io.substrait.proto.RelRoot.newBuilder()
                        .setInput(fetch)
                        .addNames("id")
                        .addNames("score")))
                .build();
    }

    private static NamedStruct candidateSchema() {
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

    private static Expression fieldReference(int fieldIndex) {
        return org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(fieldIndex);
    }

    private static Expression integer(long value) {
        return Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(value)).build();
    }

    private static io.substrait.proto.RelCommon emit(int... fields) {
        var mapping = io.substrait.proto.RelCommon.Emit.newBuilder();
        for (int field : fields) mapping.addOutputMapping(field);
        return io.substrait.proto.RelCommon.newBuilder().setEmit(mapping).build();
    }
}
