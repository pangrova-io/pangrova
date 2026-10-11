package org.pangrova.protocol.retrieval;

import java.util.List;

import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.FilterRel;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.protocol.v2.TypedValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetrievalFanOutPlanBinderTest {

    @Test
    void createsIndependentVectorAndBm25BranchesBelowOneExchange() throws Exception {
        NamedStruct schema = candidateSchema();
        RetrievalTopKBinding vector = RetrievalTopKBinding.exact(
                1, 40, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                List.of(0), schema.toByteArray());
        RetrievalTopKBinding text = RetrievalTopKBinding.artifact(
                1, 10, 80, ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                List.of(0), schema.toByteArray(),
                new RetrievalTopKBinding.ArtifactInput(
                        "catalog.schema.docs",
                        BuiltInCapabilityCatalog.bm25RetrievalPlanRef(),
                        bm25Options(), ArtifactBinding.getDefaultInstance(),
                        "bm25-shard-exact", 10, 1.0, 10));

        BoundExecutionPlan bound = RetrievalFanOutPlanBinder.bindExecution(
                producerPlan(schema),
                new RetrievalFanOutBinding(List.of(
                        new RetrievalFanOutBinding.Source("vector", vector),
                        new RetrievalFanOutBinding.Source("text", text))),
                "query-1-stage-0");

        Rel exchange = Plan.parseFrom(bound.planBytes()).getRelations(0)
                .getRoot().getInput();
        assertThat(exchange.hasExchange()).isTrue();
        assertThat(org.pangrova.protocol.substrait.SubstraitRelationLayout.describe(exchange)
                .outputSchema()).isEqualTo(RetrievalFanOutPlanBinder.candidateSchema(vector));
        Rel union = exchange.getExchange().getInput();
        assertThat(union.hasSet()).isTrue();
        assertThat(union.getSet().getInputsCount()).isEqualTo(2);
        assertThat(union.getSet().getInputsList()).allSatisfy(branch -> {
            assertThat(branch.hasProject()).isTrue();
            assertThat(branch.getProject().getExpressionsCount()).isEqualTo(3);
            assertThat(branch.getProject().getExpressions(2).getLiteral().getString())
                    .isIn("vector", "text");
            assertThat(branch.getProject().getInput().hasExtensionSingle()).isTrue();
            assertThat(branch.getProject().getCommon().getEmit().getOutputMappingList())
                    .containsExactly(2, 3, 4);
            assertThat(branch.getProject().getExpressions(0).getSelection().hasRootReference()).isTrue();
        });
        assertThat(bound.operatorBindings()).hasSize(2);
        assertThat(union.getSet().getInputsList())
                .extracting(RetrievalFanOutPlanBinderTest::candidateLimit)
                .containsExactlyInAnyOrder(40L, 80L);
        assertThat(RetrievalFanOutPlanBinder.candidateSchema(vector).getNamesList())
                .containsExactly("id", "proj_1",
                        RetrievalFanOutPlanBinder.sourceFieldName(vector));
    }

    private static long candidateLimit(Rel branch) {
        try {
            return branch.getProject().getInput().getExtensionSingle().getDetail()
                    .unpack(RelationDetailEnvelope.class)
                    .getRetrievalSource().getCandidateLimit();
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new AssertionError(failure);
        }
    }

    @Test
    void rejectsUnspecifiedScoreNullabilityBeforeRewritingBranches() {
        var schema = candidateSchema().toBuilder().setStruct(candidateSchema().getStruct()
                .toBuilder().setTypes(1, Type.newBuilder().setFp64(Type.FP64.newBuilder()))).build();
        var first = RetrievalTopKBinding.exact(1, 40, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                List.of(0), schema.toByteArray());
        var second = RetrievalTopKBinding.exact(1, 80, ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER,
                List.of(0), schema.toByteArray());
        var fanOut = new RetrievalFanOutBinding(List.of(new RetrievalFanOutBinding.Source("first", first),
                new RetrievalFanOutBinding.Source("second", second)));
        assertThatThrownBy(() -> RetrievalFanOutPlanBinder.bindExecution(producerPlan(schema), fanOut, "query"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit compatible nullability");
    }

    private static byte[] producerPlan(NamedStruct schema) {
        Rel read = Rel.newBuilder().setRead(ReadRel.newBuilder()
                .setBaseSchema(NamedStruct.newBuilder()
                        .addNames("id").addNames("content").addNames("embedding")
                        .setStruct(Type.Struct.newBuilder()
                                .addTypes(i64()).addTypes(string()).addTypes(string())))
                .setNamedTable(ReadRel.NamedTable.newBuilder()
                        .addNames("catalog").addNames("schema").addNames("docs")))
                .build();
        Expression match = call(1, Type.newBuilder().setBool(
                        Type.Boolean.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build(),
                field(1), stringLiteral("distributed"));
        Rel filter = Rel.newBuilder().setFilter(FilterRel.newBuilder()
                .setInput(read).setCondition(match)).build();
        Expression distance = call(2, fp64(),
                field(2), stringLiteral("query-vector"));
        Rel project = Rel.newBuilder().setProject(ProjectRel.newBuilder()
                .setInput(filter).addExpressions(field(0)).addExpressions(distance)
                .setCommon(io.substrait.proto.RelCommon.newBuilder().setEmit(
                        io.substrait.proto.RelCommon.Emit.newBuilder().addOutputMapping(3).addOutputMapping(4)))).build();
        Rel sort = Rel.newBuilder().setSort(SortRel.newBuilder()
                .setInput(project)
                .addSorts(SortField.newBuilder().setExpr(field(1))
                        .setDirection(SortField.SortDirection
                                .SORT_DIRECTION_ASC_NULLS_LAST))).build();
        Rel fetch = Rel.newBuilder().setFetch(FetchRel.newBuilder()
                .setInput(sort).setCountExpr(SubstraitFetchBounds.literal(10))).build();
        Rel exchange = Rel.newBuilder().setExchange(
                io.substrait.proto.ExchangeRel.newBuilder().setInput(fetch)
                        .setRoundRobin(io.substrait.proto.ExchangeRel
                                .RoundRobin.newBuilder())).build();
        return Plan.newBuilder()
                .addExtensions(function(1, "match"))
                .addExtensions(function(2, "l2_distance"))
                .addRelations(io.substrait.proto.PlanRel.newBuilder()
                        .setRoot(io.substrait.proto.RelRoot.newBuilder()
                                .setInput(exchange)
                                .addAllNames(schema.getNamesList())))
                .build().toByteArray();
    }

    private static SimpleExtensionDeclaration function(int anchor, String name) {
        return SimpleExtensionDeclaration.newBuilder().setExtensionFunction(
                SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
                        .setFunctionAnchor(anchor).setName(name)).build();
    }

    private static Expression call(
            int anchor, Type outputType, Expression first, Expression second) {
        return Expression.newBuilder().setScalarFunction(
                Expression.ScalarFunction.newBuilder()
                        .setFunctionReference(anchor)
                        .addArguments(FunctionArgument.newBuilder().setValue(first))
                        .addArguments(FunctionArgument.newBuilder().setValue(second))
                        .setOutputType(outputType)).build();
    }

    private static Expression field(int index) {
        return org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(index);
    }

    private static Expression stringLiteral(String value) {
        return Expression.newBuilder().setLiteral(
                Expression.Literal.newBuilder().setString(value)).build();
    }

    private static List<NamedValue> bm25Options() {
        return List.of(
                stringOption("query_text", "distributed"),
                stringOption("analyzer", "standard-v1"),
                doubleOption("k1", 1.2),
                doubleOption("b", 0.75));
    }

    private static NamedValue stringOption(String name, String value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder().setStringValue(value)).build();
    }

    private static NamedValue doubleOption(String name, double value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder().setDoubleValue(value)).build();
    }

    private static NamedStruct candidateSchema() {
        return NamedStruct.newBuilder().addNames("id").addNames("proj_1")
                .setStruct(Type.Struct.newBuilder().addTypes(i64()).addTypes(fp64()))
                .build();
    }

    private static Type i64() {
        return Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static Type fp64() {
        return Type.newBuilder().setFp64(Type.FP64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
    }

    private static Type string() {
        return Type.newBuilder().setString(Type.String.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
    }
}
