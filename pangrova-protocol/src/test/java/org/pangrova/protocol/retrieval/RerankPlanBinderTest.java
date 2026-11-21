package org.pangrova.protocol.retrieval;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.RetryClass;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RerankPlanBinderTest {

    @Test
    void bindsExactModelAndGovernedRerankContract() throws Exception {
        NamedStruct schema = schema();
        byte[] signature = MessageDigest.getInstance("SHA-256")
                .digest("reranker@v7".getBytes(StandardCharsets.UTF_8));

        BoundExecutionPlan bound = RerankPlanBinder.bindExecution(
                plan(schema),
                new RerankBinding(
                        2,
                        1,
                        List.of(0),
                        80,
                        10,
                        "reranker",
                        "v7",
                        signature,
                        doubleOutput(),
                        limits(),
                        "distributed database",
                        schema.toByteArray()),
                "query-1-stage-2-rerank");

        RelationDetailEnvelope envelope = Plan.parseFrom(bound.planBytes())
                .getRelations(0).getRoot().getInput().getExtensionSingle()
                .getDetail().unpack(RelationDetailEnvelope.class);
        assertThat(envelope.hasRerank()).isTrue();
        assertThat(envelope.getRerank().getModel().getModelId())
                .isEqualTo("reranker");
        assertThat(envelope.getRerank().getModel().getVersion())
                .isEqualTo("v7");
        assertThat(envelope.getRerank().getInputLimit()).isEqualTo(80);
        assertThat(envelope.getRerank().getOutputLimit()).isEqualTo(10);
        assertThat(envelope.getRerank().getInvocation().getArgumentsList())
                .extracting(argument -> argument.getName())
                .containsExactly("score", "query", "document");
        assertThat(bound.operatorBindings()).singleElement().satisfies(operator -> {
            assertThat(operator.getModel().getModel().getId())
                    .isEqualTo("reranker");
            assertThat(operator.getExecutionPolicy().getCoveragePolicy())
                    .isEqualTo(CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE);
            assertThat(operator.getExecutionPolicy().getRetryClass())
                    .isEqualTo(RetryClass.RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY);
            assertThat(operator.getResources().getProviderItems())
                    .isEqualTo(80);
            assertThat(operator.getResources().getProviderRequestBytes())
                    .isEqualTo(8 * 65_536L);
            assertThat(operator.getResources().getProviderAttempts()).isEqualTo(8);
            assertThat(operator.getResources().getProviderResponseBytes()).isEqualTo(8 * 16_384L);
            assertThat(operator.getModel().getRequestLimits().getMaxInputTokens()).isEqualTo(8_192);
        });
    }

    @Test
    void outputBindingOverfetchesBoundedCandidatesBeforeFinalRerank()
            throws Exception {
        NamedStruct schema = schema();
        byte[] signature = MessageDigest.getInstance("SHA-256")
                .digest("reranker@v7".getBytes(StandardCharsets.UTF_8));
        RerankBinding binding = new RerankBinding(
                2, 1, List.of(0), 80, 10,
                "reranker", "v7", signature,
                doubleOutput(),
                limits(),
                "distributed database", schema.toByteArray());

        BoundExecutionPlan bound = RerankPlanBinder.bindFinalTopKExecution(
                globalTopKPlan(schema, 10), binding, "rerank-output");

        Rel rerank = Plan.parseFrom(bound.planBytes()).getRelations(0)
                .getRoot().getInput();
        assertThat(rerank.hasExtensionSingle()).isTrue();
        assertThat(rerank.getExtensionSingle().getInput().getFetch().getCountExpr().getLiteral().getI64())
                .isEqualTo(80);
    }

    private static byte[] plan(NamedStruct schema) {
        Rel read = Rel.newBuilder().setRead(ReadRel.newBuilder()
                .setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder()
                        .addNames("catalog").addNames("schema").addNames("docs")))
                .build();
        return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel
                .newBuilder().setRoot(io.substrait.proto.RelRoot.newBuilder()
                        .setInput(read)
                        .addAllNames(schema.getNamesList())))
                .build().toByteArray();
    }

    private static org.pangrova.protocol.v2.LogicalType doubleOutput() {
        return org.pangrova.protocol.v2.LogicalType.newBuilder()
                .setCanonicalSql("DOUBLE")
                .setSubstraitType(Type.newBuilder().setFp64(
                        Type.FP64.newBuilder().setNullability(
                                Type.Nullability.NULLABILITY_NULLABLE)))
                .build();
    }

    private static byte[] globalTopKPlan(NamedStruct schema, long limit)
            throws Exception {
        Rel read = Plan.parseFrom(plan(schema)).getRelations(0)
                .getRoot().getInput();
        Rel sort = Rel.newBuilder().setSort(io.substrait.proto.SortRel
                .newBuilder().setInput(read).addSorts(SortField.newBuilder()
                        .setExpr(field(2))
                        .setDirection(SortField.SortDirection
                                .SORT_DIRECTION_DESC_NULLS_LAST))).build();
        Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel
                .newBuilder().setInput(sort).setCountExpr(SubstraitFetchBounds.literal(limit))).build();
        return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel
                .newBuilder().setRoot(io.substrait.proto.RelRoot.newBuilder()
                        .setInput(fetch).addAllNames(schema.getNamesList())))
                .build().toByteArray();
    }

    private static io.substrait.proto.Expression field(int index) {
        return org.pangrova.protocol.substrait.SubstraitRelationLayout.rootFieldReference(index);
    }

    private static org.pangrova.protocol.v2.CapabilityLimits limits() {
        return org.pangrova.protocol.v2.CapabilityLimits.newBuilder().setMaxBatchItems(16)
                .setMaxInputTokens(8_192).setMaxConcurrency(4).setMaxProviderItemsPerTask(80)
                .setMaxProviderAttemptsPerTask(8).setMaxRequestBytes(65_536).setMaxResponseBytes(16_384).build();
    }

    private static NamedStruct schema() {
        return NamedStruct.newBuilder()
                .addNames("id")
                .addNames("content")
                .addNames("score")
                .setStruct(Type.Struct.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                        .addTypes(Type.newBuilder().setI64(
                                Type.I64.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setString(
                                Type.String.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE)))
                        .addTypes(Type.newBuilder().setFp64(
                                Type.FP64.newBuilder().setNullability(Type.Nullability.NULLABILITY_NULLABLE))))
                .build();
    }
}
