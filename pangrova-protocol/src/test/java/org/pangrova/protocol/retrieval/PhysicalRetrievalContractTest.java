package org.pangrova.protocol.retrieval;

import java.util.List;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SortField;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.ArtifactBinding;
import org.pangrova.protocol.v2.CandidateIdentityKind;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.ScoreDirection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real canonical producer/consumer round trips; schemas are protocol inputs, not index evidence. */
class PhysicalRetrievalContractTest {
    private static final List<Integer> KEYS = List.of(2, 3, 4, 5);

    @Test
    void bindsVisibleKIndependentlyFromExplorationAndRetainsPhysicalChannels() throws Exception {
        NamedStruct schema = schema();
        var binding = RetrievalTopKBinding.artifact(1, 3, 40,
                ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER, KEYS, schema.toByteArray(),
                new RetrievalTopKBinding.ArtifactInput("items",
                        BuiltInCapabilityCatalog.filteredDiskAnnRetrievalPlanRef(), List.of(),
                        ArtifactBinding.newBuilder().setArtifactId("index-generation").build(),
                        "filtered-diskann-pq-l2", 40, 0.95, 3));
        var bound = RetrievalTopKPlanBinder.bindExecution(topK(schema).toByteArray(),
                List.of(binding), "filtered-stage");
        var detail = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput()
                .getExtensionSingle().getDetail().unpack(RelationDetailEnvelope.class).getRetrievalSource();
        assertThat(detail.getOutputLimit()).isEqualTo(3);
        assertThat(detail.getCandidateLimit()).isEqualTo(40);
        assertThat(detail.getExactRescoreLimit()).isEqualTo(40);
        assertThat(detail.getOutputContract().getSchemaVersion()).isEqualTo(2);
        assertThat(detail.getOutputContract().getIdentityKind())
                .isEqualTo(CandidateIdentityKind.CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW);
        assertThat(detail.getOutputContract().getStableKeyFieldsList())
                .containsExactlyElementsOf(RetrievalCandidateIdentity.PHYSICAL_FIELDS);
        assertThat(detail.getOutputContract().getSchema()).isEqualTo(schema);
        assertThat(bound.operatorBindings()).singleElement().satisfies(operator -> {
            assertThat(operator.getExecutionPolicy().getApproximation().getRecallK()).isEqualTo(3);
            assertThat(operator.getResources().getCandidateRows()).isEqualTo(40);
        });
    }

    @Test
    void restoresOnlyTheActualHiddenIdentityTrimAfterBinding() throws Exception {
        Rel input = read(schema());
        Rel trim = visible(input, 2);
        Plan original = rooted(trim, List.of("id", "score"));
        var scope = RetrievalOutputProjection.peel(original);
        assertThat(scope.inner().getRelations(0).getRoot().getInput()).isEqualTo(input);
        var restored = Plan.parseFrom(scope.restore(new BoundExecutionPlan(
                scope.inner().toByteArray(), List.of())).planBytes());
        assertThat(restored).isEqualTo(original);
        assertThat(SubstraitRelationLayout.describe(restored.getRelations(0).getRoot().getInput())
                .outputSchema().getNamesList()).containsExactly("id", "score");
    }

    @Test
    void ordinaryComputedSqlProjectionRemainsTheCandidateInput() {
        Rel project = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(read(schema()))
                .addExpressions(Expression.newBuilder().setLiteral(Expression.Literal.newBuilder().setI64(8)))
                .setCommon(RelCommon.newBuilder().setEmit(RelCommon.Emit.newBuilder().addOutputMapping(6))))
                .build();
        Plan original = rooted(project, List.of("computed"));
        assertThat(RetrievalOutputProjection.peel(original).inner()).isEqualTo(original);
    }

    @Test
    void invalidOrdinalWithinAnActualHiddenTrimFailsClosed() {
        Rel trim = visible(read(schema()), 2);
        trim = trim.toBuilder().setProject(trim.getProject().toBuilder().setExpressions(0,
                SubstraitRelationLayout.rootFieldReference(1))).build();
        Plan invalid = rooted(trim, List.of("id", "score"));
        assertThatThrownBy(() -> RetrievalOutputProjection.peel(invalid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("visible ordinal");
    }

    @Test
    void physicalIdentityRejectsIncompleteNullableDuplicateOrVisibleValueKeys() {
        assertThatThrownBy(() -> RetrievalCandidateIdentity.kind(schema(), List.of(0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("incomplete");
        assertThatThrownBy(() -> RetrievalCandidateIdentity.kind(schema(), List.of(2, 3, 4, 4)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid");
        var nullable = schema().toBuilder().setStruct(schema().getStruct().toBuilder().setTypes(3,
                Type.newBuilder().setString(Type.String.newBuilder()
                        .setNullability(Type.Nullability.NULLABILITY_NULLABLE)))).build();
        assertThatThrownBy(() -> RetrievalCandidateIdentity.kind(nullable, KEYS))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("types");
        var incomplete = schema().toBuilder().setNames(5, "ordinary_position").build();
        assertThatThrownBy(() -> RetrievalCandidateIdentity.kind(incomplete, KEYS))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("incomplete");
    }

    @Test
    void fanoutTagNeverBecomesPartOfPhysicalIdentity() {
        var tagged = schema().toBuilder().addNames("source_tag").setStruct(schema().getStruct()
                .toBuilder().addTypes(string())).build();
        assertThat(RetrievalCandidateIdentity.kind(tagged, KEYS))
                .isEqualTo(CandidateIdentityKind.CANDIDATE_IDENTITY_KIND_PHYSICAL_SOURCE_ROW);
        assertThatThrownBy(() -> RetrievalCandidateIdentity.kind(tagged, List.of(2, 3, 4, 5, 6)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("incomplete");
    }

    private static NamedStruct schema() {
        return NamedStruct.newBuilder().addNames("id").addNames("score")
                .addAllNames(RetrievalCandidateIdentity.PHYSICAL_FIELDS)
                .setStruct(Type.Struct.newBuilder().addTypes(integer()).addTypes(Type.newBuilder()
                        .setFp64(Type.FP64.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(string()).addTypes(string()).addTypes(string()).addTypes(integer())).build();
    }

    private static Type string() {
        return Type.newBuilder().setString(Type.String.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static Type integer() {
        return Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static Rel read(NamedStruct schema) {
        return Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("items"))).build();
    }

    private static Rel visible(Rel input, int count) {
        var projection = ProjectRel.newBuilder().setInput(input);
        int fields = SubstraitRelationLayout.describe(input).outputSchema().getStruct().getTypesCount();
        for (int index = 0; index < count; index++) {
            projection.addExpressions(SubstraitRelationLayout.rootFieldReference(index));
            projection.getCommonBuilder().getEmitBuilder().addOutputMapping(fields + index);
        }
        return Rel.newBuilder().setProject(projection).build();
    }

    private static Plan topK(NamedStruct schema) {
        Rel sort = Rel.newBuilder().setSort(io.substrait.proto.SortRel.newBuilder().setInput(read(schema))
                .addSorts(SortField.newBuilder().setExpr(SubstraitRelationLayout.rootFieldReference(1))
                        .setDirection(SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST))).build();
        Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel.newBuilder().setInput(sort)
                .setOffsetExpr(SubstraitFetchBounds.literal(0)).setCountExpr(SubstraitFetchBounds.literal(3))).build();
        return rooted(fetch, schema.getNamesList());
    }

    private static Plan rooted(Rel input, List<String> names) {
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(input).addAllNames(names))).build();
    }
}
