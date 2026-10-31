/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.List;
import java.util.Map;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.RetrievalTopKPlanHint;
import org.pangrova.planner.SerializedPlanFragment;
import org.pangrova.coordinator.query.execution.dispatch.dag.retrieval.RetrievalPlanBinder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.ScoreDirection;

import static org.assertj.core.api.Assertions.assertThat;

class TypedCapabilityPlanBindingTest {

    @Test
    void coordinatorBindsMetadataWithoutMaterializingCandidates() throws Exception {
        NamedStruct schema = candidateSchema();
        SerializedPlanFragment fragment = new SerializedPlanFragment(
                plan(schema).toByteArray(),
                List.of(new RetrievalTopKPlanHint(
                        RetrievalTopKPlanHint.Kind.EXACT,
                        "",
                        1,
                        10,
                        RetrievalTopKPlanHint.ScoreDirection.LOWER_IS_BETTER,
                        List.of(0),
                        schema.toByteArray())));

        var bound = RetrievalPlanBinder.bindTypedCapabilities(
                fragment, "query-1", 7, Map.of());

        Rel root = Plan.parseFrom(bound.planBytes()).getRelations(0).getRoot().getInput();
        assertThat(root.hasExtensionSingle()).isTrue();
        assertThat(root.getExtensionSingle().getInput().hasRead()).isTrue();
        RelationDetailEnvelope envelope = root.getExtensionSingle().getDetail().unpack(
                RelationDetailEnvelope.class);
        assertThat(envelope.getRetrievalSource().getPlanNodeId())
                .isEqualTo("query-1-stage-7-topk-0");
        assertThat(envelope.getRetrievalSource().getInvocation().getCapability())
                .isEqualTo(BuiltInCapabilityCatalog.exactTopKPlanRef());
        assertThat(envelope.getRetrievalSource().getOutputContract().getScoreDirection())
                .isEqualTo(ScoreDirection.SCORE_DIRECTION_LOWER_IS_BETTER);
        assertThat(bound.operatorBindings()).hasSize(1);
        assertThat(bound.operatorBindings().get(0).getPlanNodeId())
                .isEqualTo("query-1-stage-7-topk-0");
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
                        .setDirection(
                                SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST)))
                .build();
        Rel fetch = Rel.newBuilder().setFetch(io.substrait.proto.FetchRel.newBuilder()
                .setInput(sort)
                .setCountExpr(Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setI64(10))))
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
                                io.substrait.proto.Type.I64.newBuilder().setNullability(
                                        io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(io.substrait.proto.Type.newBuilder().setFp64(
                                io.substrait.proto.Type.FP64.newBuilder().setNullability(
                                        io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED))))
                .build();
    }

    private static Expression fieldReference(int fieldIndex) {
        return Expression.newBuilder().setSelection(
                Expression.FieldReference.newBuilder().setDirectReference(
                        Expression.ReferenceSegment.newBuilder().setStructField(
                                Expression.ReferenceSegment.StructField.newBuilder()
                                        .setField(fieldIndex)))).build();
    }
}
