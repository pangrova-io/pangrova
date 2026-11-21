package org.pangrova.protocol.retrieval;

import java.util.List;

import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

/** Retains the final SQL-only ordinal projection while binding internal candidate operators. */
final class RetrievalOutputProjection {
    private RetrievalOutputProjection() {}

    static Scope peel(Plan original) {
        var root = original.getRelations(0).getRoot();
        if (!root.getInput().hasProject()) {
            return new Scope(original, original, null);
        }
        ProjectRel projection = root.getInput().getProject();
        var inputSchema = SubstraitRelationLayout.describe(projection.getInput()).outputSchema();
        int inputFields = inputSchema.getStruct().getTypesCount();
        // Only the planner's hidden-identity trim is a binding scope. Ordinary SQL
        // projections (including computed expressions) retain their original semantics.
        if (!RetrievalCandidateIdentity.hasPhysicalFields(inputSchema)
                || projection.getExpressionsCount() != inputFields - 4
                || root.getNamesCount() != projection.getExpressionsCount()) {
            return new Scope(original, original, null);
        }
        var mapping = projection.getCommon().getEmit().getOutputMappingList();
        if (!projection.getCommon().hasEmit() || mapping.size() != projection.getExpressionsCount()) {
            throw new IllegalArgumentException("Retrieval final projection must emit only visible ordinals");
        }
        for (int index = 0; index < projection.getExpressionsCount(); index++) {
            if (!projection.getExpressions(index).equals(SubstraitFieldReference.fieldReference(index))
                    || mapping.get(index) != inputFields + index) {
                throw new IllegalArgumentException("Retrieval final projection changed a visible ordinal");
            }
        }
        List<String> names = SubstraitRelationLayout.topLevelNames(
                SubstraitRelationLayout.describe(projection.getInput()).outputSchema());
        Plan inner = original.toBuilder().setRelations(0, original.getRelations(0).toBuilder()
                .setRoot(root.toBuilder().setInput(projection.getInput()).clearNames().addAllNames(names)))
                .build();
        return new Scope(original, inner, projection);
    }

    record Scope(Plan original, Plan inner, ProjectRel projection) {
        BoundExecutionPlan restore(BoundExecutionPlan bound) {
            if (projection == null) return bound;
            try {
                Plan plan = org.pangrova.protocol.substrait.SubstraitPlanCodec.parse(bound.planBytes());
                Rel visible = Rel.newBuilder().setProject(projection.toBuilder()
                        .setInput(plan.getRelations(0).getRoot().getInput())).build();
                Plan output = plan.toBuilder().setRelations(0, plan.getRelations(0).toBuilder()
                        .setRoot(original.getRelations(0).getRoot().toBuilder().setInput(visible))).build();
                return new BoundExecutionPlan(output.toByteArray(), bound.operatorBindings());
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Invalid bound retrieval output", failure);
            }
        }
    }
}
