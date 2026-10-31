package org.pangrova.testing.nativefixture;

import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.SetRel;
import io.substrait.proto.Type;

/** Generated canonical plans for real JNI task value and failure oracles. */
public final class ActualNativeTaskPlans {
    private ActualNativeTaskPlans() {}

    /** Each UNION ALL input is an actual ValuesNode batch, not a simulated poll. */
    public static byte[] values(int batches, int rowsPerBatch) {
        if (batches < 0 || rowsPerBatch < 0) {
            throw new IllegalArgumentException("Batch and row counts must be nonnegative");
        }
        var root = Plan.newBuilder().addRelationsBuilder().getRootBuilder().addNames("value");
        if (batches <= 1) {
            root.setInput(valuesInput(0, batches == 0 ? 0 : rowsPerBatch));
        } else {
            var union = root.getInputBuilder().getSetBuilder().setOp(SetRel.SetOp.SET_OP_UNION_ALL);
            for (int batch = 0; batch < batches; batch++) {
                union.addInputs(valuesInput(Math.multiplyExact(batch, rowsPerBatch), rowsPerBatch));
            }
        }
        return Plan.newBuilder().addRelations(io.substrait.proto.PlanRel.newBuilder().setRoot(root))
                .build().toByteArray();
    }

    private static Rel valuesInput(int first, int count) {
        var input = Rel.newBuilder();
        var read = input.getReadBuilder();
        read.getBaseSchemaBuilder().addNames("value").getStructBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypesBuilder().getI64Builder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        for (int row = 0; row < count; row++) {
            read.getVirtualTableBuilder().addExpressionsBuilder().addFieldsBuilder()
                    .getLiteralBuilder().setI64(Math.addExact(first, row));
        }
        return input.build();
    }

    /** A small canonical input expands at the actual Native CrossNode, not in Java. */
    public static byte[] largeCross(int sideRows) {
        if (sideRows <= 0) throw new IllegalArgumentException("Cross input must have positive rows");
        var plan = Plan.newBuilder();
        var cross = plan.addRelationsBuilder().getRootBuilder().addNames("value")
                .getInputBuilder().getCrossBuilder();
        cross.setLeft(valuesInput(0, sideRows)).setRight(valuesInput(0, sideRows));
        cross.getCommonBuilder().getEmitBuilder().addOutputMapping(0);
        return plan.build().toByteArray();
    }

    /** A legal typed CAST evaluated by the Native operator, which must fail. */
    public static byte[] failingCast() {
        return failingCast(0);
    }

    /** Includes actual successful source batches before the failing CAST source. */
    public static byte[] failingCast(int successfulSources) {
        if (successfulSources < 0) throw new IllegalArgumentException("Successful source count must be nonnegative");
        var plan = Plan.newBuilder();
        var project = plan.addRelationsBuilder().getRootBuilder().addNames("value")
                .getInputBuilder().getProjectBuilder();
        var read = project.getInputBuilder().getReadBuilder();
        read.getBaseSchemaBuilder().addNames("text").getStructBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)
                .addTypesBuilder().getStringBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        read.getVirtualTableBuilder().addExpressionsBuilder().addFieldsBuilder().getLiteralBuilder()
                .setString("not-an-integer");
        var cast = project.addExpressionsBuilder().getCastBuilder()
                .setFailureBehavior(Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION);
        cast.getTypeBuilder().getI64Builder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        var selection = cast.getInputBuilder().getSelectionBuilder();
        selection.getRootReferenceBuilder();
        selection.getDirectReferenceBuilder().getStructFieldBuilder().setField(0);
        project.getCommonBuilder().getEmitBuilder().addOutputMapping(1);
        if (successfulSources == 0) return plan.build().toByteArray();
        var combined = Plan.newBuilder();
        var union = combined.addRelationsBuilder().getRootBuilder().addNames("value")
                .getInputBuilder().getSetBuilder().setOp(SetRel.SetOp.SET_OP_UNION_ALL);
        for (int source = 0; source < successfulSources; source++) union.addInputs(valuesInput(source, 1));
        union.addInputs(plan.getRelations(0).getRoot().getInput());
        return combined.build().toByteArray();
    }
}
