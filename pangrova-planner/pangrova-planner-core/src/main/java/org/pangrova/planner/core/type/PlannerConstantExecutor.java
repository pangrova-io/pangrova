package org.pangrova.planner.core.type;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Constant folding preserves Native UUID ordering and conversion semantics. */
public final class PlannerConstantExecutor implements RexExecutor {
    public static final PlannerConstantExecutor INSTANCE = new PlannerConstantExecutor();

    private PlannerConstantExecutor() {}

    @Override
    public void reduce(RexBuilder builder, List<RexNode> expressions, List<RexNode> reduced) {
        var ordinary = new ArrayList<RexNode>();
        var positions = new ArrayList<Integer>();
        for (RexNode expression : expressions) {
            RexNode folded = expression.accept(new RexShuttle() {
                @Override public RexNode visitCall(RexCall call) {
                    return fold(builder, (RexCall) super.visitCall(call));
                }
            });
            int index = reduced.size();
            reduced.add(folded);
            if (!requiresNativeEvaluation(folded)) {
                ordinary.add(folded);
                positions.add(index);
            }
        }
        if (!ordinary.isEmpty()) {
            var values = new ArrayList<RexNode>();
            RexUtil.EXECUTOR.reduce(builder, ordinary, values);
            for (int index = 0; index < values.size(); ++index) reduced.set(positions.get(index), values.get(index));
        }
    }

    private static RexNode fold(RexBuilder builder, RexCall call) {
        if (!(call.getOperator() instanceof UuidOrderingSqlFunction ordering)
                || !(call.operands.get(0) instanceof RexLiteral left)
                || !(call.operands.get(1) instanceof RexLiteral right)) return call;
        if (left.isNull() || right.isNull()) return builder.makeNullLiteral(call.getType());
        if (left.getType().getSqlTypeName() != SqlTypeName.UUID
                || right.getType().getSqlTypeName() != SqlTypeName.UUID) return call;
        if (!(left.getValue() instanceof UUID a) || !(right.getValue() instanceof UUID b)) {
            throw new IllegalArgumentException("A non-null UUID literal must retain its UUID value");
        }
        int comparison = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
        if (comparison == 0) {
            comparison = Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
        }
        boolean value = switch (ordering.comparison()) {
            case LESS_THAN -> comparison < 0;
            case LESS_THAN_OR_EQUAL -> comparison <= 0;
            case GREATER_THAN -> comparison > 0;
            case GREATER_THAN_OR_EQUAL -> comparison >= 0;
            default -> throw new IllegalStateException("Unexpected UUID ordering operator");
        };
        return builder.makeLiteral(value);
    }

    private static boolean requiresNativeEvaluation(RexNode expression) {
        // Calcite's binaryToUuid accepts extra bytes, while Native requires exactly 16.
        // Keep engine-owned conversions intact, including when nested in another expression.
        boolean[] required = {false};
        expression.accept(new RexVisitorImpl<Void>(true) {
            @Override public Void visitCall(RexCall call) {
                if (call.getOperator() instanceof UuidOrderingSqlFunction
                        || (call.getKind() == SqlKind.CAST
                        && (call.getType().getSqlTypeName() == SqlTypeName.UUID
                        || call.operands.stream().anyMatch(
                                operand -> operand.getType().getSqlTypeName() == SqlTypeName.UUID)))) {
                    required[0] = true;
                }
                return super.visitCall(call);
            }
        });
        return required[0];
    }
}
