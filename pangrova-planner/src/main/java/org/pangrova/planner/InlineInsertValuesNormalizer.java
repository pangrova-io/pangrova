package org.pangrova.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

/** Packs literal INSERT branches without evaluating expressions outside the query session. */
final class InlineInsertValuesNormalizer {
    private InlineInsertValuesNormalizer() {}

    static RelNode normalize(RelNode relation) {
        if (!(relation instanceof LogicalTableModify modify)
                || modify.getOperation() != TableModify.Operation.INSERT
                || !(modify.getInput() instanceof LogicalUnion union)
                || !union.all || union.getInputs().size() < 2) {
            return relation;
        }
        int width = union.getRowType().getFieldCount();
        if (width == 0) return relation;
        RelDataTypeFactory typeFactory = relation.getCluster().getTypeFactory();
        var tuples = ImmutableList.<ImmutableList<RexLiteral>>builder();
        for (RelNode branch : union.getInputs()) {
            if (!(branch instanceof LogicalProject project)
                    || !(project.getInput() instanceof LogicalValues seed)
                    || seed.getTuples().size() != 1
                    || project.getProjects().size() != width) {
                return relation;
            }
            var fields = ImmutableList.<RexLiteral>builder();
            for (int column = 0; column < width; column++) {
                RexNode value = unwrapNullabilityCast(project.getProjects().get(column), typeFactory);
                if (!(value instanceof RexLiteral literal)
                        || !SqlTypeUtil.equalSansNullability(typeFactory, literal.getType(),
                                union.getRowType().getFieldList().get(column).getType())) {
                    return packConstantExpressions(modify, union);
                }
                fields.add(literal);
            }
            tuples.add(fields.build());
        }
        var values = LogicalValues.create(relation.getCluster(), union.getRowType(), tuples.build());
        return modify.copy(modify.getTraitSet(), List.of(values));
    }

    private static RelNode packConstantExpressions(LogicalTableModify modify, LogicalUnion union) {
        RexBuilder rex = modify.getCluster().getRexBuilder();
        List<RelDataType> slotTypes = new java.util.ArrayList<>();
        List<RexNode> template = null;
        var tuples = ImmutableList.<ImmutableList<RexLiteral>>builder();
        for (RelNode branch : union.getInputs()) {
            if (!(branch instanceof LogicalProject project)
                    || !(project.getInput() instanceof LogicalValues seed)
                    || seed.getTuples().size() != 1
                    || project.getProjects().size() != union.getRowType().getFieldCount()) {
                return modify;
            }
            List<RexLiteral> literals = new java.util.ArrayList<>();
            List<RexNode> expressions = new java.util.ArrayList<>();
            for (RexNode expression : project.getProjects()) {
                RexNode rewritten = liftConstantLiterals(expression, rex, literals);
                if (rewritten == null) return modify;
                expressions.add(rewritten);
            }
            if (template == null) {
                template = List.copyOf(expressions);
                slotTypes.addAll(literals.stream().map(RexLiteral::getType).toList());
            } else if (!template.equals(expressions) || literals.size() != slotTypes.size()) {
                return modify;
            }
            for (int index = 0; index < literals.size(); index++) {
                if (!slotTypes.get(index).equals(literals.get(index).getType())) return modify;
            }
            tuples.add(ImmutableList.copyOf(literals));
        }
        if (template == null || slotTypes.isEmpty()) return modify;
        var row = modify.getCluster().getTypeFactory().builder();
        for (int index = 0; index < slotTypes.size(); index++) {
            row.add("$literal" + index, slotTypes.get(index));
        }
        var values = LogicalValues.create(modify.getCluster(), row.build(), tuples.build());
        var project = LogicalProject.create(values, List.of(), template,
                union.getRowType().getFieldNames());
        for (int column = 0; column < union.getRowType().getFieldCount(); column++) {
            if (!SqlTypeUtil.equalSansNullability(modify.getCluster().getTypeFactory(),
                    project.getRowType().getFieldList().get(column).getType(),
                    union.getRowType().getFieldList().get(column).getType())) {
                return modify;
            }
        }
        return modify.copy(modify.getTraitSet(), List.of(project));
    }

    private static RexNode liftConstantLiterals(
            RexNode expression, RexBuilder rex, List<RexLiteral> literals) {
        if (expression instanceof RexLiteral literal) {
            int ordinal = literals.size();
            literals.add(literal);
            return new RexInputRef(ordinal, literal.getType());
        }
        if (!(expression instanceof RexCall call)
                || (call.getKind() != SqlKind.CAST
                        && call.getKind() != SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
            return null;
        }
        List<RexNode> operands = new java.util.ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            RexNode rewritten = liftConstantLiterals(operand, rex, literals);
            if (rewritten == null) return null;
            operands.add(rewritten);
        }
        return rex.makeCall(call.getType(), call.getOperator(), operands);
    }

    private static RexNode unwrapNullabilityCast(RexNode expression, RelDataTypeFactory typeFactory) {
        if (!(expression instanceof RexCall cast) || cast.getKind() != SqlKind.CAST
                || cast.getOperands().size() != 1) return expression;
        RexNode operand = unwrapNullabilityCast(cast.getOperands().get(0), typeFactory);
        if (operand instanceof RexLiteral
                && SqlTypeUtil.equalSansNullability(typeFactory, operand.getType(), cast.getType())) {
            return operand;
        }
        return expression;
    }
}
