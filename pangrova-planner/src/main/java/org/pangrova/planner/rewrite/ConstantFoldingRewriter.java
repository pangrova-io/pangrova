package org.pangrova.planner.rewrite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContexts;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Folds constant expressions (no TableScan/TableModify/Exchange) into
 * LogicalValues via Calcite Interpreter evaluation.
 */
public final class ConstantFoldingRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(ConstantFoldingRewriter.class);

    private final AtomicLong foldedTotal = new AtomicLong();

    public long getFoldedTotal() {
        return foldedTotal.get();
    }

    /**
     * Attempts to fold the given expression into a constant when it is fully evaluable.
     *
     * @param relNode expression relation to fold
     * @param cluster cluster used for constant replacement
     * @return the folded relation, or the original when folding is not possible
     */
    public RelNode tryFold(RelNode relNode, RelOptCluster cluster) {
        if (!isConstantExpression(relNode)) {
            return relNode;
        }
        if (relNode instanceof LogicalValues && relNode.getInputs().isEmpty()) {
            foldedTotal.incrementAndGet();
            LOG.debug("Query already folded to constant: rows={}",
                    ((LogicalValues) relNode).getTuples().size());
            return relNode;
        }
        try {
            RelNode result = foldToLogicalValues(relNode, cluster);
            foldedTotal.incrementAndGet();
            LOG.debug("Query folded to constant: rows={}",
                    ((LogicalValues) result).getTuples().size());
            return result;
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
            LOG.debug("event=constant_folding_skipped reason=interpreter_unsupported detail={}", e.getMessage());
            return tryFoldFromProjectLiterals(relNode, cluster);
        }
    }

    /**
     * Fallback: when Interpreter cannot evaluate a LogicalProject(LogicalValues),
     * simplify project expressions via RexSimplify (handles CASE/COALESCE with
     * known-null branches), then check if all are RexLiteral.
     */
    private RelNode tryFoldFromProjectLiterals(RelNode relNode, RelOptCluster cluster) {
        if (!(relNode instanceof LogicalProject project)) {
            return relNode;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexSimplify simplify = new RexSimplify(rexBuilder,
                org.apache.calcite.plan.RelOptPredicateList.EMPTY, RexUtil.EXECUTOR);
        ImmutableList<RexLiteral> tuple = simplifyToLiterals(project.getProjects(), simplify);
        if (tuple == null) {
            return relNode;
        }
        foldedTotal.incrementAndGet();
        LOG.debug("event=fold_from_project_literals cols={}", tuple.size());
        return LogicalValues.create(cluster, relNode.getRowType(), ImmutableList.of(tuple));
    }

    /**
     * Simplifies each project expression and returns a literal tuple,
     * or null if any expression cannot be reduced to a literal.
     */
    private ImmutableList<RexLiteral> simplifyToLiterals(
            List<RexNode> projects, RexSimplify simplify) {
        ImmutableList.Builder<RexLiteral> tuple =
                ImmutableList.builderWithExpectedSize(projects.size());
        for (RexNode rex : projects) {
            RexNode simplified = (rex instanceof RexLiteral) ? rex : simplify.simplify(rex);
            if (!(simplified instanceof RexLiteral lit)) {
                LOG.debug("event=fold_fallback_skip reason=non_literal_after_simplify expr={}",
                        simplified);
                return null;
            }
            tuple.add(lit);
        }
        return tuple.build();
    }

    private boolean isConstantExpression(RelNode node) {
        if (node instanceof TableScan
                || node instanceof TableModify
                || node instanceof Exchange) {
            return false;
        }
        for (RelNode input : node.getInputs()) {
            if (!isConstantExpression(input)) {
                return false;
            }
        }
        return true;
    }

    private RelNode foldToLogicalValues(RelNode relNode, RelOptCluster cluster) {
        RelDataType rowType = relNode.getRowType();
        List<Object[]> rawRows = evaluateWithInterpreter(relNode);
        ImmutableList<ImmutableList<RexLiteral>> tuples = convertRowsToTuples(
                rawRows, rowType, cluster.getRexBuilder());
        return LogicalValues.create(cluster, rowType, tuples);
    }

    private List<Object[]> evaluateWithInterpreter(RelNode relNode) {
        List<Object[]> rows = new ArrayList<>();
        try (Interpreter interp = new Interpreter(DataContexts.EMPTY, relNode)) {
            for (Object[] values : interp) {
                rows.add(values);
            }
        }
        return rows;
    }

    private ImmutableList<ImmutableList<RexLiteral>> convertRowsToTuples(
            List<Object[]> rows, RelDataType rowType, RexBuilder rexBuilder) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples =
                ImmutableList.builderWithExpectedSize(rows.size());
        for (Object[] row : rows) {
            tuples.add(convertSingleRow(row, fields, rexBuilder));
        }
        return tuples.build();
    }

    private ImmutableList<RexLiteral> convertSingleRow(
            Object[] row, List<RelDataTypeField> fields, RexBuilder rexBuilder) {
        ImmutableList.Builder<RexLiteral> tuple =
                ImmutableList.builderWithExpectedSize(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Object value = (i < row.length) ? row[i] : null;
            RelDataType fieldType = fields.get(i).getType();
            tuple.add(toRexLiteral(value, fieldType, rexBuilder));
        }
        return tuple.build();
    }

    private RexLiteral toRexLiteral(Object value, RelDataType type, RexBuilder rexBuilder) {
        if (value == null) {
            return rexBuilder.makeNullLiteral(type);
        }
        Object converted = toMakeLiteralValue(value);
        return (RexLiteral) rexBuilder.makeLiteral(converted, type, false, false);
    }

    private static Object toMakeLiteralValue(Object value) {
        if (value instanceof NlsString nls) {
            return nls.getValue();
        }
        if (value instanceof Number num) {
            return new BigDecimal(num.toString());
        }
        return value;
    }
}
