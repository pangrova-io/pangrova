package org.pangrova.planner.cost;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Custom selectivity metadata provider that uses histogram-based estimation
 * from {@link StatisticsBasedCostEstimator} when available.
 * Falls back to Calcite's default selectivity for unsupported predicates.
 */
public final class PangrovaRelMdSelectivity
        implements MetadataHandler<BuiltInMetadata.Selectivity> {

    private static final Logger LOG = LoggerFactory.getLogger(PangrovaRelMdSelectivity.class);

    private final StatisticsBasedCostEstimator costEstimator;

    public PangrovaRelMdSelectivity(StatisticsBasedCostEstimator costEstimator) {
        this.costEstimator = costEstimator;
    }

    @Override
    public org.apache.calcite.rel.metadata.MetadataDef<BuiltInMetadata.Selectivity> getDef() {
        return BuiltInMetadata.Selectivity.DEF;
    }

    /** Creates a Calcite metadata provider backed by this selectivity estimator. */
    public RelMetadataProvider asProvider() {
        return ReflectiveRelMetadataProvider.reflectiveSource(this,
                BuiltInMethod.SELECTIVITY.method);
    }

    /** Selectivity for Filter nodes — extracts predicate and delegates to histogram. */
    public Double getSelectivity(Filter rel, RelMetadataQuery mq, RexNode predicate) {
        RexNode effectivePredicate = predicate != null ? predicate : rel.getCondition();
        Double result = estimateFromPredicate(rel.getInput(), effectivePredicate);
        if (result != null) {
            return result;
        }
        return RelMdUtil.guessSelectivity(effectivePredicate);
    }

    /** Selectivity for Join nodes — estimates join selectivity using histogram overlap. */
    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        RexNode effectivePredicate = predicate != null ? predicate : rel.getCondition();
        Double result = estimateJoinSelectivity(rel, effectivePredicate);
        if (result != null) {
            return result;
        }
        return RelMdUtil.guessSelectivity(effectivePredicate);
    }

    /** Selectivity for generic RelNode — falls back to Calcite default. */
    public Double getSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
        return RelMdUtil.guessSelectivity(predicate);
    }

    private Double estimateFromPredicate(RelNode input, RexNode predicate) {
        if (predicate == null || costEstimator == null) {
            return null;
        }
        if (!(predicate instanceof RexCall call)) {
            return null;
        }
        return estimateCallSelectivity(input, call);
    }

    private Double estimateCallSelectivity(RelNode input, RexCall call) {
        SqlKind kind = call.getKind();
        return switch (kind) {
            case EQUALS -> estimateEquality(input, call);
            case GREATER_THAN, GREATER_THAN_OR_EQUAL,
                 LESS_THAN, LESS_THAN_OR_EQUAL -> estimateRange(input, call);
            case IN -> estimateInList(input, call);
            default -> null;
        };
    }

    private Double estimateEquality(RelNode input, RexCall call) {
        ColumnRef ref = extractColumnRef(input, call);
        if (ref == null) {
            return null;
        }
        Comparable<?> value = extractLiteralValue(call);
        if (value != null) {
            return costEstimator.histogramEqualitySelectivity(ref.tableName, ref.columnName, value);
        }
        return costEstimator.estimateSelectivity(ref.tableName, ref.columnName);
    }

    private Double estimateRange(RelNode input, RexCall call) {
        ColumnRef ref = extractColumnRef(input, call);
        if (ref == null) {
            return null;
        }
        Comparable<?> literal = extractLiteralValue(call);
        if (literal == null) {
            return null;
        }
        SqlKind kind = call.getKind();
        Comparable<?> lower = isGreaterThan(kind) ? literal : null;
        Comparable<?> upper = isLessThan(kind) ? literal : null;
        return costEstimator.histogramRangeSelectivity(ref.tableName, ref.columnName, lower, upper);
    }

    private static boolean isGreaterThan(SqlKind kind) {
        return kind == SqlKind.GREATER_THAN || kind == SqlKind.GREATER_THAN_OR_EQUAL;
    }

    private static boolean isLessThan(SqlKind kind) {
        return kind == SqlKind.LESS_THAN || kind == SqlKind.LESS_THAN_OR_EQUAL;
    }

    private Double estimateInList(RelNode input, RexCall call) {
        ColumnRef ref = extractInListColumnRef(input, call);
        if (ref == null) {
            return null;
        }
        List<Comparable<?>> values = extractInListValues(call);
        if (values.isEmpty()) {
            return null;
        }
        return costEstimator.histogramInListSelectivity(ref.tableName, ref.columnName, values);
    }

    private static ColumnRef extractInListColumnRef(RelNode input, RexCall call) {
        if (call.getOperands().isEmpty()) {
            return null;
        }
        RexNode firstOp = call.getOperands().get(0);
        if (!(firstOp instanceof RexInputRef inputRef)) {
            return null;
        }
        String tableName = resolveTableName(input);
        String columnName = resolveColumnName(input, inputRef.getIndex());
        if (tableName == null || columnName == null) {
            return null;
        }
        return new ColumnRef(tableName, columnName);
    }

    private static List<Comparable<?>> extractInListValues(RexCall call) {
        List<Comparable<?>> values = new ArrayList<>();
        for (int i = 1; i < call.getOperands().size(); i++) {
            RexNode op = call.getOperands().get(i);
            if (op instanceof RexLiteral lit) {
                Comparable<?> v = lit.getValueAs(Comparable.class);
                if (v != null) {
                    values.add(v);
                }
            }
        }
        return values;
    }

    private Double estimateJoinSelectivity(Join join, RexNode condition) {
        if (costEstimator == null || condition == null) {
            return null;
        }
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
        double selectivity = 1.0;
        boolean anyEstimated = false;
        for (RexNode conj : conjunctions) {
            if (conj instanceof RexCall call && call.getKind() == SqlKind.EQUALS) {
                Double est = estimateJoinEquality(join, call);
                if (est != null) {
                    selectivity *= est;
                    anyEstimated = true;
                }
            }
        }
        return anyEstimated ? selectivity : null;
    }

    private Double estimateJoinEquality(Join join, RexCall equiCall) {
        if (equiCall.getOperands().size() != 2) {
            return null;
        }
        RexNode left = equiCall.getOperands().get(0);
        RexNode right = equiCall.getOperands().get(1);
        if (!(left instanceof RexInputRef leftRef) || !(right instanceof RexInputRef rightRef)) {
            return null;
        }
        String leftTable = resolveTableName(join.getLeft());
        String rightTable = resolveTableName(join.getRight());
        String leftCol = resolveColumnName(join.getLeft(), leftRef.getIndex());
        int rightFieldCount = join.getLeft().getRowType().getFieldCount();
        String rightCol = resolveColumnName(join.getRight(), rightRef.getIndex() - rightFieldCount);
        if (leftTable == null || rightTable == null || leftCol == null || rightCol == null) {
            return null;
        }
        double leftSel = costEstimator.estimateSelectivity(leftTable, leftCol);
        double rightSel = costEstimator.estimateSelectivity(rightTable, rightCol);
        return Math.max(leftSel, rightSel);
    }

    private ColumnRef extractColumnRef(RelNode input, RexCall call) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef ref) {
                String tableName = resolveTableName(input);
                String columnName = resolveColumnName(input, ref.getIndex());
                if (tableName != null && columnName != null) {
                    return new ColumnRef(tableName, columnName);
                }
            }
        }
        return null;
    }

    private static Comparable<?> extractLiteralValue(RexCall call) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexLiteral lit) {
                return lit.getValueAs(Comparable.class);
            }
        }
        return null;
    }

    private static String resolveTableName(RelNode node) {
        if (node instanceof TableScan scan) {
            List<String> names = scan.getTable().getQualifiedName();
            return String.join(".", names);
        }
        if (!node.getInputs().isEmpty()) {
            return resolveTableName(node.getInput(0));
        }
        return null;
    }

    private static String resolveColumnName(RelNode node, int index) {
        List<String> fieldNames = node.getRowType().getFieldNames();
        if (index >= 0 && index < fieldNames.size()) {
            return fieldNames.get(index);
        }
        return null;
    }

    private record ColumnRef(String tableName, String columnName) {}
}
