package org.pangrova.planner.cost;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Custom selectivity metadata provider that uses histogram-based estimation
 * from {@link StatisticsBasedCostEstimator} when available.
 * Falls back to Calcite's default selectivity for unsupported predicates.
 */
public final class PangrovaRelMdSelectivity
        implements MetadataHandler<BuiltInMetadata.Selectivity> {

    private static final RelMetadataProvider PROVIDER = ReflectiveRelMetadataProvider.reflectiveSource(
            new PangrovaRelMdSelectivity(), BuiltInMethod.SELECTIVITY.method);

    @Override
    public org.apache.calcite.rel.metadata.MetadataDef<BuiltInMetadata.Selectivity> getDef() {
        return BuiltInMetadata.Selectivity.DEF;
    }

    /** Uses the estimator owned by each relation's planning context. */
    public static RelMetadataProvider provider() {
        return PROVIDER;
    }

    private static StatisticsBasedCostEstimator estimator(RelNode rel) {
        return rel.getCluster().getPlanner().getContext().unwrap(StatisticsBasedCostEstimator.class);
    }

    /** Selectivity for Filter nodes — extracts predicate and delegates to histogram. */
    public Double getSelectivity(Filter rel, RelMetadataQuery mq, RexNode predicate) {
        RexNode effectivePredicate = predicate != null ? predicate : rel.getCondition();
        Double result = estimateFromPredicate(rel.getInput(), effectivePredicate, mq, estimator(rel));
        if (result != null) {
            return result;
        }
        return RelMdUtil.guessSelectivity(effectivePredicate);
    }

    /** Selectivity for Join nodes with equality keys resolved to their source columns. */
    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        RexNode effectivePredicate = predicate != null ? predicate : rel.getCondition();
        Double result = estimateJoinSelectivity(rel, effectivePredicate, mq, estimator(rel));
        if (result != null) {
            return result;
        }
        return RelMdUtil.guessSelectivity(effectivePredicate);
    }

    /** Selectivity for generic RelNode — falls back to Calcite default. */
    public Double getSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
        return RelMdUtil.guessSelectivity(predicate);
    }

    private static Double estimateFromPredicate(RelNode input, RexNode predicate, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        if (predicate == null || costEstimator == null) {
            return null;
        }
        if (!(predicate instanceof RexCall call)) {
            return null;
        }
        return estimateCallSelectivity(input, call, mq, costEstimator);
    }

    private static Double estimateCallSelectivity(RelNode input, RexCall call, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        SqlKind kind = call.getKind();
        return switch (kind) {
            case EQUALS -> estimateEquality(input, call, mq, costEstimator);
            case GREATER_THAN, GREATER_THAN_OR_EQUAL,
                 LESS_THAN, LESS_THAN_OR_EQUAL -> estimateRange(input, call, mq, costEstimator);
            case IN -> estimateInList(input, call, mq, costEstimator);
            default -> null;
        };
    }

    private static Double estimateEquality(RelNode input, RexCall call, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        ColumnRef ref = extractColumnRef(input, call, mq);
        if (ref == null) {
            return null;
        }
        Comparable<?> value = extractLiteralValue(call);
        if (value != null) {
            return costEstimator.histogramEqualitySelectivity(ref.tableName, ref.columnName, value);
        }
        return costEstimator.estimateSelectivity(ref.tableName, ref.columnName);
    }

    private static Double estimateRange(RelNode input, RexCall call, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        ColumnRef ref = extractColumnRef(input, call, mq);
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

    private static Double estimateInList(RelNode input, RexCall call, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        ColumnRef ref = extractInListColumnRef(input, call, mq);
        if (ref == null) {
            return null;
        }
        List<Comparable<?>> values = extractInListValues(call);
        if (values.isEmpty()) {
            return null;
        }
        return costEstimator.histogramInListSelectivity(ref.tableName, ref.columnName, values);
    }

    private static ColumnRef extractInListColumnRef(RelNode input, RexCall call, RelMetadataQuery mq) {
        if (call.getOperands().isEmpty()) {
            return null;
        }
        RexNode firstOp = call.getOperands().get(0);
        if (!(firstOp instanceof RexInputRef inputRef)) {
            return null;
        }
        return resolveColumn(input, inputRef.getIndex(), mq);
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

    private static Double estimateJoinSelectivity(Join join, RexNode condition, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        if (costEstimator == null || condition == null || join.getJoinType() != JoinRelType.INNER
                || !join.getSystemFieldList().isEmpty()) {
            return null;
        }
        List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);
        Double selectivity = null;
        for (RexNode conj : conjunctions) {
            if (conj instanceof RexCall call && call.getKind() == SqlKind.EQUALS) {
                Double est = estimateJoinEquality(join, call, mq, costEstimator);
                if (est != null) {
                    // Without joint statistics, the most selective key gives a conservative estimate.
                    selectivity = selectivity == null ? est : Math.min(selectivity, est);
                }
            }
        }
        return selectivity;
    }

    private static Double estimateJoinEquality(Join join, RexCall equiCall, RelMetadataQuery mq,
            StatisticsBasedCostEstimator costEstimator) {
        if (equiCall.getOperands().size() != 2) {
            return null;
        }
        RexNode left = equiCall.getOperands().get(0);
        RexNode right = equiCall.getOperands().get(1);
        if (!(left instanceof RexInputRef first) || !(right instanceof RexInputRef second)) {
            return null;
        }
        int leftFields = join.getLeft().getRowType().getFieldCount();
        int leftIndex = Math.min(first.getIndex(), second.getIndex());
        int rightIndex = Math.max(first.getIndex(), second.getIndex());
        if (leftIndex >= leftFields || rightIndex < leftFields) {
            return null;
        }
        ColumnRef leftCol = resolveColumn(join.getLeft(), leftIndex, mq);
        ColumnRef rightCol = resolveColumn(join.getRight(), rightIndex - leftFields, mq);
        if (leftCol == null || rightCol == null) {
            return null;
        }
        return costEstimator.estimateJoinEquality(leftCol.tableName, leftCol.columnName,
                rightCol.tableName, rightCol.columnName);
    }

    private static ColumnRef extractColumnRef(RelNode input, RexCall call, RelMetadataQuery mq) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef ref) {
                ColumnRef column = resolveColumn(input, ref.getIndex(), mq);
                if (column != null) {
                    return column;
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

    private static ColumnRef resolveColumn(RelNode input, int index, RelMetadataQuery mq) {
        if (index < 0 || index >= input.getRowType().getFieldCount()) {
            return null;
        }
        var origin = mq.getColumnOrigin(input, index);
        if (origin == null || origin.isDerived()) {
            return null;
        }
        var table = origin.getOriginTable();
        List<String> names = table.getQualifiedName();
        if (names.size() != 3) {
            return null;
        }
        return new ColumnRef(new QualifiedTableName(names.get(0), names.get(1), names.get(2)),
                table.getRowType().getFieldNames().get(origin.getOriginColumnOrdinal()));
    }

    private record ColumnRef(QualifiedTableName tableName, String columnName) {}
}
