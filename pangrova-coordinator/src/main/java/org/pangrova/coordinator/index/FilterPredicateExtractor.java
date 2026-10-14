package org.pangrova.coordinator.index;

import org.pangrova.spi.index.predicate.ContainmentMode;
import org.pangrova.spi.index.predicate.ContainsElement;
import org.pangrova.spi.index.predicate.EqualTo;
import org.pangrova.spi.index.predicate.IndexPredicate;
import org.pangrova.spi.index.predicate.InList;
import org.pangrova.spi.index.predicate.IsNotNull;
import org.pangrova.spi.index.predicate.IsNull;
import org.pangrova.spi.index.predicate.LikePattern;
import org.pangrova.spi.index.predicate.RangeQuery;
import org.pangrova.spi.index.predicate.SpatialOp;
import org.pangrova.spi.index.predicate.SpatialQuery;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts {@link IndexPredicate} instances from Calcite filter conditions
 * in a RelNode tree. Used by the coordinator to generate index hints for
 * split filtering without coupling the planner to metadata dependencies.
 *
 * <p>Supports: EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN,
 * GREATER_THAN_OR_EQUAL, IS_NULL, IS_NOT_NULL, IN, LIKE,
 * spatial functions (ST_INTERSECTS, ST_WITHIN, ST_CONTAINS),
 * array functions (ARRAY_CONTAINS, ARRAY_CONTAINS_ALL),
 * and AND conjunctions.</p>
 */
public final class FilterPredicateExtractor {

    private FilterPredicateExtractor() {}

    /**
     * Extracts all index predicates from filter conditions in the RelNode tree.
     *
     * @param relNode the optimized query plan
     * @return list of extracted predicates (may be empty, never null)
     */
    public static List<IndexPredicate> extract(RelNode relNode) {
        List<IndexPredicate> predicates = new ArrayList<>();
        collectPredicates(relNode, predicates);
        return List.copyOf(predicates);
    }

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(FilterPredicateExtractor.class);

    private static void collectPredicates(RelNode node, List<IndexPredicate> out) {
        if (node instanceof Filter filter) {
            RelNode scan = findTableScan(filter.getInput());
            if (scan != null) {
                RexNode cond = filter.getCondition();
                LOG.debug("event=predicate_extract conditionKind={}",
                        cond instanceof org.apache.calcite.rex.RexCall rc ? rc.getKind() : "N/A");
                extractFromCondition(cond, filter.getInput(), out);
                return;
            }
        }
        for (RelNode input : node.getInputs()) {
            collectPredicates(input, out);
        }
    }

    private static TableScan findTableScan(RelNode node) {
        if (node instanceof TableScan ts) {
            return ts;
        }
        for (RelNode input : node.getInputs()) {
            TableScan found = findTableScan(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static void extractFromCondition(
            RexNode condition, RelNode scan, List<IndexPredicate> out) {
        if (!(condition instanceof RexCall call)) {
            return;
        }
        if (call.getKind() == SqlKind.SEARCH) {
            RexNode expanded = org.apache.calcite.rex.RexUtil.expandSearch(
                    scan.getCluster().getRexBuilder(), null, call);
            extractFromCondition(expanded, scan, out);
            return;
        }
        if (call.getKind() == SqlKind.AND) {
            for (RexNode operand : call.getOperands()) {
                extractFromCondition(operand, scan, out);
            }
            return;
        }
        if (call.getKind() == SqlKind.OR) {
            extractOrPredicate(call, scan, out);
            return;
        }
        extractSinglePredicate(call, scan, out);
    }

    /**
     * Attempts to merge OR branches into a single InList when all branches
     * are equality predicates on the same column. Cross-column OR or
     * non-equality branches are conservatively skipped (no index evaluation).
     */
    private static void extractOrPredicate(
            RexCall orCall, RelNode scan, List<IndexPredicate> out) {
        String commonColumn = null;
        List<Comparable<?>> values = new ArrayList<>();

        for (RexNode operand : orCall.getOperands()) {
            OrBranchResult branch = parseOrBranch(operand, scan);
            if (branch == null) {
                return;
            }
            if (commonColumn == null) {
                commonColumn = branch.column;
            } else if (!commonColumn.equals(branch.column)) {
                return;
            }
            values.add(branch.value);
        }
        if (commonColumn != null && !values.isEmpty()) {
            out.add(new InList(commonColumn, values));
        }
    }

    private record OrBranchResult(String column, Comparable<?> value) {}

    private static OrBranchResult parseOrBranch(RexNode operand, RelNode scan) {
        if (!(operand instanceof RexCall branch)) {
            return null;
        }
        if (branch.getKind() != SqlKind.EQUALS || branch.getOperands().size() != 2) {
            return null;
        }
        return resolveEqualityBranch(branch, scan);
    }

    private static OrBranchResult resolveEqualityBranch(RexCall branch, RelNode scan) {
        RexNode left = branch.getOperands().get(0);
        RexNode right = branch.getOperands().get(1);
        RexInputRef ref = resolveInputRef(left, right);
        RexLiteral literal = resolveLiteral(left, right);
        if (ref == null || literal == null) {
            return null;
        }
        String column = scan.getRowType().getFieldList().get(ref.getIndex()).getName();
        Comparable<?> value = coerceLiteral(literal, ref);
        return value != null ? new OrBranchResult(column, value) : null;
    }

    private static RexInputRef resolveInputRef(RexNode left, RexNode right) {
        if (left instanceof RexInputRef r) {
            return r;
        }
        if (right instanceof RexInputRef r) {
            return r;
        }
        return null;
    }

    private static RexLiteral resolveLiteral(RexNode left, RexNode right) {
        if (right instanceof RexLiteral l && left instanceof RexInputRef) {
            return l;
        }
        if (left instanceof RexLiteral l && right instanceof RexInputRef) {
            return l;
        }
        return null;
    }

    private static void extractSinglePredicate(
            RexCall call, RelNode scan, List<IndexPredicate> out) {
        SqlKind kind = call.getKind();

        if (tryExtractNullCheck(call, kind, scan, out)) {
            return;
        }
        if (tryExtractSpecialPredicate(call, kind, scan, out)) {
            return;
        }
        if (call.getOperands().size() == 2) {
            extractComparison(call, scan, out);
        }
    }

    private static boolean tryExtractNullCheck(RexCall call, SqlKind kind,
                                               RelNode scan, List<IndexPredicate> out) {
        if (kind == SqlKind.IS_NULL && call.getOperands().size() == 1) {
            extractNullPredicate(call.getOperands().get(0), scan, out, true);
            return true;
        }
        if (kind == SqlKind.IS_NOT_NULL && call.getOperands().size() == 1) {
            extractNullPredicate(call.getOperands().get(0), scan, out, false);
            return true;
        }
        return false;
    }

    private static boolean tryExtractSpecialPredicate(RexCall call, SqlKind kind,
                                                      RelNode scan, List<IndexPredicate> out) {
        if (kind == SqlKind.IN && call.getOperands().size() >= 2) {
            extractInPredicate(call, scan, out);
            return true;
        }
        if (kind == SqlKind.LIKE && call.getOperands().size() == 2) {
            extractLikePredicate(call, scan, out);
            return true;
        }
        if (kind == SqlKind.OTHER_FUNCTION) {
            extractFunctionPredicate(call, scan, out);
            return true;
        }
        return false;
    }

    private static void extractNullPredicate(
            RexNode operand, RelNode scan, List<IndexPredicate> out, boolean isNull) {
        if (!(operand instanceof RexInputRef ref)) {
            return;
        }
        String column = scan.getRowType().getFieldList().get(ref.getIndex()).getName();
        out.add(isNull ? new IsNull(column) : new IsNotNull(column));
    }

    private static void extractInPredicate(
            RexCall call, RelNode scan, List<IndexPredicate> out) {
        RexNode first = call.getOperands().get(0);
        if (!(first instanceof RexInputRef ref)) {
            return;
        }
        String column = scan.getRowType().getFieldList().get(ref.getIndex()).getName();
        List<Comparable<?>> values = new ArrayList<>();
        for (int i = 1; i < call.getOperands().size(); i++) {
            if (call.getOperands().get(i) instanceof RexLiteral lit) {
                Comparable<?> v = coerceLiteral(lit, ref);
                if (v != null) {
                    values.add(v);
                }
            }
        }
        if (!values.isEmpty()) {
            out.add(new InList(column, values));
        }
    }

    private static void extractLikePredicate(
            RexCall call, RelNode scan, List<IndexPredicate> out) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (!(left instanceof RexInputRef ref)) return;
        if (!(right instanceof RexLiteral literal)) return;
        Object raw = literal.getValue();
        if (!(raw instanceof org.apache.calcite.util.NlsString nls)) return;
        String column = scan.getRowType().getFieldList()
                .get(ref.getIndex()).getName();
        out.add(new LikePattern(column, nls.getValue()));
    }

    private static void extractFunctionPredicate(
            RexCall call, RelNode scan, List<IndexPredicate> out) {
        SqlOperator operator = call.getOperator();
        String name = operator.getName().toLowerCase(java.util.Locale.ROOT);
        switch (name) {
            case "st_intersects", "st_within", "st_contains" ->
                    extractSpatialCoordinates(call, scan, out, name);
            case "array_contains", "array_contains_all" ->
                    extractArrayElements(call, scan, out, name);
            default -> { /* unsupported function — skip silently */ }
        }
    }

    private static void extractSpatialCoordinates(
            RexCall call, RelNode scan, List<IndexPredicate> out, String name) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 5 || !(operands.get(0) instanceof RexInputRef ref)) {
            return;
        }
        double[] coords = parseSpatialCoords(operands);
        if (coords == null) {
            return;
        }
        String column = scan.getRowType().getFieldList().get(ref.getIndex()).getName();
        SpatialOp op = toSpatialOp(name);
        if (op != null) {
            out.add(new SpatialQuery(column, coords[0], coords[1], coords[2], coords[3], op));
        }
    }

    private static double[] parseSpatialCoords(List<RexNode> operands) {
        double[] coords = new double[4];
        for (int i = 0; i < 4; i++) {
            if (!(operands.get(i + 1) instanceof RexLiteral lit)) {
                return null;
            }
            Object raw = lit.getValue();
            if (!(raw instanceof Number num)) {
                return null;
            }
            coords[i] = num.doubleValue();
        }
        return coords;
    }

    private static SpatialOp toSpatialOp(String name) {
        return switch (name) {
            case "st_intersects" -> SpatialOp.INTERSECTS;
            case "st_within" -> SpatialOp.WITHIN;
            case "st_contains" -> SpatialOp.CONTAINS;
            default -> null;
        };
    }

    private static void extractArrayElements(
            RexCall call, RelNode scan, List<IndexPredicate> out, String name) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() < 2 || !(operands.get(0) instanceof RexInputRef ref)) {
            return;
        }
        List<Comparable<?>> elements = parseArrayLiterals(operands);
        if (elements == null || elements.isEmpty()) {
            return;
        }
        String column = scan.getRowType().getFieldList().get(ref.getIndex()).getName();
        ContainmentMode mode = "array_contains_all".equals(name)
                ? ContainmentMode.CONTAINS_ALL
                : ContainmentMode.CONTAINS_ANY;
        out.add(new ContainsElement(column, elements, mode));
    }

    private static List<Comparable<?>> parseArrayLiterals(List<RexNode> operands) {
        List<Comparable<?>> elements = new ArrayList<>();
        for (int i = 1; i < operands.size(); i++) {
            if (!(operands.get(i) instanceof RexLiteral lit)) {
                return null;
            }
            Comparable<?> element = coerceArrayElement(lit.getValue());
            if (element == null) {
                return null;
            }
            elements.add(element);
        }
        return elements;
    }

    private static Comparable<?> coerceArrayElement(Object raw) {
        if (raw instanceof org.apache.calcite.util.NlsString nls) {
            return nls.getValue();
        }
        if (raw instanceof Comparable<?> cmp) {
            return cmp;
        }
        return null;
    }

    private static void extractComparison(
            RexCall call, RelNode scan, List<IndexPredicate> out) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        RexInputRef ref = resolveInputRef(left, right);
        RexLiteral literal = resolveLiteral(left, right);
        if (ref == null || literal == null) {
            return;
        }
        boolean swapped = left instanceof RexLiteral;
        Comparable<?> value = coerceLiteral(literal, ref);
        if (value == null) {
            return;
        }
        String column = scan.getRowType().getFieldList().get(ref.getIndex()).getName();
        IndexPredicate predicate = toIndexPredicate(call.getKind(), column, value, swapped);
        if (predicate != null) {
            out.add(predicate);
        }
    }

    private static IndexPredicate toIndexPredicate(
            SqlKind kind, String column, Comparable<?> value, boolean swapped) {
        return switch (kind) {
            case EQUALS -> new EqualTo(column, value);
            case LESS_THAN -> swapped
                    ? new RangeQuery(column, value, null, false, false)
                    : new RangeQuery(column, null, value, false, false);
            case LESS_THAN_OR_EQUAL -> swapped
                    ? new RangeQuery(column, value, null, true, false)
                    : new RangeQuery(column, null, value, false, true);
            case GREATER_THAN -> swapped
                    ? new RangeQuery(column, null, value, false, false)
                    : new RangeQuery(column, value, null, false, false);
            case GREATER_THAN_OR_EQUAL -> swapped
                    ? new RangeQuery(column, null, value, false, true)
                    : new RangeQuery(column, value, null, true, false);
            default -> null;
        };
    }

    /**
     * Coerces a Calcite literal to match the column's SQL type.
     * Calcite often represents integer literals as DECIMAL(10,0).
     */
    private static Comparable<?> coerceLiteral(RexLiteral literal, RexInputRef ref) {
        Object raw = literal.getValue();
        if (raw == null) {
            return null;
        }
        // Calcite's NlsString.toString() renders a SQL literal (including
        // charset and quotes), not the value stored in the data file. Using
        // that rendering as an index key can falsely prune matching files.
        if (raw instanceof org.apache.calcite.util.NlsString nls) {
            return nls.getValue();
        }
        SqlTypeName columnType = ref.getType().getSqlTypeName();
        if (raw instanceof java.math.BigDecimal bd) {
            return switch (columnType) {
                case DOUBLE, FLOAT, REAL -> bd.doubleValue();
                case BIGINT -> bd.longValue();
                case INTEGER -> bd.intValue();
                case SMALLINT -> bd.shortValue();
                case TINYINT -> bd.byteValue();
                default -> (Comparable<?>) raw;
            };
        }
        return (Comparable<?>) raw;
    }
}
