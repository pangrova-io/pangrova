package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.error.PlannerException;
import org.pangrova.planner.core.metadata.PlanTable;

import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.Domain;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts {@link SplitSourceDescriptor}s from a Calcite {@link RelNode} plan tree.
 *
 * <p>Encapsulates ALL Calcite tree-walking and predicate-extraction logic so that
 * downstream consumers (SplitFetcher, IndexAwareSplitFetcher) can work with the
 * Calcite-free {@link SplitSourceDescriptor} representation.</p>
 *
 * <p>This class is planner-internal: it lives in {@code pangrova-planner} where
 * Calcite is an expected dependency.</p>
 */
public final class PlanSplitSourceExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(PlanSplitSourceExtractor.class);

    private PlanSplitSourceExtractor() {}

    /**
     * Extracts deduplicated split source descriptors from a RelNode plan.
     *
     * @param relNode the Calcite plan tree
     * @return deduplicated table scan descriptors with extracted constraints
     */
    public static List<SplitSourceDescriptor> extract(RelNode relNode) {
        List<TableScanRef> refs = new ArrayList<>();
        collectTableScans(relNode, refs);
        if (refs.isEmpty()) {
            return List.of();
        }
        Map<List<String>, TableScanRef> unique = deduplicateTableRefs(refs);
        List<SplitSourceDescriptor> descriptors = new ArrayList<>(unique.size());
        for (TableScanRef ref : unique.values()) {
            Constraint constraint = buildConstraint(ref);
            List<String> columns = ref.scan().getRowType().getFieldList().stream()
                    .map(f -> f.getName())
                    .filter(name -> !org.pangrova.protocol.retrieval.RetrievalCandidateIdentity
                            .PHYSICAL_FIELDS.contains(name))
                    .toList();
            descriptors.add(new SplitSourceDescriptor(
                    ref.catalogName(), ref.schemaName(), ref.tableName(),
                    constraint, columns, java.util.Optional.ofNullable(PlanTable.require(ref.scan())
                            .unwrap(org.pangrova.connector.api.metadata.table.ConnectorReadBinding.class))));
        }
        return descriptors;
    }

    /**
     * Returns the output column names from the top-level RelNode.
     */
    public static List<String> queryOutputColumns(RelNode relNode) {
        return relNode.getRowType().getFieldNames();
    }

    // --- Internal Calcite tree-walking ---

    private static void collectTableScans(RelNode node, List<TableScanRef> refs) {
        if (node instanceof Filter filter && filter.getInput() instanceof TableScan scan) {
            List<String> qn = PlanTable.require(scan).getQualifiedName();
            refs.add(TableScanRef.fromQualifiedName(qn, scan, filter.getCondition()));
            return;
        }
        if (node instanceof TableScan scan) {
            List<String> qn = PlanTable.require(scan).getQualifiedName();
            refs.add(TableScanRef.fromQualifiedName(qn, scan, null));
        }
        for (RelNode input : node.getInputs()) {
            collectTableScans(input, refs);
        }
    }

    private static Map<List<String>, TableScanRef> deduplicateTableRefs(List<TableScanRef> refs) {
        LinkedHashMap<List<String>, TableScanRef> unique = new LinkedHashMap<>();
        for (TableScanRef ref : refs) {
            var key = List.of(ref.catalogName(), ref.schemaName(), ref.tableName());
            var previous = unique.get(key);
            if (previous != null) requireSameReadSource(previous, ref);
            if (!unique.containsKey(key)) {
                unique.put(key, ref);
            } else if (unique.get(key).filterCondition() != null) {
                TableScanRef existing = unique.get(key);
                unique.put(key, new TableScanRef(
                        existing.catalogName(), existing.schemaName(), existing.tableName(),
                        existing.scan(), null));
            }
        }
        return unique;
    }

    private static void requireSameReadSource(TableScanRef previous, TableScanRef current) {
        var type = org.pangrova.connector.api.metadata.table.ConnectorReadBinding.class;
        if (!java.util.Objects.equals(PlanTable.require(previous.scan()).unwrap(type),
                PlanTable.require(current.scan()).unwrap(type))
                || !previous.scan().getRowType().equals(current.scan().getRowType())) {
            throw new PlannerException(org.pangrova.common.exception.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Conflicting read bindings for one source table: " + current.tableName());
        }
    }

    private static Constraint buildConstraint(TableScanRef ref) {
        if (ref.filterCondition() == null) {
            return Constraint.EMPTY;
        }
        try {
            Map<ColumnHandle, Domain> domains = extractDomains(ref.filterCondition(), ref.scan());
            if (domains.isEmpty()) {
                return Constraint.EMPTY;
            }
            return () -> domains;
        } catch (Exception e) {
            LOG.debug("event=constraint_extraction_failed tableRef={} error={}",
                    ref, e.getMessage());
            return Constraint.EMPTY;
        }
    }

    // --- Predicate extraction ---

    private static Map<ColumnHandle, Domain> extractDomains(RexNode condition, TableScan scan) {
        Map<ColumnHandle, Domain> domains = new HashMap<>();
        extractDomainsRecursive(condition, scan, domains);
        return domains;
    }

    private static void extractDomainsRecursive(
            RexNode node, TableScan scan, Map<ColumnHandle, Domain> domains) {
        if (!(node instanceof RexCall call)) {
            return;
        }
        if (call.getKind() == SqlKind.AND) {
            for (RexNode operand : call.getOperands()) {
                extractDomainsRecursive(operand, scan, domains);
            }
            return;
        }
        if (call.getKind() == SqlKind.OR) {
            tryExtractOrAsInList(call, scan, domains);
            return;
        }
        extractComparisonDomain(call, scan, domains);
    }

    private static void tryExtractOrAsInList(
            RexCall orCall, TableScan scan, Map<ColumnHandle, Domain> domains) {
        String commonColumn = null;
        Set<Object> values = new LinkedHashSet<>();
        for (RexNode operand : orCall.getOperands()) {
            if (!(operand instanceof RexCall child) || child.getKind() != SqlKind.EQUALS) {
                return;
            }
            if (child.getOperands().size() != 2) {
                return;
            }
            ComparisonOperands ops = resolveComparisonOperands(
                    child.getOperands().get(0), child.getOperands().get(1));
            if (ops == null || ops.literal().getValue() == null) {
                return;
            }
            String col = scan.getRowType().getFieldList()
                    .get(ops.ref().getIndex()).getName();
            if (commonColumn == null) {
                commonColumn = col;
            } else if (!commonColumn.equals(col)) {
                return;
            }
            Comparable<?> val = coerceLiteralValue(ops);
            if (val == null) {
                return;
            }
            values.add(val);
        }
        if (commonColumn != null && !values.isEmpty()) {
            domains.merge(new NamedColumnHandle(commonColumn),
                    new Domain.InList(values),
                    PlanSplitSourceExtractor::intersectDomains);
        }
    }

    private static void extractComparisonDomain(
            RexCall call, TableScan scan, Map<ColumnHandle, Domain> domains) {
        if (call.getOperands().size() != 2) {
            return;
        }
        ComparisonOperands operands = resolveComparisonOperands(
                call.getOperands().get(0), call.getOperands().get(1));
        if (operands == null || operands.literal().getValue() == null) {
            return;
        }
        applyComparisonDomain(call, scan, domains, operands);
    }

    private static void applyComparisonDomain(
            RexCall call, TableScan scan, Map<ColumnHandle, Domain> domains,
            ComparisonOperands operands) {
        String columnName = scan.getRowType().getFieldList()
                .get(operands.ref().getIndex()).getName();
        Comparable<?> value = coerceLiteralValue(operands);
        if (value == null) {
            return;
        }
        Domain domain = toDomain(call.getKind(), value, operands.swapped());
        if (domain != null) {
            NamedColumnHandle key = new NamedColumnHandle(columnName);
            domains.merge(key, domain, PlanSplitSourceExtractor::intersectDomains);
        }
    }

    private static Comparable<?> coerceLiteralValue(ComparisonOperands operands) {
        Object raw = operands.literal().getValue();
        if (raw == null) {
            return null;
        }
        SqlTypeName columnType = operands.ref().getType().getSqlTypeName();
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
        if (raw instanceof org.apache.calcite.util.NlsString nls) {
            return nls.getValue();
        }
        return (Comparable<?>) raw;
    }

    private static ComparisonOperands resolveComparisonOperands(RexNode left, RexNode right) {
        if (left instanceof RexInputRef r && right instanceof RexLiteral l) {
            return new ComparisonOperands(r, l, false);
        }
        if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            return new ComparisonOperands(r, l, true);
        }
        return null;
    }

    private static Domain toDomain(SqlKind kind, Comparable<?> value, boolean swapped) {
        return switch (kind) {
            case EQUALS -> new Domain.Range(value, value, true, true);
            case LESS_THAN -> swapped
                    ? new Domain.Range(value, null, false, false)
                    : new Domain.Range(null, value, false, false);
            case LESS_THAN_OR_EQUAL -> swapped
                    ? new Domain.Range(value, null, true, false)
                    : new Domain.Range(null, value, false, true);
            case GREATER_THAN -> swapped
                    ? new Domain.Range(null, value, false, false)
                    : new Domain.Range(value, null, false, false);
            case GREATER_THAN_OR_EQUAL -> swapped
                    ? new Domain.Range(null, value, false, true)
                    : new Domain.Range(value, null, true, false);
            case IS_NULL -> new Domain.IsNull();
            case IS_NOT_NULL -> new Domain.IsNotNull();
            default -> null;
        };
    }

    // --- Domain intersection ---

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Domain intersectDomains(Domain existing, Domain incoming) {
        if (existing instanceof Domain.Range r1 && incoming instanceof Domain.Range r2) {
            Comparable<?> low = pickTighterLow(r1, r2);
            boolean lowInc = pickTighterLowInclusive(r1, r2);
            Comparable<?> high = pickTighterHigh(r1, r2);
            boolean highInc = pickTighterHighInclusive(r1, r2);
            if (low == null && high == null) {
                return existing;
            }
            return new Domain.Range(low, high, lowInc, highInc);
        }
        if (existing instanceof Domain.IsNull) {
            return existing;
        }
        return incoming;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<?> pickTighterLow(Domain.Range r1, Domain.Range r2) {
        if (r1.low() == null) return r2.low();
        if (r2.low() == null) return r1.low();
        int cmp = ((Comparable) r1.low()).compareTo(r2.low());
        return cmp >= 0 ? r1.low() : r2.low();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean pickTighterLowInclusive(Domain.Range r1, Domain.Range r2) {
        if (r1.low() == null) return r2.lowInclusive();
        if (r2.low() == null) return r1.lowInclusive();
        int cmp = ((Comparable) r1.low()).compareTo(r2.low());
        if (cmp > 0) return r1.lowInclusive();
        if (cmp < 0) return r2.lowInclusive();
        return r1.lowInclusive() && r2.lowInclusive();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Comparable<?> pickTighterHigh(Domain.Range r1, Domain.Range r2) {
        if (r1.high() == null) return r2.high();
        if (r2.high() == null) return r1.high();
        int cmp = ((Comparable) r1.high()).compareTo(r2.high());
        return cmp <= 0 ? r1.high() : r2.high();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static boolean pickTighterHighInclusive(Domain.Range r1, Domain.Range r2) {
        if (r1.high() == null) return r2.highInclusive();
        if (r2.high() == null) return r1.highInclusive();
        int cmp = ((Comparable) r1.high()).compareTo(r2.high());
        if (cmp < 0) return r1.highInclusive();
        if (cmp > 0) return r2.highInclusive();
        return r1.highInclusive() && r2.highInclusive();
    }

    // --- Internal records ---

    private record ComparisonOperands(RexInputRef ref, RexLiteral literal, boolean swapped) {}

    record TableScanRef(String catalogName, String schemaName, String tableName,
                        TableScan scan, RexNode filterCondition) {
        static TableScanRef fromQualifiedName(List<String> qn, TableScan scan,
                                               RexNode filterCondition) {
            return switch (qn.size()) {
                case 3 -> new TableScanRef(qn.get(0), qn.get(1), qn.get(2),
                        scan, filterCondition);
                case 2 -> new TableScanRef("default", qn.get(0), qn.get(1),
                        scan, filterCondition);
                default -> new TableScanRef("default", "default", qn.get(qn.size() - 1),
                        scan, filterCondition);
            };
        }
    }

    /**
     * Simple column handle backed by column name for predicate pushdown.
     */
    record NamedColumnHandle(String columnName) implements ColumnHandle {}
}
