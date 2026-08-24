package org.pangrova.planner.mv;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Implements structural subsumption matching for materialized view rewrite.
 *
 * <p>Given a query plan Q and a materialized view definition M, determines whether
 * Q can be answered from M — possibly with additional filtering, projection, or
 * rollup on top of M — and produces a {@link SubsumptionResult} describing the
 * required compensating operations.</p>
 *
 * <p>Three subsumption dimensions are checked:
 * <ul>
 *   <li><b>Projection subsumption</b>: query uses a subset of MV output columns</li>
 *   <li><b>Filter subsumption</b>: query adds WHERE predicates on MV columns</li>
 *   <li><b>Rollup subsumption</b>: query GROUP BY is a subset of MV GROUP BY</li>
 * </ul>
 *
 * <p>All three dimensions must be satisfiable for a match to succeed.</p>
 */
public final class StructuralSubsumptionMatcher {

    /**
     * Attempts structural subsumption matching between a query plan and an MV definition.
     *
     * @param queryPlan the incoming query's logical plan
     * @param mvDefinition the materialized view definition with structural metadata
     * @return a subsumption result if the MV can answer the query, empty otherwise
     */
    public Optional<SubsumptionResult> match(RelNode queryPlan, MaterializedViewDefinition mvDefinition) {
        if (!mvDefinition.hasStructuralMetadata()) {
            return Optional.empty();
        }

        // F-PLANNER-IDX-02 safeguard: refuse to rewrite MVs whose defining query
        // contains a WHERE clause until full predicate subsumption is implemented.
        // Without this check, a filtered MV (e.g., WHERE x > 10) could incorrectly
        // answer a broader query and silently drop rows.
        if (definingQueryHasWhereClause(mvDefinition.query())) {
            return Optional.empty();
        }

        QuerySignature querySignature = extractQuerySignature(queryPlan);
        if (querySignature == null) {
            return Optional.empty();
        }

        // Table subsumption: MV base tables must cover all query base tables
        if (!mvDefinition.baseTableNames().containsAll(querySignature.baseTables())) {
            return Optional.empty();
        }

        // Projection subsumption: MV output columns must cover all query referenced columns
        Set<String> mvColumns = mvDefinition.outputColumnNames();
        if (!mvColumns.containsAll(querySignature.referencedColumns())) {
            return Optional.empty();
        }

        // Filter subsumption: query filter predicates must reference only MV columns
        List<String> residualFilterColumns = querySignature.filterColumns();
        if (!residualFilterColumns.isEmpty() && !mvColumns.containsAll(residualFilterColumns)) {
            return Optional.empty();
        }

        // Rollup subsumption: if query has GROUP BY, it must be a subset of MV GROUP BY
        Set<String> mvGroupByColumns = mvDefinition.groupByColumnNames();
        Set<String> queryGroupByColumns = querySignature.groupByColumns();
        boolean needsRollup = false;
        if (!queryGroupByColumns.isEmpty()) {
            if (mvGroupByColumns.isEmpty()) {
                // Query needs GROUP BY but MV has no aggregation — cannot subsume
                return Optional.empty();
            }
            if (!mvGroupByColumns.containsAll(queryGroupByColumns)) {
                // Query groups by columns not in MV GROUP BY — cannot subsume
                return Optional.empty();
            }
            // If query GROUP BY is a proper subset of MV GROUP BY, we need rollup
            needsRollup = !queryGroupByColumns.equals(mvGroupByColumns);
        }

        // Determine projected columns (subset of MV columns the query actually needs)
        Set<String> projectedColumns = querySignature.referencedColumns();
        boolean needsProjection = !projectedColumns.equals(mvColumns);

        // Determine residual filter
        boolean needsFilter = !residualFilterColumns.isEmpty();

        return Optional.of(new SubsumptionResult(
                mvDefinition.name(),
                needsProjection ? projectedColumns : Set.of(),
                needsFilter ? querySignature.filterPredicateNodes() : List.of(),
                needsRollup ? queryGroupByColumns : Set.of()));
    }

    /**
     * Extracts a structural signature from the query plan for subsumption comparison.
     */
    QuerySignature extractQuerySignature(RelNode plan) {
        Set<String> baseTables = new LinkedHashSet<>();
        Set<String> referencedColumns = new LinkedHashSet<>();
        List<String> filterColumns = new ArrayList<>();
        List<RexNode> filterPredicates = new ArrayList<>();
        Set<String> groupByColumns = new LinkedHashSet<>();

        extractSignatureRecursive(plan, baseTables, referencedColumns,
                filterColumns, filterPredicates, groupByColumns);

        if (baseTables.isEmpty()) {
            return null;
        }

        return new QuerySignature(baseTables, referencedColumns, filterColumns,
                filterPredicates, groupByColumns);
    }

    private void extractSignatureRecursive(
            RelNode node,
            Set<String> baseTables,
            Set<String> referencedColumns,
            List<String> filterColumns,
            List<RexNode> filterPredicates,
            Set<String> groupByColumns) {

        if (node instanceof TableScan scan) {
            List<String> qualifiedName = scan.getTable().getQualifiedName();
            baseTables.add(String.join(".", qualifiedName));
            for (RelDataTypeField field : scan.getRowType().getFieldList()) {
                referencedColumns.add(field.getName().toLowerCase());
            }
        } else if (node instanceof Project project) {
            collectProjectColumns(project, referencedColumns);
            extractSignatureRecursive(project.getInput(), baseTables, referencedColumns,
                    filterColumns, filterPredicates, groupByColumns);
        } else if (node instanceof Filter filter) {
            RexNode condition = filter.getCondition();
            filterPredicates.add(condition);
            collectFilterColumns(condition, filter.getInput().getRowType().getFieldList(),
                    filterColumns);
            extractSignatureRecursive(filter.getInput(), baseTables, referencedColumns,
                    filterColumns, filterPredicates, groupByColumns);
        } else if (node instanceof Aggregate aggregate) {
            extractAggregateGroupBy(aggregate, groupByColumns);
            extractSignatureRecursive(aggregate.getInput(), baseTables, referencedColumns,
                    filterColumns, filterPredicates, groupByColumns);
        } else {
            for (RelNode input : node.getInputs()) {
                extractSignatureRecursive(input, baseTables, referencedColumns,
                        filterColumns, filterPredicates, groupByColumns);
            }
        }
    }

    private void collectProjectColumns(Project project, Set<String> referencedColumns) {
        List<RelDataTypeField> inputFields = project.getInput().getRowType().getFieldList();
        for (RexNode expr : project.getProjects()) {
            collectColumnRefs(expr, inputFields, referencedColumns);
        }
    }

    private void extractAggregateGroupBy(Aggregate aggregate, Set<String> groupByColumns) {
        List<RelDataTypeField> inputFields = aggregate.getInput().getRowType().getFieldList();
        for (int groupIdx : aggregate.getGroupSet()) {
            if (groupIdx < inputFields.size()) {
                groupByColumns.add(inputFields.get(groupIdx).getName().toLowerCase());
            }
        }
    }

    private void collectFilterColumns(RexNode expr, List<RelDataTypeField> fields,
                                      List<String> columns) {
        if (expr instanceof RexInputRef ref) {
            int index = ref.getIndex();
            if (index < fields.size()) {
                columns.add(fields.get(index).getName().toLowerCase());
            }
        } else if (expr instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                collectFilterColumns(operand, fields, columns);
            }
        }
    }

    private void collectColumnRefs(RexNode expr, List<RelDataTypeField> fields,
                                   Set<String> columns) {
        if (expr instanceof RexInputRef ref) {
            int index = ref.getIndex();
            if (index < fields.size()) {
                columns.add(fields.get(index).getName().toLowerCase());
            }
        } else if (expr instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                collectColumnRefs(operand, fields, columns);
            }
        }
    }

    /**
     * Conservative check for a WHERE clause in the MV's defining query text.
     * Used as a safeguard (F-PLANNER-IDX-02) to refuse rewrite for MVs whose
     * defining query has a predicate, until full predicate subsumption is
     * implemented in the matcher. False positives (refusing rewrite when no
     * real WHERE exists) are safe; false negatives (allowing rewrite when WHERE
     * exists) would be a correctness bug.
     */
    private static boolean definingQueryHasWhereClause(String query) {
        if (query == null || query.isEmpty()) {
            return false;
        }
        // Strip string literals to avoid matching the substring inside a literal.
        String stripped = stripStringLiterals(query);
        // Word-boundary search for WHERE keyword (case-insensitive).
        return java.util.regex.Pattern.compile("\\bWHERE\\b",
                java.util.regex.Pattern.CASE_INSENSITIVE).matcher(stripped).find();
    }

    private static String stripStringLiterals(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        boolean inString = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\'' || c == '"') {
                inString = !inString;
                sb.append(' ');
            } else if (!inString) {
                sb.append(c);
            } else {
                sb.append(' ');
            }
        }
        return sb.toString();
    }

    /**
     * Structural signature extracted from a query plan.
     */
    record QuerySignature(
            Set<String> baseTables,
            Set<String> referencedColumns,
            List<String> filterColumns,
            List<RexNode> filterPredicateNodes,
            Set<String> groupByColumns) {
    }
}
