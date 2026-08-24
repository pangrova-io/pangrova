package org.pangrova.planner.substrait;

import io.substrait.relation.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

/**
 * Converts DML operations (DELETE, UPDATE, MERGE) from Calcite LogicalTableModify
 * into Substrait plans suitable for copy-on-write execution in the native engine.
 */
final class DmlOperationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DmlOperationConverter.class);

    private final Function<RelNode, Rel> visitFn;

    DmlOperationConverter(Function<RelNode, Rel> visitFn) {
        this.visitFn = visitFn;
    }

    /**
     * Converts LogicalTableModify (DELETE/UPDATE/MERGE) by converting its input sub-plan.
     * The input contains the read plan (TableScan + Filter/Join) that identifies affected rows.
     * Native worker uses this read plan to execute the DML operation via connector SPI.
     */
    Rel convert(LogicalTableModify modify) {
        LOG.debug("event=convert_table_modify operation={} table={}",
                modify.getOperation(), modify.getTable().getQualifiedName());
        if (modify.getOperation() == org.apache.calcite.rel.core.TableModify.Operation.MERGE) {
            return convertMerge(modify);
        }
        if (modify.getOperation() == org.apache.calcite.rel.core.TableModify.Operation.DELETE) {
            return convertDelete(modify);
        }
        if (modify.getOperation() == org.apache.calcite.rel.core.TableModify.Operation.UPDATE) {
            return convertUpdate(modify);
        }
        return visitFn.apply(modify.getInput());
    }

    /**
     * Converts DELETE to produce surviving rows for copy-on-write.
     *
     * <p>Calcite generates Filter(WHERE) → TableScan which produces rows TO DELETE.
     * Copy-on-write needs surviving rows (NOT matching WHERE). We invert the filter
     * by wrapping it with NOT, so the plan produces rows that should remain.</p>
     */
    private Rel convertDelete(LogicalTableModify modify) {
        RelNode input = modify.getInput();
        RelNode filterCandidate = input;
        if (filterCandidate instanceof LogicalProject project) {
            filterCandidate = project.getInput();
        }
        if (filterCandidate instanceof LogicalFilter filter) {
            RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            RexNode cond = filter.getCondition();
            RexNode notCond = rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT, cond);
            RexNode isNullCond = rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL, cond);
            RexNode invertedCondition = rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.OR,
                    notCond, isNullCond);
            LogicalFilter invertedFilter = filter.copy(
                    filter.getTraitSet(), filter.getInput(), invertedCondition);
            return visitFn.apply(invertedFilter);
        }
        return visitFn.apply(filterCandidate);
    }

    /**
     * Converts UPDATE to produce all rows for copy-on-write.
     *
     * <p>Calcite generates Project(pass-through + SET expressions) → Filter(WHERE) → TableScan.
     * The Project has N+M expressions: N pass-through columns followed by M SET values.
     * updateColumnList identifies which columns are updated. Copy-on-write needs ALL rows:
     * matched rows with new values, unmatched rows with original values.</p>
     */
    private Rel convertUpdate(LogicalTableModify modify) {
        RelNode input = modify.getInput();
        List<String> updateColumns = modify.getUpdateColumnList();

        if (!(input instanceof LogicalProject outerProject)) {
            return visitFn.apply(input);
        }

        RelNode projectInput = outerProject.getInput();

        if (!(projectInput instanceof LogicalFilter)) {
            return buildUpdateProject(outerProject, projectInput, null, updateColumns);
        }

        LogicalFilter filter = (LogicalFilter) projectInput;
        return buildUpdateProject(outerProject, filter.getInput(), filter.getCondition(), updateColumns);
    }

    /**
     * Builds an N-column Project for UPDATE copy-on-write.
     * Calcite's UPDATE Project has N+M expressions (N pass-through + M SET values).
     * This method creates a clean N-column Project where updated columns use
     * CASE WHEN condition THEN set_value ELSE original END (when condition is non-null)
     * or the SET value directly (when condition is null, i.e., UPDATE without WHERE).
     */
    private Rel buildUpdateProject(LogicalProject calciteProject, RelNode tableScan,
                                   RexNode condition, List<String> updateColumns) {
        int tableColumnCount = tableScan.getRowType().getFieldCount();
        RexBuilder rexBuilder = calciteProject.getCluster().getRexBuilder();

        var updateExprMap = resolveUpdateExpressionMap(
                updateColumns, tableScan, tableColumnCount);

        List<org.apache.calcite.rex.RexNode> newExpressions = buildColumnExpressions(
                tableColumnCount, updateExprMap, calciteProject, tableScan,
                rexBuilder, condition);

        var typeFactory = calciteProject.getCluster().getTypeFactory();
        var fieldNames = tableScan.getRowType().getFieldNames();
        var fieldTypes = new java.util.ArrayList<org.apache.calcite.rel.type.RelDataType>();
        for (int i = 0; i < tableColumnCount; i++) {
            fieldTypes.add(newExpressions.get(i).getType());
        }
        var newRowType = typeFactory.createStructType(fieldTypes, fieldNames);

        LogicalProject newProject = LogicalProject.create(
                tableScan, java.util.List.of(), newExpressions, newRowType);
        return visitFn.apply(newProject);
    }

    /**
     * Resolves which table columns are updated and maps each to its SET expression
     * index in the Calcite Project. SET expressions start at position tableColumnCount.
     */
    private static java.util.Map<Integer, Integer> resolveUpdateExpressionMap(
            List<String> updateColumns, RelNode tableScan, int tableColumnCount) {
        var updatedIndices = new java.util.HashSet<Integer>();
        if (updateColumns != null) {
            for (String colName : updateColumns) {
                for (int j = 0; j < tableColumnCount; j++) {
                    if (tableScan.getRowType().getFieldList().get(j).getName()
                            .equalsIgnoreCase(colName)) {
                        updatedIndices.add(j);
                        break;
                    }
                }
            }
        }
        var exprMap = new java.util.HashMap<Integer, Integer>();
        int setIdx = 0;
        for (int j = 0; j < tableColumnCount; j++) {
            if (updatedIndices.contains(j)) {
                exprMap.put(j, tableColumnCount + setIdx);
                setIdx++;
            }
        }
        return exprMap;
    }

    /**
     * Builds per-column expressions: CASE WHEN for updated columns, pass-through
     * for non-updated columns. Casts SET expressions to match target column type
     * to prevent native engine type mismatch in CASE WHEN branches.
     */
    private static List<org.apache.calcite.rex.RexNode> buildColumnExpressions(
            int tableColumnCount, java.util.Map<Integer, Integer> updateExprMap,
            LogicalProject calciteProject, RelNode tableScan,
            RexBuilder rexBuilder, RexNode condition) {
        List<org.apache.calcite.rex.RexNode> expressions = new java.util.ArrayList<>();
        for (int i = 0; i < tableColumnCount; i++) {
            var targetType = tableScan.getRowType().getFieldList().get(i).getType();
            if (updateExprMap.containsKey(i)) {
                org.apache.calcite.rex.RexNode setExpr =
                        calciteProject.getProjects().get(updateExprMap.get(i));
                if (!setExpr.getType().getSqlTypeName().equals(targetType.getSqlTypeName())) {
                    setExpr = rexBuilder.makeAbstractCast(targetType, setExpr);
                }
                if (condition != null) {
                    org.apache.calcite.rex.RexNode originalRef =
                            rexBuilder.makeInputRef(targetType, i);
                    setExpr = rexBuilder.makeCall(
                            org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE,
                            condition, setExpr, originalRef);
                }
                expressions.add(setExpr);
            } else {
                expressions.add(rexBuilder.makeInputRef(targetType, i));
            }
        }
        return expressions;
    }

    /**
     * Converts MERGE by producing the correct plan for each MERGE type.
     *
     * <p>For MERGE UPDATE (updateColumnList non-empty): changes LEFT JOIN to RIGHT JOIN
     * so all target rows are preserved. Write mode: OVERWRITE.</p>
     *
     * <p>For MERGE DELETE (updateColumnList empty, INNER JOIN): transforms to ANTI JOIN
     * (target rows not matching source = surviving rows). Write mode: OVERWRITE.</p>
     *
     * <p>For MERGE INSERT only (updateColumnList empty, LEFT JOIN): passes through.
     * Write mode: INSERT (set by coordinator).</p>
     */
    private Rel convertMerge(LogicalTableModify modify) {
        RelNode input = modify.getInput();
        List<String> updateColumns = modify.getUpdateColumnList();
        boolean hasUpdateColumns = updateColumns != null && !updateColumns.isEmpty();

        if (!(input instanceof LogicalProject project)
                || !(project.getInput() instanceof LogicalJoin join)) {
            return visitFn.apply(input);
        }

        if (hasUpdateColumns) {
            return convertMergeUpdate(project, join, updateColumns,
                    modify.getTable().getRowType().getFieldNames());
        }
        if (join.getJoinType() == JoinRelType.INNER) {
            return convertMergeDelete(join);
        }
        return convertMergeInsertOnly(project, join);
    }

    /**
     * MERGE UPDATE: swaps inputs to a LEFT JOIN so all target rows are preserved,
     * then projects updated columns via CASE WHEN matched THEN source ELSE target.
     */
    private Rel convertMergeUpdate(LogicalProject project, LogicalJoin join,
                                   List<String> updateColumns,
                                   List<String> targetColumnNames) {
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        LogicalJoin leftJoin = swapToTargetFirstLeftJoin(join, rexBuilder);
        int sourceFieldCount = join.getLeft().getRowType().getFieldCount();
        int targetFieldCount = join.getRight().getRowType().getFieldCount();

        // Match indicator: first source column (at targetFieldCount after swap).
        // For matched rows this is non-NULL (it was used in the equi-join);
        // for unmatched rows all source columns are NULL.
        RexNode matchIndicator = rexBuilder.makeInputRef(
                leftJoin.getRowType().getFieldList().get(targetFieldCount).getType(),
                targetFieldCount);
        RexNode isMatched = rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                matchIndicator);

        // Build a trimmed output project with exactly targetFieldCount columns.
        // For non-updated columns: reference target position directly.
        // For updated columns: use CASE WHEN to handle unmatched rows:
        //   CASE WHEN <source_indicator> IS NOT NULL
        //        THEN source_col ELSE target_col END
        // This ensures unmatched target rows (LEFT JOIN produces NULL source)
        // retain their original values instead of being overwritten with NULL.
        List<RexNode> trimmedProjects = new java.util.ArrayList<>();
        List<String> outputNames = new java.util.ArrayList<>();
        for (int i = 0; i < targetFieldCount; i++) {
            String colName = targetColumnNames.get(i);
            int updateIndex = updateColumns.indexOf(colName);
            RexNode setExpression = updateIndex >= 0
                ? remapForSwappedJoin(
                    project.getProjects().get(targetFieldCount + updateIndex),
                    sourceFieldCount, targetFieldCount, rexBuilder)
                : null;
            trimmedProjects.add(buildMergeUpdateColumnExpr(
                leftJoin, i, setExpression, isMatched, rexBuilder));
            outputNames.add(colName);
        }
        var outputRowType = rexBuilder.getTypeFactory().createStructType(
                trimmedProjects.stream()
                        .map(RexNode::getType)
                        .toList(),
                outputNames);
        LogicalProject trimmedProject = LogicalProject.create(
                leftJoin, java.util.Collections.emptyList(),
                trimmedProjects, outputRowType);
        return visitFn.apply(trimmedProject);
    }

    /**
     * Builds one output expression for a MERGE UPDATE column: CASE WHEN matched
     * THEN source ELSE target for updated columns, pass-through otherwise.
     */
    private static RexNode buildMergeUpdateColumnExpr(
            LogicalJoin leftJoin, int columnIdx, RexNode setExpression,
            RexNode isMatched, RexBuilder rexBuilder) {
        if (setExpression == null) {
            return rexBuilder.makeInputRef(
                    leftJoin.getRowType().getFieldList().get(columnIdx).getType(), columnIdx);
        }
        RexNode targetRef = rexBuilder.makeInputRef(
                leftJoin.getRowType().getFieldList().get(columnIdx).getType(), columnIdx);
        return rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE,
            isMatched, setExpression, targetRef);
    }

        private static RexNode remapForSwappedJoin(
            RexNode expression, int sourceFieldCount, int targetFieldCount,
            RexBuilder rexBuilder) {
        return expression.accept(new org.apache.calcite.rex.RexShuttle() {
            @Override
            public RexNode visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
            int index = inputRef.getIndex();
            int remappedIndex = index < sourceFieldCount
                ? targetFieldCount + index
                : index - sourceFieldCount;
            return rexBuilder.makeInputRef(inputRef.getType(), remappedIndex);
            }
        });
        }

    /**
     * MERGE DELETE (INNER JOIN): transforms to LEFT JOIN + IS NULL filter
     * so only target rows not matching the source survive.
     */
    private Rel convertMergeDelete(LogicalJoin join) {
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        int targetFieldCount = join.getRight().getRowType().getFieldCount();

        // Use LEFT JOIN + IS NULL filter pattern instead of ANTI JOIN.
        // This is semantically equivalent but avoids native engine limitations
        // with ANTI JOIN non-equi conditions.
        // LEFT JOIN: all target rows preserved; source side is NULL for unmatched.
        LogicalJoin leftJoinDel = swapToTargetFirstLeftJoin(join, rexBuilder);

        // Filter: keep only unmatched rows (where source side IS NULL).
        // Use the first source column as the null indicator.
        RexNode sourceIndicator = rexBuilder.makeInputRef(
                leftJoinDel.getRowType().getFieldList().get(targetFieldCount).getType(),
                targetFieldCount);
        RexNode isNullFilter = rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL,
                sourceIndicator);
        LogicalFilter nullFilter = LogicalFilter.create(leftJoinDel, isNullFilter);

        // Project: emit only target columns (first targetFieldCount columns).
        List<RexNode> targetProjections = new java.util.ArrayList<>();
        List<String> targetNames = new java.util.ArrayList<>();
        for (int i = 0; i < targetFieldCount; i++) {
            targetProjections.add(rexBuilder.makeInputRef(
                    nullFilter.getRowType().getFieldList().get(i).getType(), i));
            targetNames.add(nullFilter.getRowType().getFieldList().get(i).getName());
        }
        var targetRowType = rexBuilder.getTypeFactory().createStructType(
                targetProjections.stream().map(RexNode::getType).toList(),
                targetNames);
        LogicalProject targetOnlyProject = LogicalProject.create(
                nullFilter, java.util.Collections.emptyList(),
                targetProjections, targetRowType);
        return visitFn.apply(targetOnlyProject);
    }

    /** MERGE INSERT only (LEFT JOIN): converts to ANTI JOIN preserving the project shape. */
    private Rel convertMergeInsertOnly(LogicalProject project, LogicalJoin join) {
        LogicalJoin antiJoin = join.copy(
                join.getTraitSet(), join.getCondition(),
                join.getLeft(), join.getRight(),
                JoinRelType.ANTI, join.isSemiJoinDone());
        LogicalProject newProject = project.copy(
                project.getTraitSet(), antiJoin,
                project.getProjects(), project.getRowType());
        return visitFn.apply(newProject);
    }

    /**
     * Swaps join inputs to (target|source) layout as a LEFT JOIN, remapping the
     * join condition field references accordingly.
     */
    private static LogicalJoin swapToTargetFirstLeftJoin(LogicalJoin join, RexBuilder rexBuilder) {
        int sourceFieldCount = join.getLeft().getRowType().getFieldCount();
        int targetFieldCount = join.getRight().getRowType().getFieldCount();
        // Remap condition from (source|target) to (target|source) layout.
        org.apache.calcite.rex.RexShuttle swapShuttle =
                new org.apache.calcite.rex.RexShuttle() {
                    @Override
                    public RexNode visitInputRef(
                            org.apache.calcite.rex.RexInputRef inputRef) {
                        int idx = inputRef.getIndex();
                        int newIdx = (idx < sourceFieldCount)
                                ? targetFieldCount + idx
                                : idx - sourceFieldCount;
                        return rexBuilder.makeInputRef(inputRef.getType(), newIdx);
                    }
                };
        RexNode remappedCondition = join.getCondition().accept(swapShuttle);
        return join.copy(
                join.getTraitSet(), remappedCondition,
                join.getRight(), join.getLeft(),
                JoinRelType.LEFT, join.isSemiJoinDone());
    }
}
