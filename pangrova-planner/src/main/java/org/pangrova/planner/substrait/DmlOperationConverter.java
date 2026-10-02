package org.pangrova.planner.substrait;

import org.pangrova.planner.PlanTable;

import static org.pangrova.planner.substrait.DmlMergeJoinSupport.swapToTargetFirstLeftJoin;

import io.substrait.relation.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Converts DML operations (DELETE, UPDATE, MERGE) from Calcite LogicalTableModify
 * into Substrait plans suitable for copy-on-write execution in the native engine.
 */
final class DmlOperationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DmlOperationConverter.class);

    private final Function<RelNode, Rel> visitFn;
    private final Consumer<LogicalJoin> requireSingleMatch;

    DmlOperationConverter(Function<RelNode, Rel> visitFn,
                          Consumer<LogicalJoin> requireSingleMatch) {
        this.visitFn = visitFn;
        this.requireSingleMatch = requireSingleMatch;
    }

    /**
     * Converts LogicalTableModify (DELETE/UPDATE/MERGE) by converting its input sub-plan.
     * The input contains the read plan (TableScan + Filter/Join) that identifies affected rows.
     * Native worker uses this read plan to execute the DML operation via connector SPI.
     */
    Rel convert(LogicalTableModify modify) {
        LOG.debug("event=convert_table_modify operation={} table={}",
                modify.getOperation(), PlanTable.require(modify)
                        .getQualifiedName());
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
        // DELETE without WHERE deletes every row. The write still needs the
        // target schema, but must not copy the unchanged table back to storage.
        return visitFn.apply(LogicalFilter.create(filterCandidate,
                filterCandidate.getCluster().getRexBuilder().makeLiteral(false)));
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
        var exprMap = new java.util.HashMap<Integer, Integer>();
        if (updateColumns != null) {
            for (int setIndex = 0; setIndex < updateColumns.size(); setIndex++) {
                String colName = updateColumns.get(setIndex);
                for (int j = 0; j < tableColumnCount; j++) {
                    if (tableScan.getRowType().getFieldList().get(j).getName()
                            .equalsIgnoreCase(colName)) {
                        exprMap.put(j, tableColumnCount + setIndex);
                        break;
                    }
                }
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
            throw unsupportedMergeShape("expected Project over Join");
        }

        if (hasUpdateColumns) {
            int targetColumnCount = join.getRight().getRowType().getFieldCount();
            if (join.getJoinType() == JoinRelType.LEFT
                    && project.getProjects().size()
                    == 2 * targetColumnCount + updateColumns.size()) {
                return convertMergeUpdateInsert(project, join, updateColumns,
                        PlanTable.require(modify)
                                .getRowType().getFieldNames());
            }
            if (join.getJoinType() == JoinRelType.INNER
                    && project.getProjects().size()
                    == targetColumnCount + updateColumns.size()) {
                return convertMergeUpdate(project, join, updateColumns,
                        PlanTable.require(modify)
                                .getRowType().getFieldNames());
            }
            throw unsupportedMergeShape("unexpected update project or join type");
        }
        if (join.getJoinType() == JoinRelType.INNER) {
            return convertMergeDelete(join);
        }
        if (join.getJoinType() == JoinRelType.LEFT) {
            return convertMergeInsertOnly(project, join);
        }
        throw unsupportedMergeShape("unexpected join type");
    }

    private static SubstraitConversionException unsupportedMergeShape(String detail) {
        return new SubstraitConversionException(
                org.pangrova.common.exception.PangrovaErrorCode.NATIVE_UNSUPPORTED_PLAN,
                "MERGE cannot be lowered to one native write: " + detail);
    }

    /**
     * MERGE UPDATE: swaps inputs to a LEFT JOIN so all target rows are preserved,
     * then projects updated columns via CASE WHEN matched THEN source ELSE target.
     */
    private Rel convertMergeUpdate(LogicalProject project, LogicalJoin join,
                                   List<String> updateColumns,
                                   List<String> targetColumnNames) {
        return visitFn.apply(buildMergeUpdateProject(
                project, join, updateColumns, targetColumnNames));
    }

    private Rel convertMergeUpdateInsert(LogicalProject project, LogicalJoin join,
                                         List<String> updateColumns,
                                         List<String> targetColumnNames) {
        LogicalProject updatedTargets = buildMergeUpdateProject(
                project, join, updateColumns, targetColumnNames);
        LogicalJoin unmatchedSources = join.copy(
                join.getTraitSet(), join.getCondition(), join.getLeft(), join.getRight(),
                JoinRelType.ANTI, join.isSemiJoinDone());
        LogicalProject insertedSources = buildMergeInsertProject(
                project, unmatchedSources, targetColumnNames);
        return visitFn.apply(LogicalUnion.create(
                List.of(updatedTargets, insertedSources), true));
    }

    private LogicalProject buildMergeInsertProject(
            LogicalProject mergeProject, LogicalJoin unmatchedSources,
            List<String> targetColumnNames) {
        int sourceColumnCount = unmatchedSources.getRowType().getFieldCount();
        List<RexNode> expressions = new java.util.ArrayList<>();
        RexBuilder rexBuilder = mergeProject.getCluster().getRexBuilder();
        for (int i = 0; i < targetColumnNames.size(); i++) {
            RexNode expression = mergeProject.getProjects().get(i);
            if (!org.apache.calcite.plan.RelOptUtil.InputFinder.bits(expression)
                    .stream().allMatch(index -> index < sourceColumnCount)) {
                throw new IllegalArgumentException(
                        "MERGE INSERT expression references target columns");
            }
            var targetType = mergeProject.getProjects().get(
                    targetColumnNames.size() + i).getType();
            expressions.add(rexBuilder.makeAbstractCast(targetType, expression));
        }
        var rowType = rexBuilder.getTypeFactory().createStructType(
                expressions.stream().map(RexNode::getType).toList(), targetColumnNames);
        return LogicalProject.create(unmatchedSources, List.of(), expressions, rowType);
    }

    private LogicalProject buildMergeUpdateProject(LogicalProject project, LogicalJoin join,
                                                    List<String> updateColumns,
                                                    List<String> targetColumnNames) {
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        LogicalJoin leftJoin = swapToTargetFirstLeftJoin(join, rexBuilder);
        // A target row must produce exactly one row from this LEFT JOIN. The
        // standard Substrait LEFT_SINGLE contract is checked by Native before
        // a write task can publish its commit fragment. If this join is ever
        // partitioned across tasks, each target partition must receive every
        // matching source row before the per-task check can be authoritative.
        requireSingleMatch.accept(leftJoin);
        int sourceFieldCount = join.getLeft().getRowType().getFieldCount();
        int targetFieldCount = join.getRight().getRowType().getFieldCount();

        // The extra non-null source marker survives matched rows even if every
        // source data column is nullable; unmatched rows have a null marker.
        int markerIndex = targetFieldCount + sourceFieldCount;
        RexNode matchIndicator = rexBuilder.makeInputRef(
                leftJoin.getRowType().getFieldList().get(markerIndex).getType(),
                markerIndex);
        RexNode isMatched = rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL,
                matchIndicator);

        // Build a trimmed output project with exactly targetFieldCount columns.
        // For non-updated columns: reference target position directly.
        // For updated columns: use CASE WHEN to handle unmatched rows:
        //   CASE WHEN <match_marker> IS NOT NULL
        //        THEN source_col ELSE target_col END
        // This ensures unmatched target rows (LEFT JOIN produces NULL source)
        // retain their original values instead of being overwritten with NULL.
        List<RexNode> trimmedProjects = new java.util.ArrayList<>();
        List<String> outputNames = new java.util.ArrayList<>();
        for (int i = 0; i < targetFieldCount; i++) {
            String colName = targetColumnNames.get(i);
            int updateIndex = updateColumnIndex(updateColumns, colName);
            RexNode setExpression = updateIndex >= 0
                ? remapForSwappedJoin(
                    project.getProjects().get(
                            project.getProjects().size() - updateColumns.size() + updateIndex),
                    sourceFieldCount, targetFieldCount, leftJoin, rexBuilder)
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
        return trimmedProject;
    }

    private static int updateColumnIndex(List<String> updateColumns, String columnName) {
        for (int i = 0; i < updateColumns.size(); i++) {
            if (updateColumns.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
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
            LogicalJoin leftJoin, RexBuilder rexBuilder) {
        return expression.accept(new org.apache.calcite.rex.RexShuttle() {
            @Override
            public RexNode visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
                int index = inputRef.getIndex();
                int remappedIndex = index < sourceFieldCount
                        ? targetFieldCount + index
                        : index - sourceFieldCount;
                // The source side becomes nullable under the target-preserving
                // LEFT join; retain the joined input's actual field type.
                return rexBuilder.makeInputRef(
                        leftJoin.getRowType().getFieldList().get(remappedIndex).getType(),
                        remappedIndex);
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
        int markerIndex = leftJoinDel.getRowType().getFieldCount() - 1;
        RexNode sourceIndicator = rexBuilder.makeInputRef(
                leftJoinDel.getRowType().getFieldList().get(markerIndex).getType(),
                markerIndex);
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

}
