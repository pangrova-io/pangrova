/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner;

import org.pangrova.planner.mv.DynamicTableResolution;
import org.pangrova.planner.lineage.ColumnDerivation;
import org.pangrova.planner.lineage.ColumnLineageExtractor;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.vector.VectorSearchNode;
import org.pangrova.planner.vector.VectorSearchRewriter;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Opaque result of SQL compilation. Wraps the Calcite {@link RelNode} tree internally
 * and exposes only coordinator-safe methods. No Calcite types appear in this class's
 * public API — coordinator code holds and passes {@code PlannedQuery} without any
 * Calcite imports.
 *
 * <p>Thread-safe: immutable once constructed.</p>
 */
public final class PlannedQuery {

    private static final Logger LOG = LoggerFactory.getLogger(PlannedQuery.class);
    private static final ColumnLineageExtractor LINEAGE_EXTRACTOR = new ColumnLineageExtractor();

    private final RelNode plan;
    private final List<String> columnNames;
    private final List<RuntimeFilterDescriptor> runtimeFilterDescriptors;
    private final List<DynamicTableResolution.MergeOnRead> mergeOnReadHints;

    PlannedQuery(RelNode plan, List<String> columnNames,
                 List<RuntimeFilterDescriptor> runtimeFilterDescriptors,
                 List<DynamicTableResolution.MergeOnRead> mergeOnReadHints) {
        this.plan = plan;
        this.columnNames = List.copyOf(columnNames);
        this.runtimeFilterDescriptors = List.copyOf(runtimeFilterDescriptors);
        this.mergeOnReadHints = List.copyOf(mergeOnReadHints);
    }

    /**
     * Returns the internal Calcite RelNode for classes that are already
     * Calcite-coupled (IndexAwareSplitFetcher, DispatchContextEnricher).
     * Do not call from coordinator code that should remain Calcite-free.
     * For split fetching, prefer {@link #splitSources()} instead.
     */
    public RelNode plan() {
        return plan;
    }

    /**
     * Extracts Calcite-free split source descriptors from the plan tree.
     * Used by SplitFetcher to enumerate connector splits without Calcite imports.
     */
    public List<SplitSourceDescriptor> splitSources() {
        return PlanSplitSourceExtractor.extract(plan);
    }

    /**
     * Returns the output column names from the top-level query plan.
     * Equivalent to the plan's row type field names.
     */
    public List<String> queryOutputColumns() {
        return plan.getRowType().getFieldNames();
    }

    /**
     * Returns SQL type name strings for each output column (e.g., "INT", "VARCHAR").
     * Used by CTAS to derive the destination table schema without importing Calcite.
     */
    public List<String> queryOutputColumnTypeNames() {
        return plan.getRowType().getFieldList().stream()
                .map(f -> mapSqlTypeName(f.getType().getSqlTypeName()))
                .toList();
    }

    private static String mapSqlTypeName(org.apache.calcite.sql.type.SqlTypeName sqlType) {
        return switch (sqlType) {
            case INTEGER -> "INT";
            case BIGINT -> "BIGINT";
            case SMALLINT -> "SMALLINT";
            case TINYINT -> "TINYINT";
            case FLOAT, REAL -> "FLOAT";
            case DOUBLE -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
            case VARCHAR, CHAR -> "VARCHAR";
            case VARBINARY, BINARY -> "VARBINARY";
            case DECIMAL -> "DECIMAL";
            case DATE -> "DATE";
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "TIMESTAMP";
            case TIME -> "TIME";
            case INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> "BIGINT";
            case INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND,
                 INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND,
                 INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> "BIGINT";
            default -> throw new IllegalArgumentException(
                    "Unsupported SQL type for CTAS schema derivation: " + sqlType);
        };
    }

    // ── Plan metadata ──

    public List<String> columnNames() {
        return columnNames;
    }

    public List<RuntimeFilterDescriptor> runtimeFilterDescriptors() {
        return runtimeFilterDescriptors;
    }

    public List<DynamicTableResolution.MergeOnRead> mergeOnReadHints() {
        return mergeOnReadHints;
    }

    // ── Query classification predicates ──

    public boolean isConstant() {
        return PangrovaPlanner.isFoldedConstant(plan);
    }

    public boolean isZeroFetchSort() {
        return PangrovaPlanner.isZeroFetchSort(plan);
    }

    public boolean isMergeOperation() {
        return PangrovaPlanner.isMergeOperation(plan);
    }

    public boolean containsUnnest() {
        return PangrovaPlanner.containsUnnest(plan);
    }

    public boolean isTablelessConstant() {
        return PangrovaPlanner.isTablelessConstant(plan);
    }

    public String planExplanation() {
        return plan.explain();
    }

    // ── Table reference extraction (for access control, cache, lineage) ──

    public List<List<String>> tableReferences() {
        List<List<String>> refs = new ArrayList<>();
        collectTableRefsRecursive(plan, refs);
        return List.copyOf(refs);
    }

    private static void collectTableRefsRecursive(RelNode node, List<List<String>> refs) {
        if (node instanceof TableScan scan) {
            refs.add(scan.getTable().getQualifiedName());
        }
        for (RelNode input : node.getInputs()) {
            collectTableRefsRecursive(input, refs);
        }
    }

    // ── Security policy application ──

    /**
     * Applies column masking policies and returns the transformed query.
     * Delegates to the applier which operates on the internal RelNode.
     */
    public PlannedQuery applyColumnMasks(ColumnMaskApplier applier, UserIdentity identity) {
        RelNode transformed = applier.applyColumnMasks(plan, identity);
        return transformed == plan ? this : withPlan(transformed);
    }

    /**
     * Applies row-level security filters and returns the transformed query.
     * Delegates to the applier which operates on the internal RelNode.
     */
    public PlannedQuery applyRowFilters(RowFilterApplier applier, UserIdentity identity) {
        RelNode transformed = applier.applyRowFilters(plan, identity);
        return transformed == plan ? this : withPlan(transformed);
    }

    // ── Column-level lineage (planner-internal Calcite analysis) ──

    /**
     * Extracts column-level derivations from the plan.
     * Returns an empty list if extraction fails.
     */
    public List<ColumnDerivation> columnLineage() {
        try {
            return LINEAGE_EXTRACTOR.extract(plan);
        } catch (Exception e) {
            LOG.debug("event=column_lineage_extraction_failed error={}", e.getMessage());
            return List.of();
        }
    }

    // ── DML write target (for DML context registration) ──

    /**
     * Returns the DML write target of this query, when the plan contains a table modify.
     *
     * @return write target with qualified table name and operation, or empty for read-only queries
     */
    public Optional<DmlWriteTarget> dmlWriteTarget() {
        return findTableModify(plan).map(modify ->
                new DmlWriteTarget(
                        modify.getTable().getQualifiedName(),
                        DmlWriteTarget.DmlOperation.fromCalciteName(
                                modify.getOperation().name())));
    }

    private static Optional<TableModify> findTableModify(RelNode node) {
        if (node instanceof TableModify modify) {
            return Optional.of(modify);
        }
        for (RelNode input : node.getInputs()) {
            Optional<TableModify> found = findTableModify(input);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    // ── Vector search detection (Calcite-free API for coordinator) ──

    /**
     * Detects whether this query plan contains a vector search pattern (ORDER BY
     * distance(column, vector) LIMIT K backed by a VECTOR index). Returns the
     * rewritten VectorSearchNode if detected, empty otherwise.
     *
     * <p>This method encapsulates the Calcite-internal instanceof-LogicalSort
     * check so that coordinator code never imports Calcite types.</p>
     *
     * @param indexLookup function: (tableName, columnName) → matching VECTOR IndexDescriptors
     * @return the vector search node if pattern matched, empty otherwise
     */
    public Optional<VectorSearchNode> tryDetectVectorSearch(
            BiFunction<String, String, List<IndexDescriptor>> indexLookup) {
        if (!(plan instanceof LogicalSort sortNode)) {
            return Optional.empty();
        }
        return VectorSearchRewriter.tryRewrite(sortNode, indexLookup);
    }

    // ── Plan fragmentation ──

    public FragmentedPlan fragment(int parallelismOverride) {
        return PlanFragmenter.fragment(plan, parallelismOverride);
    }

    public FragmentedPlan fragment() {
        return PlanFragmenter.fragment(plan, -1);
    }

    // ── Constant plan evaluation ──

    public ConstantPlanEvaluator.EvaluatedConstant evaluateConstant() {
        return ConstantPlanEvaluator.evaluateExpression(plan);
    }

    // ── AQE re-optimization ──

    public PlannedQuery reoptimize(PangrovaPlanner planner) {
        RelNode reoptimized = planner.reoptimize(plan);
        return new PlannedQuery(reoptimized, columnNames,
                runtimeFilterDescriptors, mergeOnReadHints);
    }

    /** Package-private: returns a copy with the plan replaced (used by security appliers). */
    PlannedQuery withPlan(RelNode newPlan) {
        return new PlannedQuery(newPlan, columnNames,
                runtimeFilterDescriptors, mergeOnReadHints);
    }

    /**
     * Test-only factory: wraps a raw RelNode as a PlannedQuery with empty metadata.
     * Production code MUST use {@link SqlCompiler#compileToPlannedQuery} instead.
     */
    public static PlannedQuery forTesting(RelNode plan) {
        return new PlannedQuery(plan, List.of(), List.of(), List.of());
    }
}
