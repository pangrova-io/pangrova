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

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.planner.mv.DynamicTableResolution;
import org.pangrova.planner.lineage.ColumnDerivation;
import org.pangrova.planner.lineage.ColumnLineageExtractor;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.planner.stability.PlanHasher;
import org.pangrova.planner.vector.VectorSearchNode;
import org.pangrova.planner.vector.VectorSearchRewriter;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
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
 * <p>Metadata is immutable. The Calcite tree remains query-owned and must not be
 * cached across queries or mutated concurrently. Worker dispatch uses serialized
 * immutable protocol bytes, not this tree.</p>
 */
@NotThreadSafe
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
     * Returns the SQL output names preserved before optimization removes alias projects.
     */
    public List<String> queryOutputColumns() {
        if (columnNames.size() != plan.getRowType().getFieldCount()) {
            throw new IllegalStateException("SQL output names differ from the planned column count");
        }
        return List.copyOf(columnNames);
    }

    /**
     * Returns SQL type name strings for each output column (e.g., "INT", "VARCHAR").
     * Used by CTAS to derive the destination table schema without importing Calcite.
     */
    public List<String> queryOutputColumnTypeNames() {
        return plan.getRowType().getFieldList().stream()
                .map(f -> mapSqlTypeName(f.getType()))
                .toList();
    }

    private static String mapSqlTypeName(org.apache.calcite.rel.type.RelDataType type) {
        if (type instanceof org.pangrova.planner.type.StorageVectorRelDataType vector) {
            return vector.logicalType().getCanonicalSql();
        }
        org.apache.calcite.sql.type.SqlTypeName sqlType = type.getSqlTypeName();
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
            case TIMESTAMP -> type.getPrecision() > 6 ? "TIMESTAMP_NS" : "TIMESTAMP";
            case TIMESTAMP_TZ, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> throw new IllegalArgumentException(
                    "CTAS cannot preserve timezone timestamp precision in the destination schema: " + type);
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
        return List.copyOf(columnNames);
    }

    public List<RuntimeFilterDescriptor> runtimeFilterDescriptors() {
        return List.copyOf(runtimeFilterDescriptors);
    }

    public List<DynamicTableResolution.MergeOnRead> mergeOnReadHints() {
        return List.copyOf(mergeOnReadHints);
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

    /**
     * Returns a stable digest of the fully rewritten plan.
     *
     * <p>Callers that use this value for result-cache isolation must invoke it
     * only after row filters and column masks have been applied. The digest
     * includes filter and projection expressions, so a change to an effective
     * security policy cannot reuse an entry produced by the previous policy.</p>
     */
    public String effectivePlanDigest() {
        return PlanHasher.planHash(plan);
    }

    /** Serializes this protected relation without reparsing an unprotected SQL string. */
    public byte[] serializeValidated(SqlCompiler compiler) {
        return compiler.convertValidateAndSerializeFragment(plan, columnNames).planBytes();
    }

    /** Whether this plan can be evaluated independently on each source partition. */
    public boolean isSplitIndependent() {
        return PartitionLocalPlanInspector.isIndependent(plan);
    }

    /**
     * Returns whether this secured plan is safe for ordinary result caching.
     * External AI/FILE invocations and non-deterministic expressions are never
     * cached because provider state, policy, usage accounting, and side effects
     * are not represented by relational source invalidation events.
     */
    public boolean isResultCacheSafe() {
        return isResultCacheSafe(plan);
    }

    private static boolean isResultCacheSafe(RelNode node) {
        if (node instanceof TableModify) {
            return false;
        }
        CacheSafetyInspector inspector = new CacheSafetyInspector();
        node.accept(inspector);
        if (!inspector.safe) {
            return false;
        }
        for (RelNode input : node.getInputs()) {
            if (!isResultCacheSafe(input)) {
                return false;
            }
        }
        return true;
    }

    private static final class CacheSafetyInspector extends RexShuttle {
        private boolean safe = true;

        @Override
        public org.apache.calcite.rex.RexNode visitCall(RexCall call) {
            if (!call.getOperator().isDeterministic()) {
                safe = false;
            }
            String operator = call.getOperator().getName()
                    .toLowerCase(java.util.Locale.ROOT);
            if (operator.startsWith("ai_") || operator.startsWith("ai:")
                    || operator.startsWith("file_") || operator.startsWith("file:")) {
                safe = false;
            }
            if (call instanceof RexSubQuery subQuery && !isResultCacheSafe(subQuery.rel)) {
                safe = false;
            }
            return super.visitCall(call);
        }
    }

    // ── Table reference extraction (for access control, cache, lineage) ──

    public List<List<String>> tableReferences() {
        List<List<String>> refs = new ArrayList<>();
        collectTableRefsRecursive(plan, refs);
        return List.copyOf(refs);
    }

    private static void collectTableRefsRecursive(RelNode node, List<List<String>> refs) {
        if (node instanceof TableScan scan) {
            refs.add(PlanTable.require(scan).getQualifiedName());
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

    /** Extracts complete column derivations; unknown origins reject the compiled query. */
    public List<ColumnDerivation> columnLineage() {
        return LINEAGE_EXTRACTOR.extract(plan);
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
                        PlanTable.require(modify).getQualifiedName(),
                        DmlWriteTarget.DmlOperation.fromCalciteName(
                                modify.getOperation().name()),
                        org.pangrova.planner.declaration.CompiledWriteDeclaration.capture(modify)));
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

    /** Carries physical identity through producer/exchange/merge and trims only final output. */
    public FragmentedPlan fragmentWithPhysicalRetrievalIdentity() {
        RelNode extended = org.pangrova.planner.retrieval.PhysicalRetrievalIdentityRewriter.rewrite(plan);
        FragmentedPlan fragments = PlanFragmenter.fragment(extended, -1);
        int visibleCount = plan.getRowType().getFieldCount();
        var stages = fragments.stages().stream().map(stage -> {
            if (!stage.taskType().equals("OUTPUT")) {
                return stage;
            }
            RelNode output = stage.fragment().planNode;
            List<org.apache.calcite.rex.RexNode> expressions = java.util.stream.IntStream
                    .range(0, visibleCount).mapToObj(index ->
                            (org.apache.calcite.rex.RexNode) org.apache.calcite.rex.RexInputRef.of(
                                    index, output.getRowType())).toList();
            var visible = org.apache.calcite.rel.logical.LogicalProject.create(
                    output, List.of(), expressions, plan.getRowType().getFieldNames());
            return new PlanStage(stage.stageId(), new PlanFragment(visible,
                    stage.fragment().estimatedShuffleBytes(), stage.fragment().estimatedShuffleRows()),
                    stage.parallelism(), stage.taskType());
        }).toList();
        return new FragmentedPlan(stages, fragments.transfers(), fragments.stageDependencies());
    }

    /** Serializes only this protected plan's exchange stages; no original SQL retry or persisted node is needed. */
    public AuthorizedStageGraph serializeAuthorizedStageGraph(SqlCompiler compiler, int parallelism) {
        var purpose = AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT;
        return AuthorizedStageGraph.compile(PlanFragmenter.fragment(plan, parallelism, purpose),
                compiler, columnNames, purpose);
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
