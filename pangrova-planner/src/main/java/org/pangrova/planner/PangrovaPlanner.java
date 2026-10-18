package org.pangrova.planner;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.type.PlannerTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.pangrova.planner.mv.DynamicTableResolution;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;
import java.util.Map;

import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.planner.cost.StatisticsBasedCostEstimator;
import org.pangrova.planner.runtimefilter.DynamicPartitionPruningConfig;
import org.pangrova.planner.runtimefilter.RuntimeFilterDescriptor;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.planner.optimization.CascadesCboPass;
import org.pangrova.planner.optimization.HepOptimizationPass;
import org.pangrova.planner.optimization.MaterializedViewRewritePass;
import org.pangrova.planner.optimization.OptimizationContext;
import org.pangrova.planner.stats.StatisticsCollector;

/**
 * Calcite-based query planner: parse → validate → convert → optimize → RelNode.
 * Phase 0 uses HepPlanner with basic RBO rules (constant folding).
 * Thread-safe: creates fresh validator/cluster per plan() call since Calcite
 * SqlValidator and VolcanoPlanner are stateful and not thread-safe.
 */
public final class PangrovaPlanner {

    private final PangrovaParserFactory parserFactory;
    private final RelDataTypeFactory typeFactory;
    private final CalciteCatalogReader catalogReader;
    private final java.util.concurrent.atomic.AtomicLong constantFoldedTotal =
            new java.util.concurrent.atomic.AtomicLong();
    private final PangrovaSqlOperatorTable operatorTable = new PangrovaSqlOperatorTable();

    // Optional advanced feature integrations (null when not wired)
    private final StatisticsCollector statisticsCollector;
    private final StatisticsBasedCostEstimator costEstimator;
    private final CostModelConfig costModelConfig;
    private final boolean cboEnabled;
    private final QueryResultCache queryResultCache;

    // Post-optimization: MV rewrite + DPP analysis
    private final MaterializedViewRewritePass materializedViewRewritePass;

    // Pre-parse Dynamic Table planning support (SQL rewrite + merge-on-read hints)
    private final DynamicTablePlanningSupport dynamicTablePlanningSupport;

    // Cross-catalog metadata version pinning (null when not wired)
    private final CrossCatalogVersionPinner versionPinner;

    // Pluggable optimization pipeline runner
    private final PlanOptimizationRunner optimizationRunner;

    public PangrovaPlanner() {
        this(CalciteSchema.createRootSchema(false));
    }

    /**
     * Creates a planner with a custom root schema for table resolution.
     * Used when MetadataService provides catalog schemas to Calcite.
     */
    public PangrovaPlanner(CalciteSchema rootSchema) {
        this(rootSchema, List.of(""));
    }

    /**
     * Creates a planner with a custom root schema and default schema path.
     * The defaultSchemaPath controls how Calcite resolves unqualified table names.
     * Example: ["iceberg_catalog", "default"] resolves "lineitem" to "iceberg_catalog.default.lineitem".
     */
    public PangrovaPlanner(CalciteSchema rootSchema, List<String> defaultSchemaPath) {
        this(rootSchema, defaultSchemaPath, PlannerContext.NONE);
    }

    /**
     * Full constructor with optional advanced features via {@link PlannerContext}.
     * Null fields in dependencies disable the corresponding feature.
     */
    public PangrovaPlanner(CalciteSchema rootSchema, List<String> defaultSchemaPath,
                         PlannerContext deps) {
        this.parserFactory = new PangrovaParserFactory();
        this.typeFactory = new PlannerTypeFactory();
        this.catalogReader = PlannerComponentFactory.buildCatalogReader(
                typeFactory, rootSchema, defaultSchemaPath);
        var hepProgram = PlannerComponentFactory.buildHepProgram();
        this.statisticsCollector = deps.statisticsCollector();
        this.costEstimator = deps.statisticsCollector() != null
                ? new StatisticsBasedCostEstimator(deps.statisticsCollector())
                : null;
        this.queryResultCache = deps.queryResultCache();
        this.materializedViewRewritePass = new MaterializedViewRewritePass(
                deps.materializedViewRewriter(),
                deps.dynamicPartitionPruningConfig(),
                deps.runtimeFilterRegistry());
        DynamicTableSqlRewriter dynamicTableSqlRewriter = deps.dynamicTableRewriter() != null
                ? new DynamicTableSqlRewriter(this.parserFactory, deps.dynamicTableRewriter())
                : null;
        this.dynamicTablePlanningSupport =
                new DynamicTablePlanningSupport(this.parserFactory, dynamicTableSqlRewriter);
        this.versionPinner = deps.metadataVersionProvider() != null
                ? new CrossCatalogVersionPinner(deps.metadataVersionProvider()) : null;
        // CBO disabled by default: Calcite RelDecorrelator hits internal assertion
        // errors (allLessThan) on correlated subqueries under CBO. Enable via
        // explicit CboConfig once the Calcite decorator issue is resolved.
        var cboConfig = deps.cboConfig() != null ? deps.cboConfig() : org.pangrova.planner.cost.CboConfig.DISABLED;
        var selectedCostModel = deps.costModelConfig() != null ? deps.costModelConfig() : CostModelConfig.SSD_DEFAULT;
        this.costModelConfig = selectedCostModel;
        this.cboEnabled = cboConfig.enabled();
        var optimizationContext = new OptimizationContext(cboConfig, selectedCostModel, this.costEstimator);
        this.optimizationRunner = new PlanOptimizationRunner(
                List.of(
                        new HepOptimizationPass(hepProgram),
                        new CascadesCboPass()),
                optimizationContext);
    }

    /**
     * Parse, validate, optimize, and constant-fold SQL into a RelNode plan.
     * Constant expressions (no TableScan/TableModify/Exchange) are folded
     * into LogicalValues by the registered Calcite reduction rules.
     * Pre-parse: Dynamic Table names are rewritten to physical target tables.
     * Post-optimization: MV rewrite is applied when MaterializedViewQueryRewriter is wired.
     */
    /**
     * Parse, validate, optimize, and constant-fold SQL into a PlanResult.
     * Returns an immutable PlanResult containing the RelNode plan, pre-optimization
     * column names, and runtime filter descriptors — eliminating the race condition
     * from the previous volatile-field design.
     */
    public PlanResult plan(String sql) {
        // Collect merge-on-read resolutions during SQL rewrite
        List<DynamicTableResolution.MergeOnRead> mergeOnReadHints =
                dynamicTablePlanningSupport.collectMergeOnReadHints(sql);
        var rewrite = dynamicTablePlanningSupport.rewriteIfWired(sql);
        String rewrittenSql = rewrite.sql();
        dynamicTablePlanningSupport.logCteMaterializationHints(rewrittenSql);
        SqlNode sqlNode = parse(rewrittenSql);
        SqlValidator validator = buildValidator();
        SqlNode validated = validate(validator, sqlNode);
        RelOptCluster cluster = buildCluster();
        RelNode relNode = convert(validator, cluster, validated);

        // Pin metadata versions for all catalogs referenced in the query
        Map<String, Long> pinnedVersions = pinMetadataVersions(relNode);

        // Preserve SQL aliases before optimization — HEP rules like
        // PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW replace user-defined aliases
        // (e.g. "next_price", "rnk") with synthetic names ("w0$o0", "$1").
        List<String> preOptColumnNames = List.copyOf(relNode.getRowType().getFieldNames());
        if (containsMutation(relNode)) {
            verifyMetadataVersions(pinnedVersions);
            return new PlanResult(relNode, preOptColumnNames, List.of(), mergeOnReadHints,
                    rewrite.physicalToLogical());
        }
        RelNode optimized = optimizationRunner.optimize(relNode);
        List<RuntimeFilterDescriptor> filterDescriptors =
                materializedViewRewritePass.analyzeForRuntimeFilters(optimized);
        RelNode materializedViewRewritten = materializedViewRewritePass.tryMaterializedViewRewrite(optimized);
        recordConstantFold(materializedViewRewritten);

        // Verify metadata versions unchanged after compilation
        verifyMetadataVersions(pinnedVersions);

        return new PlanResult(materializedViewRewritten, preOptColumnNames, filterDescriptors,
                mergeOnReadHints, rewrite.physicalToLogical());
    }

    /** Binds SQL without deriving facts from expressions that security policies may replace. */
    public PlanResult planForPolicyApplication(String sql) {
        List<DynamicTableResolution.MergeOnRead> mergeOnReadHints =
                dynamicTablePlanningSupport.collectMergeOnReadHints(sql);
        var rewrite = dynamicTablePlanningSupport.rewriteIfWired(sql);
        String rewrittenSql = rewrite.sql();
        dynamicTablePlanningSupport.logCteMaterializationHints(rewrittenSql);
        SqlValidator validator = buildValidator();
        SqlNode validated = validate(validator, parse(rewrittenSql));
        RelNode relation = convert(validator, buildCluster(), validated);
        Map<String, Long> pinnedVersions = pinMetadataVersions(relation);
        verifyMetadataVersions(pinnedVersions);
        return new PlanResult(relation, relation.getRowType().getFieldNames(), List.of(),
                mergeOnReadHints, rewrite.physicalToLogical());
    }

    /** Optimizes only after authorization and policies; mutations retain their physical target until COW. */
    PlannedQuery optimizeProtectedQuery(PlannedQuery query) {
        if (containsMutation(query.plan())) {
            return query;
        }
        RelNode optimized = optimizationRunner.optimize(query.plan());
        List<RuntimeFilterDescriptor> descriptors = materializedViewRewritePass.analyzeForRuntimeFilters(optimized);
        // An MV substitution would introduce a different source that has not passed this query's authority.
        recordConstantFold(optimized);
        return new PlannedQuery(optimized, query.columnNames(), descriptors,
                query.mergeOnReadHints(), query.physicalToLogicalDynamicTables());
    }

    /**
     * Parse SQL text into an AST Statement without Calcite conversion.
     * Used to detect DDL/DML statements that bypass the Calcite planner.
     */
    public org.pangrova.parser.ast.Statement parseToAst(String sql) {
        return parserFactory.parseToAst(sql);
    }

    /**
     * Compiles a MERGE INTO statement into a RelNode plan.
     *
     * <p>The resulting plan represents:
     * HashJoin(source=build, target=probe) → ConditionalProject
     * (MATCHED UPDATE/DELETE, NOT MATCHED INSERT) → Native task-owned overwrite sink.
     *
     * <p>The actual Substrait conversion and worker dispatch is handled by the
     * caller (PlanExecutionFactory via NativeDmlCompileFunction).
     *
     * @param mergeStmt the parsed MERGE INTO AST node
     * @return plan result containing the compiled RelNode
     */
    public PlanResult compileMerge(org.pangrova.parser.ast.statement.dml.MergeStatement mergeStmt) {
        return planFromAst(mergeStmt);
    }

    /**
     * Plans a pre-parsed AST Statement directly, bypassing the SQL text parse step.
     * Converts AST → SqlNode → validate → RelNode → optimize → fold.
     * Eliminates the AST → SQL string → re-parse round-trip for DML statements
     * that are already parsed (DELETE, UPDATE, MERGE dispatched via NativeDmlCompileFunction).
     *
     * @param stmt the parsed AST statement
     * @return plan result containing the optimized RelNode
     */
    public PlanResult planFromAst(org.pangrova.parser.ast.Statement stmt) {
        SqlNode sqlNode = parserFactory.convertAstToSqlNode(stmt);
        SqlValidator validator = buildValidator();
        SqlNode validated = validate(validator, sqlNode);
        RelOptCluster cluster = buildCluster();
        RelNode relNode = convert(validator, cluster, validated);
        Map<String, Long> pinnedVersions = pinMetadataVersions(relNode);
        List<String> preOptColumnNames = List.copyOf(relNode.getRowType().getFieldNames());
        if (containsMutation(relNode)) {
            verifyMetadataVersions(pinnedVersions);
            return new PlanResult(relNode, preOptColumnNames, List.of());
        }
        RelNode optimized = optimizationRunner.optimize(relNode);
        List<RuntimeFilterDescriptor> filterDescriptors =
                materializedViewRewritePass.analyzeForRuntimeFilters(optimized);
        RelNode materializedViewRewritten =
                materializedViewRewritePass.tryMaterializedViewRewrite(optimized);
        recordConstantFold(materializedViewRewritten);
        verifyMetadataVersions(pinnedVersions);
        return new PlanResult(materializedViewRewritten, preOptColumnNames, filterDescriptors);
    }

    // ---- Cross-catalog metadata version pinning ----

    /**
     * Mutation predicates describe matches, not all physical rows in a replacement write.
     * Relational optimizers can substitute original columns using those predicates or
     * discard nonmatching rows. Preserve the validated mutation until security policies
     * and copy-on-write conversion have established the complete replacement relation.
     */
    private static boolean containsMutation(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.TableModify) {
            return true;
        }
        return node.getInputs().stream().anyMatch(PangrovaPlanner::containsMutation);
    }

    private Map<String, Long> pinMetadataVersions(RelNode relNode) {
        return versionPinner != null ? versionPinner.pinMetadataVersions(relNode) : Map.of();
    }

    private void verifyMetadataVersions(Map<String, Long> pinnedVersions) {
        if (versionPinner != null) {
            versionPinner.verifyMetadataVersions(pinnedVersions);
        }
    }

    /**
     * Returns true if the given RelNode is a folded constant — a LogicalValues
     * with no inputs, produced by the constant folding step in plan().
     */
    public static boolean isFoldedConstant(RelNode relNode) {
        return PlanShapeChecks.isFoldedConstant(relNode);
    }

    /**
     * Checks whether the plan is a tableless constant expression (no TableScan,
     * TableModify, or Exchange anywhere in the tree).
     */
    public static boolean isTablelessConstant(RelNode node) {
        return PlanShapeChecks.isTablelessConstant(node);
    }

    /**
     * Returns true if the plan is a LIMIT 0 sort (zero rows returned).
     * Used by the coordinator to skip worker dispatch for empty result sets.
     */
    public static boolean isZeroFetchSort(RelNode relNode) {
        return PlanShapeChecks.isZeroFetchSort(relNode);
    }

    /**
     * Returns true if the plan is a MERGE DML operation.
     * Used by the coordinator to skip split-count checks for MERGE.
     */
    public static boolean isMergeOperation(RelNode relNode) {
        return PlanShapeChecks.isMergeOperation(relNode);
    }

    /**
     * Returns true if the plan contains an UNNEST/UNCOLLECT node.
     * The native layer does not support UNNEST; the coordinator handles it locally.
     */
    public static boolean containsUnnest(RelNode node) {
        return PlanShapeChecks.containsUnnest(node);
    }

    /** Counter for constant-folded queries. Exposed for metrics collection. */
    public long getConstantFoldedTotal() {
        return constantFoldedTotal.get();
    }

    private void recordConstantFold(RelNode plan) {
        if (PlanShapeChecks.isFoldedConstant(plan)) {
            constantFoldedTotal.incrementAndGet();
        }
    }

    /** Returns the statistics collector, or null if not wired. Package-private to limit API surface. */
    StatisticsCollector getStatisticsCollector() {
        return statisticsCollector;
    }

    /** Returns the cost estimator, or null if statistics not wired. Package-private. */
    StatisticsBasedCostEstimator getCostEstimator() {
        return costEstimator;
    }

    /** Returns the query result cache, or null if not wired. */
    public QueryResultCache getQueryResultCache() {
        return queryResultCache;
    }

    // ---- DPP (Dynamic Partition Pruning) ----

    /** Returns the runtime filter registry for DPP. */
    public RuntimeFilterRegistry getRuntimeFilterRegistry() {
        return materializedViewRewritePass.getRuntimeFilterRegistry();
    }

    /** Returns the DPP configuration. */
    public DynamicPartitionPruningConfig getDynamicPartitionPruningConfig() {
        return materializedViewRewritePass.getDynamicPartitionPruningConfig();
    }

    /**
     * Analyze a plan for partition-pruning-eligible joins without registering filters.
     * Useful for testing and inspection.
     */
    public List<RuntimeFilterDescriptor> analyzeJoinsForPartitionPruning(RelNode plan) {
        return materializedViewRewritePass.analyzeJoinsForPartitionPruning(plan);
    }

    private SqlNode parse(String sql) {
        try {
            return parserFactory.parseToSqlNode(sql);
        } catch (org.pangrova.parser.ParseException e) {
            throw e;
        } catch (Exception e) {
            throw new PlannerException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Parse failed: " + e.getMessage(), e);
        }
    }

    private SqlNode validate(SqlValidator validator, SqlNode sqlNode) {
        try {
            return validator.validate(sqlNode);
        } catch (Exception e) {
            throw new PlannerException(
                    PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Validation failed: " + e.getMessage(), e);
        }
    }

    private RelNode convert(SqlValidator validator, RelOptCluster cluster, SqlNode validated) {
        try {
            SqlToRelConverter converter = new SqlToRelConverter(
                    null, validator, catalogReader, cluster,
                    StandardConvertletTable.INSTANCE,
                    SqlToRelConverter.config()
                            .addRelBuilderConfigTransform(config -> config.withBloat(-1)));
            RelNode rel = converter.convertQuery(validated, false, true).rel;
            return InlineInsertValuesNormalizer.normalize(
                    org.pangrova.planner.embedding.GeneratedColumnMutationRewrite.rewrite(
                            DecorrelationSupport.safeDecorrelate(rel), validated, operatorTable));
        } catch (Exception e) {
            throw new PlannerException(
                    PangrovaErrorCode.PLANNER_OPTIMIZATION_FAILURE,
                    "Conversion failed: " + e.getMessage(), e);
        }
    }

    /**
     * Re-optimizes an existing plan fragment using the current statistics state.
     * Called by the coordinator's AQE callback after runtime statistics have been
     * fed into the {@link StatisticsCollector} via {@code recordRuntimeFeedback}.
     *
     * <p>Re-runs the full optimization pipeline on the given plan. Failures
     * stop compilation before a new fragment is dispatched.</p>
     *
     * @param plan the existing plan fragment to re-optimize
     * @return the re-optimized plan
     */
    public RelNode reoptimize(RelNode plan) {
        return optimizationRunner.reoptimize(plan, null);
    }

    /**
     * Re-optimizes a plan fragment with an optional join strategy hint (F-PLANNER-05).
     * When {@code joinStrategyHint} is {@code BROADCAST_HASH_JOIN}, configures a low
     * broadcast threshold so that {@code BroadcastJoinRule} wins during optimization.
     *
     * @param plan the existing plan fragment to re-optimize
     * @param joinStrategyHint optional join strategy hint from AQE, or null
     * @return the re-optimized plan
     */
    public RelNode reoptimize(RelNode plan, org.pangrova.planner.adaptive.JoinStrategy joinStrategyHint) {
        return optimizationRunner.reoptimize(plan, joinStrategyHint);
    }

    private SqlValidator buildValidator() {
        return PlannerComponentFactory.buildValidator(operatorTable, catalogReader, typeFactory);
    }

    private RelOptCluster buildCluster() {
        return PlannerComponentFactory.buildCluster(
                typeFactory, costEstimator, costModelConfig, cboEnabled);
    }
}
