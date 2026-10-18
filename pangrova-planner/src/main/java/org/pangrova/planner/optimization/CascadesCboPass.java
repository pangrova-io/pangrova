package org.pangrova.planner.optimization;

import org.pangrova.spi.connector.metadata.table.QualifiedTableName;

import org.pangrova.planner.PlanTable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.join.BroadcastJoinRule;
import org.pangrova.planner.cost.join.HashJoinRule;
import org.pangrova.planner.cost.join.SortMergeJoinRule;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.CancelFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cost-based optimization pass using Calcite's VolcanoPlanner (Cascades framework).
 * Applies join reorder (commute + associate) and join algorithm selection rules.
 * Falls back to O(n²) greedy join reorder when the join count exceeds the
 * Volcano threshold. Only applicable when CBO is enabled and the plan has ≥2 joins.
 */
public final class CascadesCboPass implements OptimizationPass {

    private static final Logger LOG = LoggerFactory.getLogger(CascadesCboPass.class);

    @Override
    public RelNode optimize(RelNode relNode, OptimizationContext context) {
        CboConfig cboConfig = context.cboConfig();
        int joinCount = countJoins(relNode);
        try {
            if (joinCount > cboConfig.volcanoJoinThreshold()) {
                return greedyJoinReorder(relNode, context);
            }
            return cascadesOptimize(relNode, context);
        } catch (Exception | AssertionError e) {
            // Returning the logical input here can silently remove required
            // exchanges and execute a distributed query as a single stage.
            throw new IllegalStateException("CBO failed for " + joinCount + " joins", e);
        }
    }

    @Override
    public String name() {
        return "CascadesCbo";
    }

    @Override
    public boolean isApplicable(RelNode relNode, OptimizationContext context) {
        return context.cboConfig().enabled()
                && countJoins(relNode) >= 2;
    }

    /**
     * Cascades-style CBO using Calcite VolcanoPlanner with join reorder rules.
     * Stage 1: quick search with commute + associate rules and join algorithm selection.
     * Stage 2: bushy tree exploration when Stage 1 cost exceeds threshold.
     *
     * <p>Uses cooperative cancellation via {@link CancelFlag} checked by VolcanoPlanner
     * at rule application boundaries. On timeout the flag is set and the current rule
     * completes gracefully before optimization returns.</p>
     */
    private RelNode cascadesOptimize(RelNode input, OptimizationContext context) {
        CboConfig cboConfig = context.cboConfig();
        VolcanoPlanner volcano = createVolcanoPlanner(input, context);
        CancelFlag cancelFlag = volcano.getContext().unwrap(CancelFlag.class);
        if (cancelFlag == null) {
            throw new IllegalStateException("CBO planner must have a cancellation flag");
        }
        AtomicBoolean cancelledFlag = cancelFlag.atomicBoolean;
        cancelledFlag.set(false);
        RelNode stage1Result = runVolcanoWithTimeout(
                volcano, cboConfig.stage1TimeoutMs(), 1, cancelledFlag);
        if (stage1Result == input) {
            return input;
        }
        if (shouldRunStage2(volcano, stage1Result, cboConfig)) {
            cancelledFlag.set(false);
            volcano.addRule(CoreRules.JOIN_TO_MULTI_JOIN);
            volcano.addRule(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);
            return runVolcanoWithTimeout(
                    volcano, cboConfig.stage2TimeoutMs(), 2, cancelledFlag);
        }
        return stage1Result;
    }

    private static final List<RelOptRule> STATIC_RULES = List.of(
            CoreRules.JOIN_COMMUTE,
            // Reassociation changes predicates and inputs. Physical join copies would
            // retain an obsolete algorithm and distribution after that transformation.
            CoreRules.JOIN_ASSOCIATE.config.withOperandFor(LogicalJoin.class).toRule(),
            new BroadcastJoinRule(),
            new SortMergeJoinRule()
    );

    private static final ConcurrentHashMap<CostModelConfig, List<RelOptRule>> RULE_CACHE =
            new ConcurrentHashMap<>();

    private static List<RelOptRule> cachedRules(CostModelConfig costModelConfig) {
        return RULE_CACHE.computeIfAbsent(costModelConfig, config -> {
            List<RelOptRule> rules = new ArrayList<>(STATIC_RULES.size() + 1);
            rules.addAll(STATIC_RULES);
            rules.add(new HashJoinRule(config));
            return List.copyOf(rules);
        });
    }

    private static VolcanoPlanner createVolcanoPlanner(
            RelNode input, OptimizationContext context) {
        // RelOptCluster owns its planner. Registering its nodes in a second
        // VolcanoPlanner fails and silently leaves a single-stage logical plan.
        if (!(input.getCluster().getPlanner() instanceof VolcanoPlanner volcano)) {
            throw new IllegalStateException("CBO requires the query's VolcanoPlanner");
        }
        for (RelOptRule rule : cachedRules(context.costModelConfig())) {
            volcano.addRule(rule);
        }
        volcano.setRoot(volcano.register(input, null));
        return volcano;
    }

    private static boolean shouldRunStage2(
            VolcanoPlanner volcano, RelNode stage1Result, CboConfig cboConfig) {
        try {
            RelOptCost bestCost = volcano.getCost(
                    stage1Result, stage1Result.getCluster().getMetadataQuery());
            if (bestCost == null || bestCost.isInfinite()) {
                return true;
            }
            return bestCost.getRows() + bestCost.getCpu() + bestCost.getIo()
                    > cboConfig.stage2CostThreshold();
        } catch (Exception e) {
            LOG.debug("event=stage2_cost_check_failed error={}", e.getMessage());
            return false;
        }
    }

    /**
     * Runs VolcanoPlanner with a cooperative timeout. A watchdog thread sets the
     * {@code cancelledFlag} on timeout; VolcanoPlanner checks this flag at each
     * rule application boundary and stops gracefully, allowing the current rule
     * to complete before returning the best plan found so far.
     */
    private static RelNode runVolcanoWithTimeout(
            VolcanoPlanner volcano, long timeoutMs, int stage,
            AtomicBoolean cancelledFlag) {
        CountDownLatch optimizationDone = new CountDownLatch(1);
        ScheduledExecutorService watchdog = createCboWatchdog(stage);
        ScheduledFuture<?> timeout = watchdog.schedule(() -> {
            LOG.warn("event=cbo_timeout stage={} timeoutMs={} action=cooperative_cancel",
                    stage, timeoutMs);
            cancelledFlag.set(true);
            try {
                if (!optimizationDone.await(
                        GRACEFUL_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    LOG.warn("event=cbo_graceful_wait_exceeded stage={}", stage);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
        try {
            RelNode result = volcano.findBestExp();
            LOG.info("event=cbo_complete stage={}", stage);
            return result;
        } catch (RelOptPlanner.CannotPlanException e) {
            // The root may be a RelSubset, not an executable RelNode. A
            // cancelled or unplannable search must never dispatch it.
            throw new IllegalStateException(
                    "CBO could not produce a plan at stage " + stage
                            + (cancelledFlag.get() ? " (cancelled)" : ""), e);
        } finally {
            optimizationDone.countDown();
            timeout.cancel(false);
        }
    }

    /** Maximum time to wait for the current rule to complete after cancellation. */
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 500;

    /** Shared CBO watchdog executor. Daemon threads, remove-on-cancel for GC friendliness. */
    private static final ScheduledExecutorService CBO_WATCHDOG_EXECUTOR;

    static {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cbo-watchdog");
            t.setDaemon(true);
            return t;
        });
        if (exec instanceof java.util.concurrent.ScheduledThreadPoolExecutor pool) {
            pool.setRemoveOnCancelPolicy(true);
        }
        CBO_WATCHDOG_EXECUTOR = exec;
    }

    private static ScheduledExecutorService createCboWatchdog(int stage) {
        return CBO_WATCHDOG_EXECUTOR;
    }

    /**
     * O(n²) greedy join reorder for queries exceeding the Volcano join threshold.
     * Extracts leaf relations and join predicates, then iteratively joins the pair
     * with lowest estimated cardinality product, adjusted by index scan cost reduction.
     */
    private static RelNode greedyJoinReorder(RelNode input, OptimizationContext context) {
        List<RelNode> relations = new ArrayList<>();
        List<RexNode> conditionPool = new ArrayList<>();
        extractJoinGraph(input, relations, conditionPool);
        int n = relations.size();
        LOG.info("event=greedy_join_reorder relations={}", n);
        if (n < 2) {
            return input;
        }
        RelNode result = buildGreedyJoinTree(input, relations, conditionPool, context);
        LOG.info("event=greedy_join_reorder_complete result_joins={}",
                countJoins(result));
        return result;
    }

    private static RelNode buildGreedyJoinTree(
            RelNode input, List<RelNode> relations, List<RexNode> conditionPool,
            OptimizationContext context) {
        RelMetadataQuery mq = input.getCluster().getMetadataQuery();
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        List<RelNode> remaining = new ArrayList<>(relations);

        // Track which original field-index ranges each remaining entry owns.
        // Conditions use RexInputRef indices relative to the original flattened
        // leaf order; index-based matching avoids the name-collision bug (F-PLANNER-01).
        List<Set<Integer>> fieldRanges = new ArrayList<>();
        int offset = 0;
        for (RelNode rel : relations) {
            Set<Integer> range = new HashSet<>();
            int count = rel.getRowType().getFieldCount();
            for (int i = offset; i < offset + count; i++) {
                range.add(i);
            }
            fieldRanges.add(range);
            offset += count;
        }

        while (remaining.size() > 1) {
            JoinPairSelection best = findBestPair(remaining, mq, context);
            int bestLeft = best.leftIdx();
            int bestRight = best.rightIdx();
            RelNode left = remaining.get(bestLeft);
            RelNode right = remaining.get(bestRight);
            Set<Integer> leftRange = fieldRanges.get(bestLeft);
            Set<Integer> rightRange = fieldRanges.get(bestRight);
            RexNode cond = matchJoinCondition(leftRange, rightRange, conditionPool, rexBuilder);

            // F-PLANNER-06: If condition is TRUE (cross join) and other pairs have
            // actual join predicates, skip this pair and try another with real conditions.
            if (cond.isAlwaysTrue() && remaining.size() > 2) {
                JoinPairOverride override = findConditionedPairOverride(
                        bestLeft, bestRight, remaining, fieldRanges, conditionPool, rexBuilder);
                if (override != null) {
                    bestLeft = override.leftIdx();
                    bestRight = override.rightIdx();
                    left = remaining.get(bestLeft);
                    right = remaining.get(bestRight);
                    leftRange = fieldRanges.get(bestLeft);
                    rightRange = fieldRanges.get(bestRight);
                    cond = override.condition();
                }
            }

            RelNode joined = LogicalJoin.create(
                    left, right, List.of(), cond, Set.of(), JoinRelType.INNER);
            Set<Integer> mergedRange = new HashSet<>(leftRange);
            mergedRange.addAll(rightRange);
            remaining.remove(bestRight);
            fieldRanges.remove(bestRight);
            remaining.remove(bestLeft);
            fieldRanges.remove(bestLeft);
            remaining.add(joined);
            fieldRanges.add(mergedRange);
        }
        return remaining.get(0);
    }

    /**
     * Result of selecting the best join pair from the remaining relations.
     */
    private record JoinPairSelection(int leftIdx, int rightIdx, double cost) {}

    /**
     * Override result when a conditioned pair is found to avoid a cross-join.
     */
    private record JoinPairOverride(int leftIdx, int rightIdx, RexNode condition) {}

    /**
     * Searches for a pair of relations that has an actual join predicate (not cross-join),
     * to override the cost-optimal pair that would produce a cross-join.
     *
     * @return the overriding pair with its join condition, or null if no conditioned pair exists
     */
    private static JoinPairOverride findConditionedPairOverride(
            int skipLeft, int skipRight, List<RelNode> remaining,
            List<Set<Integer>> fieldRanges, List<RexNode> conditionPool,
            RexBuilder rexBuilder) {
        for (int i = 0; i < remaining.size(); i++) {
            for (int j = i + 1; j < remaining.size(); j++) {
                if (i == skipLeft && j == skipRight) continue;
                Set<Integer> lr = fieldRanges.get(i);
                Set<Integer> rr = fieldRanges.get(j);
                RexNode testCond = matchJoinCondition(lr, rr, new ArrayList<>(conditionPool), rexBuilder);
                if (!testCond.isAlwaysTrue()) {
                    RexNode cond = matchJoinCondition(lr, rr, conditionPool, rexBuilder);
                    return new JoinPairOverride(i, j, cond);
                }
            }
        }
        return null;
    }

    /**
     * Selects the pair with lowest cost. When an {@link IndexCostFactor} is available,
     * each relation's row count is multiplied by the index cost multiplier for its
     * filter columns, so indexed tables appear cheaper and are preferred as the
     * inner (right) side of joins.
     */
    private static JoinPairSelection findBestPair(List<RelNode> remaining, RelMetadataQuery mq,
                                      OptimizationContext context) {
        double bestCost = Double.MAX_VALUE;
        int bestI = 0;
        int bestJ = 1;
        for (int i = 0; i < remaining.size(); i++) {
            for (int j = i + 1; j < remaining.size(); j++) {
                double costI = adjustedRowCount(remaining.get(i), mq, context);
                double costJ = adjustedRowCount(remaining.get(j), mq, context);
                double cost = costI * costJ;
                if (cost < bestCost) {
                    bestCost = cost;
                    bestI = i;
                    bestJ = j;
                }
            }
        }
        return new JoinPairSelection(bestI, bestJ, bestCost);
    }

    /**
     * Returns the row count for a relation, reduced by the index cost multiplier
     * when index metadata is available. Extracts filter predicate columns from
     * Filter nodes above the scan. For non-leaf nodes (joins) or nodes without
     * filters, returns the raw row count estimate.
     */
    private static double adjustedRowCount(RelNode node, RelMetadataQuery mq,
                                           OptimizationContext context) {
        double rows = mq.getRowCount(node);
        if (context == null || context.indexCostFactor() == null) {
            return rows;
        }
        if (node instanceof Join) {
            return rows;
        }
        Set<String> filterColumns = extractFilterColumns(node);
        if (filterColumns.isEmpty()) {
            return rows;
        }
        QualifiedTableName tableName = resolveTableNameFromScan(node);
        if (tableName == null) {
            return rows;
        }
        double multiplier = context.indexCostFactor().computeCostMultiplier(tableName, filterColumns);
        return rows * multiplier;
    }

    /**
     * Extracts column names referenced in filter predicates by walking down
     * through single-input nodes (Project, Filter) to find Filter conditions.
     */
    private static Set<String> extractFilterColumns(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.Filter filter) {
            Set<String> cols = new HashSet<>();
            RelNode input = filter.getInput();
            for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
                conjunct.accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
                        int idx = inputRef.getIndex();
                        if (idx < input.getRowType().getFieldCount()) {
                            cols.add(input.getRowType().getFieldNames().get(idx));
                        }
                        return null;
                    }
                });
            }
            return cols;
        }
        if (node.getInputs().size() == 1) {
            return extractFilterColumns(node.getInput(0));
        }
        return Set.of();
    }

    private static QualifiedTableName resolveTableNameFromScan(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.TableScan scan) {
            var parts = PlanTable.require(scan).getQualifiedName();
            return parts.size() == 3 ? new QualifiedTableName(parts.get(0), parts.get(1), parts.get(2)) : null;
        }
        if (node.getInputs().size() == 1) {
            return resolveTableNameFromScan(node.getInput(0));
        }
        return null;
    }

    private static void extractJoinGraph(
            RelNode node, List<RelNode> relations, List<RexNode> conditions) {
        if (node instanceof Join join) {
            extractJoinGraph(join.getLeft(), relations, conditions);
            extractJoinGraph(join.getRight(), relations, conditions);
            if (join.getCondition() != null && !join.getCondition().isAlwaysTrue()) {
                conditions.addAll(RelOptUtil.conjunctions(join.getCondition()));
            }
        } else {
            relations.add(node);
        }
    }

    /**
     * Finds join conditions whose referenced RexInputRef indices span both left
     * and right field-index ranges. Matched conditions are consumed from the pool.
     * Returns TRUE literal when no condition matches (cross join).
     *
     * <p>Uses index-based matching (not name-based) to avoid the F-PLANNER-01 bug
     * where overlapping field names across tables caused all conditions to be missed.</p>
     */
    private static RexNode matchJoinCondition(
            Set<Integer> leftRange, Set<Integer> rightRange,
            List<RexNode> conditionPool, RexBuilder rexBuilder) {
        List<RexNode> matched = new ArrayList<>();
        var it = conditionPool.iterator();
        while (it.hasNext()) {
            RexNode cond = it.next();
            Set<Integer> refs = collectInputRefIndices(cond);
            boolean touchesLeft = refs.stream().anyMatch(leftRange::contains);
            boolean touchesRight = refs.stream().anyMatch(rightRange::contains);
            if (touchesLeft && touchesRight) {
                matched.add(cond);
                it.remove();
            }
        }
        if (matched.isEmpty()) {
            return rexBuilder.makeLiteral(true);
        }
        return RexUtil.composeConjunction(rexBuilder, matched);
    }

    private static Set<Integer> collectInputRefIndices(RexNode node) {
        Set<Integer> indices = new HashSet<>();
        node.accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(org.apache.calcite.rex.RexInputRef inputRef) {
                indices.add(inputRef.getIndex());
                return null;
            }
        });
        return indices;
    }

    static int countJoins(RelNode node) {
        int count = node instanceof Join ? 1 : 0;
        for (RelNode child : node.getInputs()) {
            count += countJoins(child);
        }
        return count;
    }
}
