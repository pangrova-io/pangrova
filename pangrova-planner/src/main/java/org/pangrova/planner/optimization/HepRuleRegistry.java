package org.pangrova.planner.optimization;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Registry of standard optimization rules for the Heuristic (HEP) planner.
 *
 * <p>Organizes 60+ rules into logical categories and provides methods to register
 * them into a {@link HepProgramBuilder}. Rules are applied in a fixed order that
 * respects logical dependencies (e.g., decorrelation before filter pushdown,
 * expression reduction before project removal).
 *
 * <p>Rules that cause infinite loops in HEP (JOIN_COMMUTE, JOIN_ASSOCIATE) are
 * excluded — they belong in the cost-based Cascades/Volcano planner.
 */
public final class HepRuleRegistry {

    private HepRuleRegistry() {
        // utility class
    }

    /**
     * Registers all standard HEP rules into the given program builder.
     * Call order determines rule application order in the HEP planner.
     */
    public static void registerAll(HepProgramBuilder builder) {
        registerSubqueryDecorrelation(builder);
        registerExpressionReduction(builder);
        registerProjectRules(builder);
        registerFilterRules(builder);
        registerJoinRules(builder);
        registerAggregateRules(builder);
        registerSortRules(builder);
        registerSetOperationRules(builder);
        registerCalcRules(builder);
        registerWindowRules(builder);
        registerExchangeRules(builder);
        registerCorrelateRules(builder);
    }

    /**
     * Returns the total count of distinct rules registered by {@link #registerAll}.
     */
    public static int totalRuleCount() {
        return allRules().size();
    }

    /**
     * Returns an unmodifiable list of all rules in registration order.
     */
    public static List<RelOptRule> allRules() {
        List<RelOptRule> rules = new ArrayList<>();
        rules.addAll(subqueryDecorrelationRules());
        rules.addAll(expressionReductionRules());
        rules.addAll(projectRules());
        rules.addAll(filterRules());
        rules.addAll(joinRules());
        rules.addAll(aggregateRules());
        rules.addAll(sortRules());
        rules.addAll(setOperationRules());
        rules.addAll(calcRules());
        rules.addAll(windowRules());
        rules.addAll(exchangeRules());
        rules.addAll(correlateRules());
        return Collections.unmodifiableList(rules);
    }

    // ---- Subquery Decorrelation (3 rules) ----

    public static void registerSubqueryDecorrelation(HepProgramBuilder builder) {
        for (RelOptRule rule : subqueryDecorrelationRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> subqueryDecorrelationRules() {
        return List.of(
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
        );
    }

    // ---- Expression Reduction ----

    public static void registerExpressionReduction(HepProgramBuilder builder) {
        for (RelOptRule rule : expressionReductionRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> expressionReductionRules() {
        return List.of(
                CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                CoreRules.FILTER_REDUCE_EXPRESSIONS,
                CoreRules.CALC_REDUCE_EXPRESSIONS,
                CoreRules.JOIN_REDUCE_EXPRESSIONS,
                CoreRules.WINDOW_REDUCE_EXPRESSIONS,
                CoreRules.PROJECT_FILTER_VALUES_MERGE,
                CoreRules.FILTER_VALUES_MERGE,
                CoreRules.PROJECT_VALUES_MERGE,
                org.apache.calcite.rel.rules.PruneEmptyRules.PROJECT_INSTANCE
        );
    }

    // ---- Project Rules (5 rules) ----

    public static void registerProjectRules(HepProgramBuilder builder) {
        for (RelOptRule rule : projectRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> projectRules() {
        return List.of(
                CoreRules.PROJECT_MERGE.config.withOperandSupplier(top -> top.operand(Project.class)
                        .oneInput(bottom -> bottom.operand(Project.class)
                                .predicate(HepRuleRegistry::stableProjection).anyInputs()))
                        .as(ProjectMergeRule.Config.class).toRule(),
                CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_FILTER_TRANSPOSE,
                CoreRules.PROJECT_JOIN_TRANSPOSE,
                CoreRules.PROJECT_SET_OP_TRANSPOSE
        );
    }

    // ---- Filter Rules (7 rules) ----

    public static void registerFilterRules(HepProgramBuilder builder) {
        for (RelOptRule rule : filterRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> filterRules() {
        return List.of(
                CoreRules.FILTER_MERGE,
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.FILTER_PROJECT_TRANSPOSE.config.withOperandFor(Filter.class,
                        filter -> !RexUtil.containsCorrelation(filter.getCondition()),
                        Project.class, HepRuleRegistry::stableProjection).toRule(),
                CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                CoreRules.FILTER_SET_OP_TRANSPOSE,
                CoreRules.FILTER_SCAN,
                CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE
        );
    }

    /** Substitution must preserve the producer slot and evaluation domain of volatile calls. */
    private static boolean stableProjection(Project project) {
        return project.getProjects().stream().allMatch(RexUtil::isDeterministic);
    }

    // ---- Join Rules (4 rules) ----
    // Unrestricted JOIN_COMMUTE and JOIN_ASSOCIATE can loop in HEP.
    // JOIN_EXTRACT_FILTER is excluded: it extracts the join condition into a Filter
    // above the Join, creating a cross join. This conflicts with FILTER_INTO_JOIN and
    // breaks Substrait conversion (the JoinRel loses its equi-condition). The rule is
    // designed for Volcano/Cascades planners; the CBO pass handles join reordering.

    public static void registerJoinRules(HepProgramBuilder builder) {
        for (RelOptRule rule : joinRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> joinRules() {
        return List.of(
                org.pangrova.planner.rule.LiteralJoinKeyFilterRule.INSTANCE,
                // Re-inference across a literal input can repeatedly wrap the same Join with filters in HEP.
                // The complete literal domain is handled by LiteralJoinKeyFilterRule.
                JoinPushTransitivePredicatesRule.Config.DEFAULT.withOperandSupplier(b -> b.operand(Join.class)
                        .predicate(join -> !isLiteralRelation(join.getLeft()) && !isLiteralRelation(join.getRight()))
                        .anyInputs()).as(JoinPushTransitivePredicatesRule.Config.class).toRule(),
                CoreRules.JOIN_CONDITION_PUSH,
                CoreRules.JOIN_PUSH_EXPRESSIONS,
                CoreRules.JOIN_TO_SEMI_JOIN
        );
    }

    private static boolean isUnconditionedInnerJoin(LogicalJoin join) {
        return join.getJoinType() == JoinRelType.INNER && join.getCondition().isAlwaysTrue()
                && join.getVariablesSet().isEmpty() && join.getHints().isEmpty();
    }

    private static boolean canReassociateInnerJoin(LogicalJoin join) {
        return join.getJoinType() == JoinRelType.INNER && !join.getCondition().isAlwaysTrue()
                && join.getVariablesSet().isEmpty() && join.getHints().isEmpty()
                && RexUtil.isDeterministic(join.getCondition())
                && !RexUtil.containsCorrelation(join.getCondition());
    }

    private static boolean isLiteralRelation(RelNode node) {
        if (node instanceof HepRelVertex vertex) {
            return isLiteralRelation(vertex.getCurrentRel());
        }
        if (node instanceof Project || node instanceof Filter || node instanceof Calc) {
            return isLiteralRelation(node.getInput(0));
        }
        return node instanceof Values;
    }

    // ---- Aggregate Rules (7 rules) ----

    public static void registerAggregateRules(HepProgramBuilder builder) {
        for (RelOptRule rule : aggregateRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> aggregateRules() {
        // AGGREGATE_EXPAND_DISTINCT_AGGREGATES excluded: it rewrites single-DISTINCT
        // + non-DISTINCT aggregates into GROUPING SETS with GROUPING() calls that
        // the native engine cannot execute. The native engine handles DISTINCT invocation natively via
        // AggregationNode's per-measure distinct and BOOLEAN masks.
        return List.of(
                CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
                CoreRules.AGGREGATE_PROJECT_MERGE,
                CoreRules.AGGREGATE_REMOVE,
                CoreRules.AGGREGATE_JOIN_TRANSPOSE,
                CoreRules.AGGREGATE_UNION_AGGREGATE,
                CoreRules.AGGREGATE_STAR_TABLE
        );
    }

    // ---- Sort Rules (5 rules) ----

    public static void registerSortRules(HepProgramBuilder builder) {
        for (RelOptRule rule : sortRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> sortRules() {
        return List.of(
                CoreRules.SORT_REMOVE,
                CoreRules.SORT_PROJECT_TRANSPOSE,
                CoreRules.SORT_UNION_TRANSPOSE,
                CoreRules.SORT_JOIN_TRANSPOSE,
                CoreRules.LIMIT_MERGE,
                org.pangrova.planner.rule.LimitPushdownRule.INSTANCE
        );
    }

    // ---- Set Operation Rules (6 rules) ----

    public static void registerSetOperationRules(HepProgramBuilder builder) {
        for (RelOptRule rule : setOperationRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> setOperationRules() {
        return List.of(
                CoreRules.UNION_MERGE,
                CoreRules.UNION_REMOVE,
                CoreRules.UNION_PULL_UP_CONSTANTS,
                CoreRules.UNION_TO_DISTINCT,
                CoreRules.INTERSECT_TO_DISTINCT,
                CoreRules.MINUS_MERGE
        );
    }

    // ---- Calc Rules (2 rules) ----

    public static void registerCalcRules(HepProgramBuilder builder) {
        for (RelOptRule rule : calcRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> calcRules() {
        return List.of(
                CoreRules.CALC_MERGE,
                CoreRules.CALC_REMOVE
        );
    }

    // ---- Window Rules (1 rule) ----

    public static void registerWindowRules(HepProgramBuilder builder) {
        for (RelOptRule rule : windowRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> windowRules() {
        return List.of(
                CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
        );
    }

    // ---- Exchange Rules (2 rules) ----

    public static void registerExchangeRules(HepProgramBuilder builder) {
        for (RelOptRule rule : exchangeRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> exchangeRules() {
        return List.of(
                CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
                CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS
        );
    }

    // ---- Correlate and late join rules ----

    public static void registerCorrelateRules(HepProgramBuilder builder) {
        for (RelOptRule rule : correlateRules()) {
            builder.addRuleInstance(rule);
        }
    }

    static List<RelOptRule> correlateRules() {
        return List.of(
                CoreRules.PROJECT_CORRELATE_TRANSPOSE,
                CoreRules.FILTER_CORRELATE,
                // FILTER_CORRELATE can expose ordinary predicates after the main filter rules.
                CoreRules.FILTER_INTO_JOIN.config.withDescription("FilterIntoLateCartesianJoin")
                        .withOperandSupplier(top -> top.operand(Filter.class)
                                .oneInput(input -> input.operand(LogicalJoin.class)
                                        .predicate(HepRuleRegistry::isUnconditionedInnerJoin).anyInputs()))
                        .as(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.class).toRule(),
                // A successful application removes one cartesian join, so there is no reverse match.
                JoinAssociateRule.Config.DEFAULT.withAllowAlwaysTrueCondition(false)
                        .withOperandSupplier(top -> top.operand(LogicalJoin.class)
                                .predicate(HepRuleRegistry::canReassociateInnerJoin)
                                .inputs(left -> left.operand(LogicalJoin.class)
                                                .predicate(HepRuleRegistry::isUnconditionedInnerJoin).anyInputs(),
                                        right -> right.operand(RelNode.class).anyInputs()))
                        .as(JoinAssociateRule.Config.class).toRule()
        );
    }
}
