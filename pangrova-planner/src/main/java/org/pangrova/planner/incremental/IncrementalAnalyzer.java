package org.pangrova.planner.incremental;

import org.pangrova.planner.enums.MergeOperator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Analyzes a Dynamic Table's defining query for incrementalizability.
 *
 * <p>Two analysis modes:
 * <ul>
 *   <li>{@link #analyzeRelNode(RelNode)} — preferred, operates on the Calcite
 *       logical plan and inspects actual {@link AggregateCall} operators</li>
 *   <li>{@link #analyze(String)} — legacy fallback, uses regex on SQL text</li>
 * </ul>
 *
 * <p>A DT query is incrementalizable when all its aggregation operators can be
 * merged from partial results without re-scanning the full dataset:</p>
 * <ul>
 *   <li>{@code SUM(x)} → {@code old_sum + delta_sum}</li>
 *   <li>{@code COUNT(*)} → {@code old_count + delta_count}</li>
 *   <li>{@code MIN(x)} → {@code min(old_min, delta_min)}</li>
 *   <li>{@code MAX(x)} → {@code max(old_max, delta_max)}</li>
 *   <li>{@code AVG(x)} → decompose to {@code SUM/COUNT}, recompose</li>
 * </ul>
 *
 * <p>Non-incrementalizable operators (e.g. {@code COUNT(DISTINCT x)},
 * {@code MEDIAN}, {@code PERCENTILE}) force a full refresh fallback.</p>
 */
public final class IncrementalAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalAnalyzer.class);

    private static final Set<SqlKind> NON_INCREMENTAL_KINDS = Set.of(
            SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP,
            SqlKind.VAR_POP, SqlKind.VAR_SAMP,
            SqlKind.PERCENTILE_CONT, SqlKind.PERCENTILE_DISC);

    private static final Pattern AGG_PATTERN = Pattern.compile(
            "\\b(SUM|COUNT|MIN|MAX|AVG|COUNT\\s*\\(\\s*DISTINCT)\\s*\\(",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern NON_INCREMENTAL_PATTERN = Pattern.compile(
            "\\bCOUNT\\s*\\(\\s*DISTINCT\\b"
                    + "|\\b(?:MEDIAN|PERCENTILE_CONT|PERCENTILE_DISC"
                    + "|APPROX_COUNT_DISTINCT|STDDEV|VARIANCE|VAR_POP|VAR_SAMP"
                    + "|STDDEV_POP|STDDEV_SAMP)\\s*\\(",
            Pattern.CASE_INSENSITIVE);

    private IncrementalAnalyzer() {
    }

    /**
     * Analyze a Calcite RelNode plan for incrementalizability.
     * Walks the plan tree to find Aggregate nodes and inspects each
     * AggregateCall's SqlKind — immune to SQL formatting variations.
     *
     * @param relNode the validated Calcite logical plan
     * @return analysis result with per-column merge strategies
     */
    public static IncrementalAnalysis analyzeRelNode(RelNode relNode) {
        Objects.requireNonNull(relNode, "relNode must not be null");

        List<AggregateCall> aggCalls = new ArrayList<>();
        collectAggregateCalls(relNode, aggCalls);

        if (aggCalls.isEmpty()) {
            LOG.info("event=incremental_relnode_no_aggregation");
            return IncrementalAnalysis.incrementalizable(List.of());
        }

        List<ColumnMergeStrategy> strategies = new ArrayList<>();
        for (AggregateCall call : aggCalls) {
            SqlKind kind = call.getAggregation().getKind();

            if (call.isDistinct() || NON_INCREMENTAL_KINDS.contains(kind)) {
                String name = call.getAggregation().getName();
                String label = call.isDistinct() ? name + "(DISTINCT)" : name;
                LOG.info("event=incremental_relnode_non_incrementalizable operator={}", label);
                return IncrementalAnalysis.nonIncrementalizable(
                        "Non-incrementalizable operator: " + label);
            }

            MergeOperator mergeOp = toMergeOperatorFromKind(kind);
            if (mergeOp == null) {
                return IncrementalAnalysis.nonIncrementalizable(
                        "Unsupported operator: " + call.getAggregation().getName());
            }
            strategies.add(new ColumnMergeStrategy(
                    call.getAggregation().getName(), mergeOp));
        }

        LOG.info("event=incremental_relnode_incrementalizable strategies={}", strategies.size());
        return IncrementalAnalysis.incrementalizable(strategies);
    }

    private static void collectAggregateCalls(RelNode node, List<AggregateCall> result) {
        if (node instanceof Aggregate agg) {
            result.addAll(agg.getAggCallList());
        }
        for (RelNode input : node.getInputs()) {
            collectAggregateCalls(input, result);
        }
    }

    private static MergeOperator toMergeOperatorFromKind(SqlKind kind) {
        return switch (kind) {
            case SUM, SUM0 -> MergeOperator.SUM;
            case COUNT -> MergeOperator.SUM;
            case MIN -> MergeOperator.MIN;
            case MAX -> MergeOperator.MAX;
            case AVG -> MergeOperator.AVG_DECOMPOSE;
            default -> null;
        };
    }

    /**
     * Analyze a DT defining query for incrementalizability via SQL text regex.
     * Prefer {@link #analyzeRelNode(RelNode)} when a Calcite plan is available.
     *
     * @param queryText the SQL text of the DT's defining query
     * @return analysis result with per-column merge strategies
     */
    public static IncrementalAnalysis analyze(String queryText) {
        Objects.requireNonNull(queryText, "queryText must not be null");

        Matcher nonIncremental = NON_INCREMENTAL_PATTERN.matcher(queryText);
        if (nonIncremental.find()) {
            String operator = nonIncremental.group(0).trim().toUpperCase(Locale.ROOT);
            LOG.info("event=incremental_analysis_non_incrementalizable query={} operator={}",
                    truncate(queryText), operator);
            return IncrementalAnalysis.nonIncrementalizable(
                    "Non-incrementalizable operator: " + operator);
        }

        List<ColumnMergeStrategy> strategies = new ArrayList<>();
        Matcher aggMatcher = AGG_PATTERN.matcher(queryText);
        boolean hasAggregation = false;

        while (aggMatcher.find()) {
            hasAggregation = true;
            String operator = aggMatcher.group(1).trim().toUpperCase(Locale.ROOT);
            if (operator.startsWith("COUNT") && operator.contains("DISTINCT")) {
                continue;
            }
            MergeOperator mergeOp = toMergeOperator(operator);
            if (mergeOp == null) {
                return IncrementalAnalysis.nonIncrementalizable(
                        "Unsupported operator: " + operator);
            }
            strategies.add(new ColumnMergeStrategy(operator, mergeOp));
        }

        if (!hasAggregation) {
            LOG.info("event=incremental_analysis_no_aggregation query={}", truncate(queryText));
            return IncrementalAnalysis.incrementalizable(List.of());
        }

        LOG.info("event=incremental_analysis_incrementalizable query={} strategies={}",
                truncate(queryText), strategies.size());
        return IncrementalAnalysis.incrementalizable(strategies);
    }

    private static MergeOperator toMergeOperator(String operator) {
        return switch (operator) {
            case "SUM" -> MergeOperator.SUM;
            case "COUNT" -> MergeOperator.SUM;
            case "MIN" -> MergeOperator.MIN;
            case "MAX" -> MergeOperator.MAX;
            case "AVG" -> MergeOperator.AVG_DECOMPOSE;
            default -> null;
        };
    }

    private static String truncate(String s) {
        return s.length() > 100 ? s.substring(0, 100) + "..." : s;
    }
}
