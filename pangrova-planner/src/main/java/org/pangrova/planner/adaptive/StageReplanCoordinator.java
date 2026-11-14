package org.pangrova.planner.adaptive;

import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.api.enums.JoinStrategy;
import org.pangrova.planner.api.adaptive.ReOptimizationResult;
import org.pangrova.planner.api.adaptive.StageReplanDecision;
import org.pangrova.planner.api.adaptive.StageReplanCallback;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.Objects;

import org.pangrova.planner.statistics.CardinalityFeedbackStore;
import org.pangrova.planner.statistics.StatisticsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates the three AQE components (statistics collector, partition coalescer,
 * join strategy switcher) to enable stage replan after shuffle statistics are available.
 *
 * <p>After a shuffle stage completes, the coordinator:
 * <ol>
 *   <li>Collects partition statistics from the completed stage</li>
 *   <li>Feeds statistics to the partition coalescer to merge small partitions</li>
 *   <li>Feeds statistics to the join strategy switcher to switch from sort-merge
 *       to broadcast when the build side is small</li>
 *   <li>Produces a {@link StageReplanDecision} that downstream stages use to
 *       re-optimize before dispatching</li>
 * </ol>
 *
 * <p>This class is the single coordination point that the execution driver calls
 * after each shuffle stage completes, replacing ad-hoc calls to individual AQE
 * components.</p>
 */
public class StageReplanCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(StageReplanCoordinator.class);

    private final PartitionCoalescer coalescer;
    private final AdaptiveQueryExecutor queryExecutor;
    private final StatisticsCollector statisticsCollector;
    private final CardinalityFeedbackStore feedbackStore;
    private final AdaptiveQueryConfig config;
    private final StageReplanCallback callback;

    /**
     * Creates a coordinator with all AQE components, cardinality feedback, and a replan callback.
     *
     * @param coalescer           merges small partitions based on target size
     * @param queryExecutor       evaluates join strategy switches and re-optimization triggers
     * @param statisticsCollector records runtime cardinality feedback for future planning
     * @param feedbackStore       EMA-based cardinality correction store; may be null
     * @param config              AQE configuration (thresholds, enabled flag)
     * @param callback            callback invoked after producing a replan decision; may be null
     */
    public StageReplanCoordinator(PartitionCoalescer coalescer,
                                  AdaptiveQueryExecutor queryExecutor,
                                  StatisticsCollector statisticsCollector,
                                  CardinalityFeedbackStore feedbackStore,
                                  AdaptiveQueryConfig config,
                                  StageReplanCallback callback) {
        this.coalescer = Objects.requireNonNull(coalescer, "coalescer");
        this.queryExecutor = Objects.requireNonNull(queryExecutor, "queryExecutor");
        this.statisticsCollector = Objects.requireNonNull(statisticsCollector, "statisticsCollector");
        this.feedbackStore = feedbackStore;
        this.config = Objects.requireNonNull(config, "config");
        this.callback = callback;
    }

    public StageReplanCoordinator(PartitionCoalescer coalescer,
                                  AdaptiveQueryExecutor queryExecutor,
                                  StatisticsCollector statisticsCollector,
                                  AdaptiveQueryConfig config,
                                  StageReplanCallback callback) {
        this(coalescer, queryExecutor, statisticsCollector, null, config, callback);
    }

    public StageReplanCoordinator(PartitionCoalescer coalescer,
                                  AdaptiveQueryExecutor queryExecutor,
                                  StatisticsCollector statisticsCollector,
                                  AdaptiveQueryConfig config) {
        this(coalescer, queryExecutor, statisticsCollector, null, config, null);
    }

    /**
     * Callback invoked after a shuffle stage completes. Collects partition statistics,
     * feeds them to the coalescer and join strategy switcher, and produces a replan
     * decision for downstream stages.
     *
     * <p>When AQE is disabled, returns {@link StageReplanDecision#NO_REPLAN} immediately.</p>
     *
     * @param stats               partition statistics from the completed shuffle stage
     * @param currentJoinStrategy the current join strategy for the downstream join
     * @param tableName           optional table name for cardinality feedback recording;
     *                            null skips feedback
     * @return a decision containing coalesce result, join strategy change, and
     *         whether re-optimization was triggered
     */
    public StageReplanDecision onShuffleStageCompleted(ShuffleStageStats stats,
                                                       JoinStrategy currentJoinStrategy,
                                                       QualifiedTableName tableName) {
        Objects.requireNonNull(stats, "stats");
        Objects.requireNonNull(currentJoinStrategy, "currentJoinStrategy");

        if (!config.enabled()) {
            return StageReplanDecision.NO_REPLAN;
        }

        // Step 1: Evaluate partition coalescing
        CoalesceResult coalesceResult = coalescer.evaluate(stats, config);

        // Step 2: Evaluate join strategy switch and re-optimization trigger
        ReOptimizationResult reOptResult = queryExecutor.processStageCompletion(
                stats, currentJoinStrategy, config.broadcastThresholdBytes());

        // Step 3: Record runtime cardinality feedback (independent of re-optimization)
        if (tableName != null && stats.estimatedRowCount() > 0 && stats.actualRowCount() > 0) {
            statisticsCollector.recordRuntimeFeedback(
                    tableName, stats.estimatedRowCount(), stats.actualRowCount());
            if (feedbackStore != null) {
                feedbackStore.record(tableName.canonicalKey(), stats.estimatedRowCount(), stats.actualRowCount());
            }
        }

        // Step 4: Determine optimal parallelism based on actual data size
        int recommendedParallelism = queryExecutor.evaluateParallelism(stats, 0);

        boolean needsReplan = coalesceResult.isCoalesced() || reOptResult.triggered();

        StageReplanDecision decision = new StageReplanDecision(
                needsReplan,
                coalesceResult,
                reOptResult.newJoinStrategy(),
                reOptResult.triggered(),
                reOptResult.reason(),
                recommendedParallelism);

        notifyCallback(stats.stageId(), decision);

        return decision;
    }

    /**
     * Simplified callback when no table name is available for cardinality feedback.
     */
    public StageReplanDecision onShuffleStageCompleted(ShuffleStageStats stats,
                                                       JoinStrategy currentJoinStrategy) {
        return onShuffleStageCompleted(stats, currentJoinStrategy, null);
    }

    private void notifyCallback(String stageId, StageReplanDecision decision) {
        if (callback == null) {
            return;
        }
        try {
            callback.onReplanDecision(stageId, decision);
        } catch (RuntimeException e) {
            LOG.error("event=stage_replan_callback_failed stageId={} needsReplan={}",
                    stageId, decision.needsReplan(), e);
        }
    }
}
