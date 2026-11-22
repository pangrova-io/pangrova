package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.protocol.task.CanonicalTaskRequests;

/** Selects ranges exclusively from the finalized input of a partition-local live stage. */
final class CoalescedShuffleInputs {
    private final AdaptiveQueryConfig config;
    private final PartitionCoalescer coalescer = new PartitionCoalescer();

    CoalescedShuffleInputs(AdaptiveQueryConfig config) {
        this.config = config;
    }

    CoalesceResult select(Stage stage, StageDAG dag) {
        var plan = stage.plan();
        if (config == null || !config.enabled() || plan.stagePlan().mode() != StagePlanMode.LIVE_QUERY
                || !plan.taskType().equals("COMPUTE") || plan.parallelism() < 2) {
            return CoalesceResult.NO_OP;
        }
        var output = dag.findDownstreamEdge(stage.stageId());
        if (output == null || (output.shuffleType() != ShuffleType.HASH
                && output.shuffleType() != ShuffleType.ROUND_ROBIN && output.shuffleType() != ShuffleType.SINGLETON)) {
            return CoalesceResult.NO_OP;
        }
        var upstream = dag.edges().getOrDefault(stage.stageId(), java.util.List.of());
        if (upstream.size() != 1) {
            return CoalesceResult.NO_OP;
        }
        var producer = dag.findStage(upstream.get(0));
        if (producer == null || producer.shuffleHandle() == null) {
            return CoalesceResult.NO_OP;
        }
        var input = dag.findDownstreamEdge(producer.stageId());
        if (input == null || input.downstreamStageId() != stage.stageId() || !input.isHashShuffle()
                || !input.requireComplete() || input.partitionCount() != plan.parallelism()
                || !plan.stagePlan().requireLiveFragment()
                        .supportsCoalescedHashInput(input.exchangeId(), input.distributionKeys())) {
            return CoalesceResult.NO_OP;
        }
        return coalescer.evaluate(producer.requireShuffleReceipt(), input.partitionCount(), config,
                CanonicalTaskRequests.MAX_EXTERNAL_PARTITIONS_PER_INPUT);
    }
}
