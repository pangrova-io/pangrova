package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/** Selects ranges exclusively from the finalized inputs of a partition-local live stage. */
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
        var upstream = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (upstream.size() == 1) {
            return selectSingle(stage, dag, upstream.get(0));
        }
        return upstream.size() == 2 ? selectJoin(stage, dag, upstream) : CoalesceResult.NO_OP;
    }

    private CoalesceResult selectSingle(Stage stage, StageDAG dag, int producerId) {
        var producer = dag.findStage(producerId);
        var edge = eligibleInput(stage, dag, producer);
        if (edge == null || !stage.plan().stagePlan().requireLiveFragment()
                .supportsCoalescedHashInput(edge.exchangeId(), edge.distributionKeys())) {
            return CoalesceResult.NO_OP;
        }
        return coalescer.evaluate(producer.requireShuffleReceipt(), edge.partitionCount(), config,
                CanonicalTaskRequests.MAX_EXTERNAL_PARTITIONS_PER_INPUT);
    }

    private CoalesceResult selectJoin(Stage stage, StageDAG dag, List<Integer> upstream) {
        var exchanges = new HashMap<String, List<Integer>>();
        var receipts = new ArrayList<ShuffleCommitReceipt>(upstream.size());
        for (int producerId : upstream) {
            var producer = dag.findStage(producerId);
            var edge = eligibleInput(stage, dag, producer);
            if (edge == null || exchanges.put(edge.exchangeId(), edge.distributionKeys()) != null) {
                return CoalesceResult.NO_OP;
            }
            receipts.add(producer.requireShuffleReceipt());
        }
        int partitions = stage.plan().parallelism();
        if (!stage.plan().stagePlan().requireLiveFragment().supportsCoalescedHashJoin(exchanges, partitions)) {
            return CoalesceResult.NO_OP;
        }
        return coalescer.evaluateAligned(receipts, partitions, config,
                CanonicalTaskRequests.MAX_EXTERNAL_PARTITIONS_PER_INPUT);
    }

    private static StageEdge eligibleInput(Stage consumer, StageDAG dag, Stage producer) {
        if (producer == null || producer.shuffleHandle() == null || !producer.exchangeReadyForConsumers()) {
            return null;
        }
        var edge = dag.findDownstreamEdge(producer.stageId());
        return edge != null && edge.downstreamStageId() == consumer.stageId() && edge.isHashShuffle()
                && edge.requireComplete() && edge.partitionCount() == consumer.plan().parallelism() ? edge : null;
    }
}
