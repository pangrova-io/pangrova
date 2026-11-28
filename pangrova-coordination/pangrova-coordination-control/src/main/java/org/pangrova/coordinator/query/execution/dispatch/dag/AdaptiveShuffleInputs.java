package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.ShuffleInputLayout;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.coordinator.generation.template.PersistedStagePlan;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;

import java.util.HashMap;
import java.util.List;

/** Selects ranges exclusively from the finalized inputs of a partition-local stage. */
final class AdaptiveShuffleInputs {
    private final AdaptiveQueryConfig config;
    private final PartitionCoalescer coalescer = new PartitionCoalescer();

    AdaptiveShuffleInputs(AdaptiveQueryConfig config) {
        this.config = config;
    }

    record Selection(CoalesceResult coalesced, ShuffleInputLayout layout) {
        static Selection coalesced(CoalesceResult result) {
            return new Selection(result, ShuffleInputLayout.coalesced(result));
        }
    }

    private static final Selection NO_OP = Selection.coalesced(CoalesceResult.NO_OP);

    Selection select(Stage stage, StageDAG dag) {
        var plan = stage.plan();
        if (config == null || !config.enabled()
                || !plan.taskType().equals("COMPUTE") || plan.parallelism() < 2
                || (plan.stagePlan().mode() != StagePlanMode.LIVE_QUERY
                    && !(plan.stagePlan() instanceof PersistedStagePlan))) {
            return NO_OP;
        }
        var output = dag.findDownstreamEdge(stage.stageId());
        if (output == null || (output.shuffleType() != ShuffleType.HASH
                && output.shuffleType() != ShuffleType.ROUND_ROBIN && output.shuffleType() != ShuffleType.SINGLETON)) {
            return NO_OP;
        }
        var upstream = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (upstream.size() == 1) {
            return Selection.coalesced(selectSingle(stage, dag, upstream.get(0)));
        }
        return upstream.size() == 2 ? selectJoin(stage, dag, upstream) : NO_OP;
    }

    private CoalesceResult selectSingle(Stage stage, StageDAG dag, int producerId) {
        var producer = dag.findStage(producerId);
        var edge = eligibleInput(stage, dag, producer);
        if (edge == null || !isSafeSingle(stage, edge)) {
            return CoalesceResult.NO_OP;
        }
        return coalescer.evaluate(producer.requireShuffleReceipt(), edge.partitionCount(), config,
                CanonicalTaskRequests.MAX_EXTERNAL_PARTITIONS_PER_INPUT);
    }

    private Selection selectJoin(Stage stage, StageDAG dag, List<Integer> upstream) {
        var exchanges = new HashMap<String, List<Integer>>();
        var receipts = new HashMap<String, ShuffleCommitReceipt>();
        for (int producerId : upstream) {
            var producer = dag.findStage(producerId);
            var edge = eligibleInput(stage, dag, producer);
            if (edge == null || exchanges.put(edge.exchangeId(), edge.distributionKeys()) != null) {
                return NO_OP;
            }
            receipts.put(edge.exchangeId(), producer.requireShuffleReceipt());
        }
        int partitions = stage.plan().parallelism();
        if (receipts.values().stream().anyMatch(receipt -> receipt.partitions().size() != partitions)) return NO_OP;
        var stagePlan = stage.plan().stagePlan();
        if (stagePlan instanceof PersistedStagePlan persisted) {
            return persisted.stage().coalescibleHashInputs()
                    ? Selection.coalesced(coalesceJoin(List.copyOf(receipts.values()), partitions)) : NO_OP;
        }
        var proof = stagePlan.requireLiveFragment().alignedHashJoin(exchanges, partitions).orElse(null);
        if (proof == null) return NO_OP;
        var left = receipts.get(proof.leftExchangeId());
        var right = receipts.get(proof.rightExchangeId());
        var coalesced = coalesceJoin(List.of(left, right), partitions);
        return new Selection(coalesced, SkewedHashJoinInputs.select(proof, left, right, coalesced, config));
    }

    private CoalesceResult coalesceJoin(List<ShuffleCommitReceipt> receipts, int partitions) {
        return coalescer.evaluateAligned(receipts, partitions, config,
                CanonicalTaskRequests.MAX_EXTERNAL_PARTITIONS_PER_INPUT);
    }

    private static boolean isSafeSingle(Stage stage, StageEdge edge) {
        var plan = stage.plan().stagePlan();
        return plan instanceof PersistedStagePlan persisted ? persisted.stage().coalescibleHashInputs()
                : plan.requireLiveFragment().supportsCoalescedHashInput(edge.exchangeId(), edge.distributionKeys());
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
