package org.pangrova.coordinator.query.execution.dispatch.dag;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.ShuffleInputLayout;
import org.pangrova.planner.adaptive.AdaptiveQueryConfig;
import org.pangrova.planner.adaptive.PartitionCoalescer;
import org.pangrova.planner.analysis.PlanFragment.AlignedHashJoin;
import org.pangrova.planner.api.adaptive.CoalesceResult;
import org.pangrova.planner.enums.ShuffleType;
import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.coordinator.generation.template.PersistedStagePlan;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Selects ranges exclusively from the finalized inputs of a partition-local stage. */
final class AdaptiveShuffleInputs {
    private final AdaptiveQueryConfig config;
    private final PartitionCoalescer coalescer = new PartitionCoalescer();

    AdaptiveShuffleInputs(AdaptiveQueryConfig config) {
        this.config = config;
    }

    ShuffleInputLayout select(Stage stage, StageDAG dag) {
        var plan = stage.plan();
        if (config == null || !config.enabled()
                || !plan.taskType().equals("COMPUTE") || plan.parallelism() < 2
                || (plan.stagePlan().mode() != StagePlanMode.LIVE_QUERY
                    && !(plan.stagePlan() instanceof PersistedStagePlan))) {
            return ShuffleInputLayout.DEFAULT;
        }
        var output = dag.findDownstreamEdge(stage.stageId());
        if (output == null || (output.shuffleType() != ShuffleType.HASH
                && output.shuffleType() != ShuffleType.ROUND_ROBIN && output.shuffleType() != ShuffleType.SINGLETON)) {
            return ShuffleInputLayout.DEFAULT;
        }
        var upstream = dag.edges().getOrDefault(stage.stageId(), List.of());
        if (upstream.size() == 1) {
            return ShuffleInputLayout.coalesced(selectSingle(stage, dag, upstream.get(0)));
        }
        return upstream.size() == 2 ? selectJoin(stage, dag, upstream) : ShuffleInputLayout.DEFAULT;
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

    private ShuffleInputLayout selectJoin(Stage stage, StageDAG dag, List<Integer> upstream) {
        var exchanges = new HashMap<String, List<Integer>>();
        var receipts = new HashMap<String, ShuffleCommitReceipt>();
        for (int producerId : upstream) {
            var producer = dag.findStage(producerId);
            var edge = eligibleInput(stage, dag, producer);
            if (edge == null || exchanges.put(edge.exchangeId(), edge.distributionKeys()) != null) {
                return ShuffleInputLayout.DEFAULT;
            }
            receipts.put(edge.exchangeId(), producer.requireShuffleReceipt());
        }
        int partitions = stage.plan().parallelism();
        if (receipts.values().stream().anyMatch(receipt -> receipt.partitions().size() != partitions)) {
            return ShuffleInputLayout.DEFAULT;
        }
        var stagePlan = stage.plan().stagePlan();
        var proof = stagePlan instanceof PersistedStagePlan persisted
                ? persistedJoin(persisted, receipts)
                : stagePlan.requireLiveFragment().alignedHashJoin(exchanges, partitions).orElse(null);
        if (proof == null) return ShuffleInputLayout.DEFAULT;
        var left = receipts.get(proof.leftExchangeId());
        var right = receipts.get(proof.rightExchangeId());
        var coalesced = coalesceJoin(List.of(left, right), partitions);
        return SkewedHashJoinInputs.select(proof, left, right, coalesced, config);
    }

    private static AlignedHashJoin persistedJoin(
            PersistedStagePlan persisted, Map<String, ShuffleCommitReceipt> receipts) {
        if (!persisted.stage().coalescibleHashInputs()) return null;
        var ordered = receipts.keySet().stream().sorted().toList();
        var split = persisted.stage().splittableHashInputs();
        return new AlignedHashJoin(
                ordered.get(0), ordered.get(1), split.contains(ordered.get(0)), split.contains(ordered.get(1)));
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
