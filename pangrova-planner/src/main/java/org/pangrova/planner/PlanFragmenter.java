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

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.cost.distribution.WindowPartitioning;
import org.pangrova.protocol.v2.BatchSerde;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Splits a physical plan into self-contained producer and consumer fragments.
 * A producer terminates in a standard Substrait exchange and a consumer starts
 * at a typed exchange-input leaf. Runtime endpoints never enter plan bytes.
 */
final class PlanFragmenter {

    private static final int DEFAULT_PARALLELISM = 4;
    private static final int SINGLE_PARALLELISM = 1;
    private static final long DEFAULT_BYTES_PER_PARTITION = 256L * 1024 * 1024;
    private static final int DEFAULT_MAX_PARALLELISM = 128;
    private static final BatchSerde EXCHANGE_SERDE = BatchSerde.BATCH_SERDE_ARROW_BATCH;

    private PlanFragmenter() {}

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride) {
        return fragment(root, sessionParallelismOverride, AuthorizedStageGraph.TerminalPurpose.QUERY_RESULT);
    }

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride,
            AuthorizedStageGraph.TerminalPurpose terminalPurpose) {
        checkNotNull(root, "root RelNode must not be null");
        FragmentContext context = new FragmentContext(
                sessionParallelismOverride,
                DEFAULT_BYTES_PER_PARTITION,
                DEFAULT_MAX_PARALLELISM,
                checkNotNull(terminalPurpose, "terminalPurpose must not be null"));
        return context.fragment(root);
    }

    static ShuffleType toShuffleType(RelDistribution.Type distributionType) {
        return switch (distributionType) {
            case BROADCAST_DISTRIBUTED -> ShuffleType.BROADCAST;
            case HASH_DISTRIBUTED -> ShuffleType.HASH;
            case RANGE_DISTRIBUTED -> ShuffleType.RANGE;
            case RANDOM_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED -> ShuffleType.ROUND_ROBIN;
            case SINGLETON -> ShuffleType.SINGLETON;
            case ANY -> ShuffleType.ANY;
        };
    }

    private static ShuffleType toShuffleType(PangrovaDistribution distribution) {
        return switch (distribution.type()) {
            case HASH -> ShuffleType.HASH;
            case BROADCAST -> ShuffleType.BROADCAST;
            case ROUND_ROBIN -> ShuffleType.ROUND_ROBIN;
            case SINGLETON -> ShuffleType.SINGLETON;
            case ANY -> ShuffleType.ANY;
        };
    }

    private static final class FragmentContext {

        private final Map<Integer, MutableStage> stages = new HashMap<>();
        private final Map<Integer, List<Integer>> dependencies = new HashMap<>();
        private final List<StageTransfer> transfers = new ArrayList<>();
        private final int sessionParallelismOverride;
        private final long bytesPerPartition;
        private final int maxParallelism;
        private final AuthorizedStageGraph.TerminalPurpose terminalPurpose;
        private int nextStageId;

        private FragmentContext(
                int sessionParallelismOverride,
                long bytesPerPartition,
                int maxParallelism,
                AuthorizedStageGraph.TerminalPurpose terminalPurpose) {
            this.sessionParallelismOverride = sessionParallelismOverride;
            this.bytesPerPartition = bytesPerPartition;
            this.maxParallelism = maxParallelism;
            this.terminalPurpose = terminalPurpose;
        }

        FragmentedPlan fragment(RelNode root) {
            root = enforceGlobalOperators(root, true);
            int parallelism = resolveStageParallelism(root);
            int computeStageId = createStage(root, parallelism, "COMPUTE");
            setPlan(computeStageId, rewrite(root, computeStageId, parallelism));
            if (parallelism == SINGLE_PARALLELISM
                    || terminalPurpose == AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT) {
                stages.get(computeStageId).taskType = "OUTPUT";
            } else {
                addOutputStage(computeStageId);
            }
            return build();
        }

        /** Aggregates, windows and global ordering require a semantically complete input distribution. */
        private RelNode enforceGlobalOperators(RelNode node, boolean root) {
            if (!node.getInputs().isEmpty()) {
                List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
                boolean changed = false;
                for (RelNode input : node.getInputs()) {
                    RelNode rewritten = enforceGlobalOperators(input, false);
                    inputs.add(rewritten);
                    changed |= rewritten != input;
                }
                if (changed) {
                    node = node.copy(node.getTraitSet(), inputs);
                }
            }
            if (node instanceof Aggregate aggregate
                    && !hasOnlyValuesLeaves(aggregate.getInput())) {
                PangrovaDistribution distribution = (aggregate.getGroupSets() == null
                        || aggregate.getGroupSets().size() == 1)
                        && !aggregate.getGroupSet().isEmpty()
                        ? PangrovaDistribution.hash(aggregate.getGroupSet().asList())
                        : PangrovaDistribution.SINGLETON;
                RelNode input = enforceDistribution(aggregate.getInput(), distribution);
                if (input != aggregate.getInput()) {
                    return aggregate.copy(aggregate.getTraitSet(), input,
                            aggregate.getGroupSet(), aggregate.getGroupSets(),
                            aggregate.getAggCallList());
                }
            }
            if (node instanceof Window window && !hasOnlyValuesLeaves(window.getInput())) {
                return WindowPartitioning.enforce(window, FragmentContext::enforceDistribution);
            }
            if (node instanceof Sort sort
                    && !(root && terminalPurpose == AuthorizedStageGraph.TerminalPurpose.QUERY_RESULT
                            && sort.fetch != null
                            && (sort.offset == null || RexLiteral.intValue(sort.offset) == 0))
                    && !hasOnlyValuesLeaves(sort.getInput())) {
                RelNode input = enforceDistribution(sort.getInput(),
                        PangrovaDistribution.SINGLETON);
                if (input != sort.getInput()) {
                    return sort.copy(sort.getTraitSet(), input,
                            sort.getCollation(), sort.offset, sort.fetch);
                }
            }
            return node;
        }

        private static RelNode enforceDistribution(
                RelNode input, PangrovaDistribution distribution) {
            ExchangeBoundary boundary = exchangeBoundary(input);
            if (boundary != null && boundary.distribution().equals(distribution)) {
                return input;
            }
            return new PangrovaExchange(input.getCluster(),
                    input.getTraitSet().replace(distribution), input, distribution);
        }

        private RelNode rewrite(RelNode node, int stageId, int stageParallelism) {
            ExchangeBoundary boundary = exchangeBoundary(node);
            if (boundary != null) {
                return materializeBoundary(boundary, stageId, stageParallelism);
            }
            if (node.getInputs().isEmpty()) {
                return node;
            }
            List<RelNode> rewrittenInputs = new ArrayList<>(node.getInputs().size());
            boolean changed = false;
            for (RelNode input : node.getInputs()) {
                RelNode rewritten = rewrite(input, stageId, stageParallelism);
                rewrittenInputs.add(rewritten);
                changed |= rewritten != input;
            }
            return changed ? node.copy(node.getTraitSet(), rewrittenInputs) : node;
        }

        private RelNode materializeBoundary(
                ExchangeBoundary boundary,
                int downstreamStageId,
                int downstreamParallelism) {
            int producerParallelism = resolveStageParallelism(boundary.input());
            int upstreamStageId = createStage(
                    boundary.input(), producerParallelism, "COMPUTE");
            RelNode producerInput = rewrite(
                    boundary.input(), upstreamStageId, producerParallelism);
            PangrovaExchange producer = boundProducer(
                    boundary, producerInput, downstreamParallelism);
            setPlan(upstreamStageId, producer);

            String exchangeId = exchangeId(upstreamStageId, downstreamStageId);
            addTransfer(boundary.distribution(), exchangeId,
                    upstreamStageId, downstreamStageId, downstreamParallelism);
            return new PangrovaExchangeInput(
                    boundary.node().getCluster(),
                    boundary.node().getTraitSet(),
                    boundary.node().getRowType(),
                    new PangrovaExchangeInput.Specification(
                            "exchange-input-" + exchangeId,
                            exchangeId,
                            upstreamStageId,
                            downstreamStageId,
                            boundary.distribution(),
                            downstreamParallelism,
                            EXCHANGE_SERDE,
                            true),
                    new PlanFragment(producerInput, 0, 0).resultColumnModels());
        }

        private PangrovaExchange boundProducer(
                ExchangeBoundary boundary,
                RelNode producerInput,
                int partitionCount) {
            PangrovaDistribution distribution = boundary.distribution();
            validateMaterializedDistribution(distribution);
            return new PangrovaExchange(
                    boundary.node().getCluster(),
                    boundary.node().getTraitSet(),
                    producerInput,
                    distribution,
                    partitionCount);
        }

        private void addOutputStage(int computeStageId) {
            MutableStage compute = stages.get(computeStageId);
            RelNode computeRoot = compute.plan;
            int outputStageId = createStage(
                    computeRoot, SINGLE_PARALLELISM, "OUTPUT");
            String exchangeId = exchangeId(computeStageId, outputStageId);
            PangrovaExchange producer = new PangrovaExchange(
                    computeRoot.getCluster(),
                    computeRoot.getTraitSet(),
                    computeRoot,
                    PangrovaDistribution.SINGLETON,
                    SINGLE_PARALLELISM);
            compute.plan = producer;
            addTransfer(PangrovaDistribution.SINGLETON, exchangeId,
                    computeStageId, outputStageId, SINGLE_PARALLELISM);
            RelNode exchangeInput = new PangrovaExchangeInput(
                    compute.plan.getCluster(),
                    compute.plan.getTraitSet(),
                    compute.plan.getRowType(),
                    new PangrovaExchangeInput.Specification(
                            "exchange-input-" + exchangeId,
                            exchangeId,
                            computeStageId,
                            outputStageId,
                            PangrovaDistribution.SINGLETON,
                            SINGLE_PARALLELISM,
                            EXCHANGE_SERDE,
                            true),
                    new PlanFragment(computeRoot, 0, 0).resultColumnModels());
            setPlan(outputStageId, finalReducer(computeRoot, exchangeInput));
        }

        /** Native reapplies Top-K after the singleton boundary to globally order task-local candidates. */
        private static RelNode finalReducer(RelNode computeRoot, RelNode exchangeInput) {
            if (computeRoot instanceof Sort sort
                    && sort.fetch != null
                    && (sort.offset == null || RexLiteral.intValue(sort.offset) == 0)) {
                return sort.copy(
                        sort.getTraitSet(),
                        exchangeInput,
                        sort.getCollation(),
                        sort.offset,
                        sort.fetch);
            }
            return exchangeInput;
        }

        private void addTransfer(
                PangrovaDistribution distribution,
                String exchangeId,
                int upstreamStageId,
                int downstreamStageId,
                int partitionCount) {
            dependencies.computeIfAbsent(
                    downstreamStageId, ignored -> new ArrayList<>()).add(upstreamStageId);
            transfers.add(new StageTransfer(
                    upstreamStageId,
                    downstreamStageId,
                    toShuffleType(distribution),
                    distribution.keys(),
                    exchangeId,
                    partitionCount,
                    EXCHANGE_SERDE,
                    true));
        }

        private int resolveStageParallelism(RelNode root) {
            List<ExchangeBoundary> boundaries = new ArrayList<>();
            collectDirectBoundaries(root, boundaries);
            // An unfragmented VALUES source is replicated verbatim by every
            // task. It has no splits to partition, so an explicit session
            // override must not multiply rows or provider calls. A boundary
            // itself still keeps its requested downstream partition count.
            if (boundaries.isEmpty() && hasOnlyValuesLeaves(root)) {
                return SINGLE_PARALLELISM;
            }
            if (boundaries.stream().anyMatch(FragmentContext::isSingleton)) {
                return SINGLE_PARALLELISM;
            }
            if (sessionParallelismOverride > 0) {
                return Math.min(sessionParallelismOverride, maxParallelism);
            }
            if (boundaries.isEmpty()) {
                return computeDynamicParallelism(root);
            }
            return boundaries.stream()
                    .filter(boundary -> boundary.distribution().type()
                            != PangrovaDistribution.Type.BROADCAST)
                    .mapToInt(boundary -> computeDynamicParallelism(boundary.input()))
                    .max()
                    .orElseGet(() -> computeDynamicParallelism(root));
        }

        private static boolean hasOnlyValuesLeaves(RelNode node) {
            if (exchangeBoundary(node) != null) {
                return false;
            }
            if (node.getInputs().isEmpty()) {
                return node instanceof Values;
            }
            return node.getInputs().stream()
                    .allMatch(FragmentContext::hasOnlyValuesLeaves);
        }

        private static boolean isSingleton(ExchangeBoundary boundary) {
            return boundary.distribution().type() == PangrovaDistribution.Type.SINGLETON;
        }

        private void collectDirectBoundaries(
                RelNode node,
                List<ExchangeBoundary> boundaries) {
            ExchangeBoundary boundary = exchangeBoundary(node);
            if (boundary != null) {
                boundaries.add(boundary);
                return;
            }
            for (RelNode input : node.getInputs()) {
                collectDirectBoundaries(input, boundaries);
            }
        }

        private int computeDynamicParallelism(RelNode node) {
            return FragmentSourceParallelism.resolve(node, bytesPerPartition,
                    maxParallelism, DEFAULT_PARALLELISM);
        }

        private int createStage(RelNode plan, int parallelism, String taskType) {
            int stageId = nextStageId++;
            stages.put(stageId, new MutableStage(
                    stageId, plan, parallelism, taskType));
            return stageId;
        }

        private void setPlan(int stageId, RelNode plan) {
            MutableStage stage = stages.get(stageId);
            if (stage == null) {
                throw new IllegalStateException("Stage not found: " + stageId);
            }
            stage.plan = plan;
        }

        private FragmentedPlan build() {
            List<PlanStage> planStages = stages.values().stream()
                    .sorted(Comparator.comparingInt(stage -> stage.id))
                    .map(FragmentContext::immutableStage)
                    .toList();
            Map<Integer, List<Integer>> immutableDependencies = new LinkedHashMap<>();
            dependencies.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> immutableDependencies.put(
                            entry.getKey(), List.copyOf(entry.getValue())));
            return new FragmentedPlan(planStages, transfers, immutableDependencies);
        }

        private static PlanStage immutableStage(MutableStage stage) {
            PlanFragment fragment = new PlanFragment(
                    stage.plan,
                    PlanFragment.estimateBytes(stage.plan),
                    PlanFragment.estimateRows(stage.plan));
            return new PlanStage(
                    stage.id, fragment, stage.parallelism, stage.taskType);
        }

        private static ExchangeBoundary exchangeBoundary(RelNode node) {
            if (node instanceof PangrovaExchange exchange) {
                return new ExchangeBoundary(
                        node, exchange.getInput(), exchange.targetDistribution());
            }
            if (node instanceof Exchange exchange) {
                return new ExchangeBoundary(
                        node, exchange.getInput(), mapDistribution(exchange.getDistribution()));
            }
            return null;
        }

        private static PangrovaDistribution mapDistribution(RelDistribution distribution) {
            return switch (distribution.getType()) {
                case HASH_DISTRIBUTED -> PangrovaDistribution.hash(
                        distribution.getKeys().stream().map(Integer::valueOf).toList());
                case BROADCAST_DISTRIBUTED -> PangrovaDistribution.BROADCAST;
                case RANDOM_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED ->
                        PangrovaDistribution.ROUND_ROBIN;
                case SINGLETON -> PangrovaDistribution.SINGLETON;
                case RANGE_DISTRIBUTED -> throw new IllegalArgumentException(
                        "Range exchange requires typed partition bounds");
                case ANY -> throw new IllegalArgumentException(
                        "ANY distribution cannot materialize an exchange");
            };
        }

        private static void validateMaterializedDistribution(
                PangrovaDistribution distribution) {
            if (distribution.type() == PangrovaDistribution.Type.ANY) {
                throw new IllegalArgumentException(
                        "ANY distribution cannot materialize an exchange");
            }
            if (distribution.type() == PangrovaDistribution.Type.HASH
                    && distribution.keys().isEmpty()) {
                throw new IllegalArgumentException(
                        "Hash exchange requires at least one partition key");
            }
        }

        private static String exchangeId(int upstreamStageId, int downstreamStageId) {
            return "exchange-" + upstreamStageId + "-to-" + downstreamStageId;
        }
    }

    private record ExchangeBoundary(
            RelNode node,
            RelNode input,
            PangrovaDistribution distribution) {}

    private static final class MutableStage {
        private final int id;
        private final int parallelism;
        private RelNode plan;
        private String taskType;

        private MutableStage(
                int id,
                RelNode plan,
                int parallelism,
                String taskType) {
            this.id = id;
            this.plan = plan;
            this.parallelism = parallelism;
            this.taskType = taskType;
        }
    }
}
