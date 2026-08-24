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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Fragments a RelNode tree into a {@link FragmentedPlan} at Exchange boundaries.
 * Each {@link PlanStage} wraps a sub-tree as an opaque {@link PlanFragment} — no
 * Calcite types escape into coordinator code.
 *
 * <p>Lives in {@code org.pangrova.planner} (same package as {@link PlanFragment}) so it
 * can access {@link PlanFragment}'s package-private constructor and static helpers.</p>
 *
 * <p>Coordinator code reaches this only via {@link PlannedQuery#fragment(int)} —
 * it never imports this class directly.</p>
 */
final class PlanFragmenter {

    private static final Logger LOG = LoggerFactory.getLogger(PlanFragmenter.class);
    private static final int DEFAULT_HASH_PARALLELISM = 4;
    private static final int SINGLE_PARALLELISM = 1;
    private static final long DEFAULT_BYTES_PER_PARTITION = 256L * 1024 * 1024; // 256 MB
    private static final int DEFAULT_MAX_PARALLELISM = 128;

    private PlanFragmenter() {}

    /**
     * Fragments the plan using the given parallelism override.
     *
     * @param root                       the root RelNode to fragment
     * @param sessionParallelismOverride positive value overrides dynamic parallelism; -1 means auto
     */
    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride) {
        checkNotNull(root, "root RelNode must not be null");
        FragmentContext ctx = new FragmentContext(
                sessionParallelismOverride, DEFAULT_BYTES_PER_PARTITION, DEFAULT_MAX_PARALLELISM);
        int rootStageId = ctx.visitNode(root);
        ctx.setTaskType(rootStageId, "OUTPUT");
        // OUTPUT stage must have parallelism=1 so the client has a single result endpoint.
        ctx.setParallelism(rootStageId, SINGLE_PARALLELISM);
        return ctx.buildFragmentedPlan();
    }

    /** Converts a Calcite RelDistribution.Type to the coordinator-safe ShuffleType. */
    static ShuffleType toShuffleType(RelDistribution.Type distType) {
        return switch (distType) {
            case BROADCAST_DISTRIBUTED -> ShuffleType.BROADCAST;
            case HASH_DISTRIBUTED -> ShuffleType.HASH;
            case RANGE_DISTRIBUTED -> ShuffleType.RANGE;
            case SINGLETON -> ShuffleType.SINGLETON;
            default -> ShuffleType.ANY;
        };
    }

    /**
     * Mutable context accumulating stages and edges during the RelNode tree walk.
     */
    private static final class FragmentContext {

        private int nextStageId;
        private final Map<Integer, MutableStage> stageMap = new HashMap<>();
        private final Map<Integer, List<Integer>> edges = new HashMap<>();
        private final List<StageTransfer> transfers = new ArrayList<>();

        private final int sessionParallelismOverride;
        private final long bytesPerPartition;
        private final int maxParallelism;

        FragmentContext(int sessionParallelismOverride,
                        long bytesPerPartition, int maxParallelism) {
            this.sessionParallelismOverride = sessionParallelismOverride;
            this.bytesPerPartition = bytesPerPartition > 0
                    ? bytesPerPartition : DEFAULT_BYTES_PER_PARTITION;
            this.maxParallelism = maxParallelism > 0
                    ? maxParallelism : DEFAULT_MAX_PARALLELISM;
        }

        int visitNode(RelNode node) {
            if (node instanceof Exchange exchange) {
                return visitRootExchange(exchange);
            }
            List<RelNode> inputs = node.getInputs();
            if (inputs.isEmpty()) {
                return createStage(node, SINGLE_PARALLELISM, "COMPUTE");
            }
            return visitWithChildren(node, inputs);
        }

        private int visitRootExchange(Exchange exchange) {
            int upstreamId = visitNode(exchange.getInput());
            int parallelism = resolveParallelism(exchange);
            int stageId = createStage(exchange, parallelism, "COMPUTE");
            addEdgeWithDist(stageId, upstreamId, exchange.getDistribution());
            return stageId;
        }

        private int visitWithChildren(RelNode node, List<RelNode> inputs) {
            List<ChildResult> children = collectChildren(inputs);
            boolean anyExchange = children.stream().anyMatch(c -> c.fromExchange);
            if (!anyExchange) {
                return collapseIntoFirst(node, children);
            }
            return createStageWithDeps(node, children);
        }

        private List<ChildResult> collectChildren(List<RelNode> inputs) {
            List<ChildResult> results = new ArrayList<>(inputs.size());
            for (RelNode input : inputs) {
                if (input instanceof Exchange exchange) {
                    int upstreamId = visitNode(exchange.getInput());
                    int parallelism = resolveParallelism(exchange);
                    results.add(new ChildResult(upstreamId, true, parallelism,
                            exchange.getDistribution()));
                } else {
                    int childId = visitNode(input);
                    results.add(new ChildResult(childId, false, SINGLE_PARALLELISM, null));
                }
            }
            return results;
        }

        private int collapseIntoFirst(RelNode node, List<ChildResult> children) {
            int merged = children.get(0).stageId;
            replacePlan(merged, node);
            for (int i = 1; i < children.size(); i++) {
                mergeStage(children.get(i).stageId, merged);
            }
            return merged;
        }

        private int createStageWithDeps(RelNode node, List<ChildResult> children) {
            int maxPar = children.stream()
                    .filter(c -> c.fromExchange)
                    .mapToInt(c -> c.parallelism)
                    .max()
                    .orElse(SINGLE_PARALLELISM);
            int stageId = createStage(node, maxPar, "COMPUTE");
            for (ChildResult c : children) {
                if (c.fromExchange && c.distribution != null) {
                    addEdgeWithDist(stageId, c.stageId, c.distribution);
                } else {
                    addEdge(stageId, c.stageId);
                }
            }
            return stageId;
        }

        private int resolveParallelism(Exchange exchange) {
            if (sessionParallelismOverride > 0) {
                return Math.min(sessionParallelismOverride, maxParallelism);
            }
            RelDistribution distribution = exchange.getDistribution();
            return switch (distribution.getType()) {
                case HASH_DISTRIBUTED, RANGE_DISTRIBUTED ->
                        computeDynamicParallelism(exchange);
                default -> SINGLE_PARALLELISM;
            };
        }

        private int computeDynamicParallelism(Exchange exchange) {
            RelNode input = exchange.getInput();
            try {
                var metadata = input.getCluster().getMetadataQuery();
                Double rowCount = metadata.getRowCount(input);
                Double avgRowSize = metadata.getAverageRowSize(input);
                if (rowCount != null && avgRowSize != null) {
                    if (rowCount <= 0 || avgRowSize <= 0) {
                        return SINGLE_PARALLELISM;
                    }
                    long estimatedBytes = (long) (rowCount * avgRowSize * 0.5);
                    if (estimatedBytes > bytesPerPartition) {
                        return (int) Math.min(maxParallelism,
                                estimatedBytes / bytesPerPartition + 1);
                    }
                }
            } catch (Exception e) {
                LOG.warn("event=dynamic_parallelism_fallback reason={}", e.getMessage());
            }
            return DEFAULT_HASH_PARALLELISM;
        }

        int createStage(RelNode plan, int parallelism, String taskType) {
            int id = nextStageId++;
            stageMap.put(id, new MutableStage(id, plan, parallelism, taskType));
            return id;
        }

        void setTaskType(int stageId, String taskType) {
            findStage(stageId).taskType = taskType;
        }

        void setParallelism(int stageId, int parallelism) {
            findStage(stageId).parallelism = parallelism;
        }

        void replacePlan(int stageId, RelNode newPlan) {
            findStage(stageId).plan = newPlan;
        }

        void addEdge(int downstream, int upstream) {
            edges.computeIfAbsent(downstream, k -> new ArrayList<>()).add(upstream);
        }

        void addEdgeWithDist(int downstream, int upstream, RelDistribution dist) {
            addEdge(downstream, upstream);
            List<Integer> keys = dist.getKeys() != null
                    ? dist.getKeys().stream().map(Integer::valueOf).toList()
                    : List.of();
            transfers.add(new StageTransfer(upstream, downstream,
                    toShuffleType(dist.getType()), keys));
        }

        void mergeStage(int sourceId, int targetId) {
            for (List<Integer> deps : edges.values()) {
                deps.replaceAll(id -> id == sourceId ? targetId : id);
            }
            List<Integer> sourceDeps = edges.remove(sourceId);
            if (sourceDeps != null) {
                edges.computeIfAbsent(targetId, k -> new ArrayList<>())
                        .addAll(sourceDeps);
            }
            transfers.replaceAll(t -> {
                int up = t.upstreamStageId() == sourceId ? targetId : t.upstreamStageId();
                int down = t.downstreamStageId() == sourceId ? targetId : t.downstreamStageId();
                return (up != t.upstreamStageId() || down != t.downstreamStageId())
                        ? new StageTransfer(up, down, t.shuffleType(), t.distributionKeys())
                        : t;
            });
            stageMap.remove(sourceId);
        }

        FragmentedPlan buildFragmentedPlan() {
            List<PlanStage> stages = stageMap.values().stream()
                    .map(s -> {
                        long estBytes = PlanFragment.estimateBytes(s.plan);
                        long estRows = PlanFragment.estimateRows(s.plan);
                        PlanFragment fragment = new PlanFragment(s.plan, estBytes, estRows);
                        return new PlanStage(s.id, fragment, s.parallelism, s.taskType);
                    })
                    .toList();
            return new FragmentedPlan(stages, transfers, edges);
        }

        private MutableStage findStage(int stageId) {
            MutableStage stage = stageMap.get(stageId);
            if (stage == null) {
                throw new IllegalStateException("Stage not found: " + stageId);
            }
            return stage;
        }
    }

    private record ChildResult(int stageId, boolean fromExchange,
                               int parallelism, RelDistribution distribution) {}

    private static final class MutableStage {
        final int id;
        RelNode plan;
        int parallelism;
        String taskType;

        MutableStage(int id, RelNode plan, int parallelism, String taskType) {
            this.id = id;
            this.plan = plan;
            this.parallelism = parallelism;
            this.taskType = taskType;
        }
    }
}
