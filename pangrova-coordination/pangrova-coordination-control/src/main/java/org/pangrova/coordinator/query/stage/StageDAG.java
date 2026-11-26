package org.pangrova.coordinator.query.stage;

import org.pangrova.query.api.enums.StageState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.pangrova.planner.analysis.AuthorizedStageGraph;
import org.pangrova.planner.analysis.FragmentedPlan;
import org.pangrova.planner.analysis.PlanStage;
import org.pangrova.planner.analysis.StageTransfer;
import org.pangrova.planner.analysis.StageRuntimeFilter;
import org.pangrova.shuffle.exchange.enums.ExchangeReadMode;
import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Directed acyclic graph of execution stages.
 * Edges map each stageId to its upstream dependency stageIds.
 * StageEdges carry distribution type metadata for each edge.
 *
 * <p>Holds both {@link Stage} entities (with state machines) and
 * the underlying {@link StageNode} plan records for backward compatibility.
 *
 * <p>Pre-computes O(1) lookup indexes for stageId-based queries and a
 * reverse (upstream → downstream) edge index for efficient DAG traversal.
 */
public record StageDAG(
        List<Stage> stageEntities,
        List<StageNode> stages,
        Map<Integer, List<Integer>> edges,
        List<StageEdge> stageEdges,
        Map<Integer, Stage> stageEntityIndex,
        Map<Integer, StageNode> stageNodeIndex,
        Map<Integer, List<Integer>> downstreamIndex,
        Optional<StageRuntimeFilter> runtimeFilter
) {

    public StageDAG {
        checkNotNull(stageEntities, "stageEntities must not be null");
        checkNotNull(stages, "stages must not be null");
        checkNotNull(edges, "edges must not be null");
        checkNotNull(stageEdges, "stageEdges must not be null");
        stageEntities = List.copyOf(stageEntities);
        stages = List.copyOf(stages);
        edges = Map.copyOf(edges);
        stageEdges = List.copyOf(stageEdges);
        stageEntityIndex = stageEntityIndex != null ? Map.copyOf(stageEntityIndex) : Map.of();
        stageNodeIndex = stageNodeIndex != null ? Map.copyOf(stageNodeIndex) : Map.of();
        downstreamIndex = downstreamIndex != null ? Map.copyOf(downstreamIndex) : Map.of();
        checkNotNull(runtimeFilter, "runtimeFilter must not be null");
    }

    /**
     * Convenience constructor: computes indexes from the primary fields.
     * Validates that the edge graph contains no cycles before construction.
     *
     * @throws IllegalStateException if the edge graph contains a cycle
     */
    public static StageDAG create(List<Stage> stageEntities, List<StageNode> stages,
                                  Map<Integer, List<Integer>> edges, List<StageEdge> stageEdges) {
        return create(stageEntities, stages, edges, stageEdges, Optional.empty());
    }

    private static StageDAG create(List<Stage> stageEntities, List<StageNode> stages,
            Map<Integer, List<Integer>> edges, List<StageEdge> stageEdges, Optional<StageRuntimeFilter> filter) {
        validateNoCycles(edges);
        return new StageDAG(stageEntities, stages, edges, stageEdges,
                buildEntityIndex(stageEntities), buildNodeIndex(stages),
                buildDownstreamIndex(edges), filter);
    }

    /**
     * Creates a StageDAG from a planner-produced {@link FragmentedPlan}.
     * Converts each {@link PlanStage} to a {@link StageNode} and each
     * {@link StageTransfer} to a {@link StageEdge} — no Calcite types needed.
     * Stage entities are not populated — call {@link #withStages(String)} to create them.
     */
    public static StageDAG fromFragmentedPlan(FragmentedPlan fragmentedPlan) {
        checkNotNull(fragmentedPlan, "fragmentedPlan must not be null");
        List<StageNode> nodes = fragmentedPlan.stages().stream()
                .map(ps -> new StageNode(
                        ps.stageId(), ps.fragment(), ps.parallelism(), ps.taskType(),
                        List.of(),
                        ps.fragment().estimatedShuffleBytes(),
                        ps.fragment().estimatedShuffleRows()))
                .toList();
        List<StageEdge> stageEdges = fragmentedPlan.transfers().stream()
                .map(t -> new StageEdge(
                        t.upstreamStageId(), t.downstreamStageId(),
                        t.shuffleType(), t.distributionKeys(), t.exchangeId(),
                        t.partitionCount(), t.batchSerde(), t.requireComplete()))
                .toList();
        return create(List.of(), nodes, fragmentedPlan.stageDependencies(), stageEdges, fragmentedPlan.runtimeFilter());
    }

    /** Restores the same execution topology from immutable authorized stage bytes, without Calcite or SQL. */
    public static StageDAG fromAuthorizedStageGraph(AuthorizedStageGraph graph) {
        checkNotNull(graph, "graph must not be null");
        List<StageNode> nodes = graph.stages().stream()
                .map(stage -> new StageNode(stage.stageId(), stage, stage.parallelism(), stage.taskType(),
                        List.of(), stage.estimatedShuffleBytes(), stage.estimatedShuffleRows()))
                .toList();
        List<StageEdge> transfers = graph.transfers().stream()
                .map(edge -> new StageEdge(edge.upstreamStageId(), edge.downstreamStageId(),
                        edge.shuffleType(), edge.distributionKeys(), edge.exchangeId(),
                        edge.partitionCount(), edge.batchSerde(), edge.requireComplete()))
                .toList();
        Map<Integer, List<Integer>> dependencies = new HashMap<>();
        for (var edge : graph.transfers()) {
            dependencies.computeIfAbsent(edge.downstreamStageId(), ignored -> new ArrayList<>())
                    .add(edge.upstreamStageId());
        }
        dependencies.replaceAll((ignored, ids) -> List.copyOf(ids));
        return create(List.of(), nodes, dependencies, transfers);
    }

    /**
     * Creates a StageDAG from StageNode list with edge metadata.
     */
    public static StageDAG fromNodes(List<StageNode> nodes, Map<Integer, List<Integer>> edges,
                                     List<StageEdge> stageEdges) {
        return create(List.of(), nodes, edges, stageEdges);
    }

    /**
     * Creates a new StageDAG with Stage entities wrapping each StageNode.
     *
     * @param queryId the query identifier for state machine context
     * @return a new StageDAG with populated stageEntities
     */
    public StageDAG withStages(String queryId) {
        checkNotNull(queryId, "queryId must not be null");
        List<Stage> entities = stages.stream()
                .map(node -> Stage.create(node, queryId))
                .toList();
        return create(entities, stages, edges, stageEdges, runtimeFilter);
    }

    /**
     * Returns the read mode for a specific edge. Defaults to BLOCKING
     * when no edge metadata is found.
     */
    public ExchangeReadMode getEdgeReadMode(int upstreamId, int downstreamId) {
        for (StageEdge e : stageEdges) {
            if (e.upstreamStageId() == upstreamId
                    && e.downstreamStageId() == downstreamId) {
                return e.readMode();
            }
        }
        return ExchangeReadMode.BLOCKING;
    }

    /**
     * Finds the downstream edge for a given upstream stage.
     * Returns the first {@link StageEdge} where the upstream matches,
     * or {@code null} if no downstream edge exists (e.g., OUTPUT stage).
     */
    public StageEdge findDownstreamEdge(int upstreamStageId) {
        for (StageEdge e : stageEdges) {
            if (e.upstreamStageId() == upstreamStageId) {
                return e;
            }
        }
        return null;
    }

    /**
     * Finds a {@link StageNode} by stageId in O(1) via pre-computed index.
     * Returns {@code null} if no matching stage exists.
     */
    public StageNode findStageNode(int stageId) {
        return stageNodeIndex.get(stageId);
    }

    /**
     * Returns stages with no upstream dependencies (entry points of the DAG).
     */
    public List<StageNode> leafStages() {
        return stages.stream()
                .filter(s -> upstreamDeps(s.stageId()).isEmpty())
                .toList();
    }

    /**
     * Returns Stage entities with no upstream dependencies (root/source stages).
     */
    public List<Stage> rootStageEntities() {
        return stageEntities.stream()
                .filter(s -> upstreamDeps(s.stageId()).isEmpty())
                .toList();
    }

    /**
     * Returns stages whose all upstream dependencies are in the completed set.
     */
    public List<StageNode> readyStages(Set<Integer> completedStages) {
        checkNotNull(completedStages, "completedStages must not be null");
        return stages.stream()
                .filter(s -> !completedStages.contains(s.stageId()))
                .filter(s -> completedStages.containsAll(upstreamDeps(s.stageId())))
                .toList();
    }

    /**
     * Active states that indicate a stage is making progress. Used to
     * verify upstream health in pipeline scheduling: a downstream stage
     * can be scheduled once all its upstreams are in one of these states.
     * Terminal-failure states (FAILED, CANCELLED) and the initial
     * PLANNED state are excluded to prevent premature scheduling.
     */
    private static final Set<StageState> ACTIVE_STATES = Set.of(
            StageState.SCHEDULING,
            StageState.RUNNING,
            StageState.FLUSHING,
            StageState.FINISHED);

    /**
     * Returns Stage entities whose all upstream dependencies are in the active set
     * and have reached an active state (SCHEDULING, RUNNING, FLUSHING, or FINISHED).
     * This prevents premature downstream scheduling when an upstream stage is in
     * a terminal-failure state (FAILED, CANCELLED) or has not yet started (PLANNED).
     */
    public List<Stage> readyStageEntities(Set<Integer> completedStages) {
        checkNotNull(completedStages, "completedStages must not be null");
        return stageEntities.stream()
                .filter(s -> !completedStages.contains(s.stageId()))
                .filter(s -> {
                    List<Integer> deps = upstreamDeps(s.stageId());
                    if (!completedStages.containsAll(deps)) {
                        return false;
                    }
                    // Verify upstream stages are in an active state
                    for (int depId : deps) {
                        Stage upstream = stageEntityIndex.get(depId);
                        if (upstream != null && (!ACTIVE_STATES.contains(upstream.stateMachine().getState())
                                || !upstream.exchangeReadyForConsumers())) {
                            return false;
                        }
                    }
                    return true;
                })
                .toList();
    }

    /** Revalidates producer state at admission after an earlier ready-stage snapshot. */
    public boolean admitInputs(int stageId) {
        for (int producerId : upstreamDeps(stageId)) {
            if (!stageEntityIndex.get(producerId).admitDownstream()) return false;
        }
        return true;
    }

    /**
     * Finds a Stage entity by stageId in O(1) via pre-computed index.
     */
    public Stage findStage(int stageId) {
        return stageEntityIndex.get(stageId);
    }

    /**
     * Returns the direct downstream stageIds for a given upstream stageId.
     * Uses the pre-computed reverse index for O(1) lookup.
     */
    public List<Integer> directDownstream(int upstreamStageId) {
        return downstreamIndex.getOrDefault(upstreamStageId, List.of());
    }

    private List<Integer> upstreamDeps(int stageId) {
        return edges.getOrDefault(stageId, List.of());
    }

    private static Map<Integer, Stage> buildEntityIndex(List<Stage> entities) {
        if (entities == null || entities.isEmpty()) {
            return Map.of();
        }
        var map = new HashMap<Integer, Stage>(entities.size());
        for (Stage s : entities) {
            map.put(s.stageId(), s);
        }
        return map;
    }

    private static Map<Integer, StageNode> buildNodeIndex(List<StageNode> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return Map.of();
        }
        var map = new HashMap<Integer, StageNode>(nodes.size());
        for (StageNode n : nodes) {
            map.put(n.stageId(), n);
        }
        return map;
    }

    /**
     * Validates that the edge graph contains no cycles using DFS.
     * The edges map is downstream → [upstreams], so a cycle means a stage
     * transitively depends on itself.
     *
     * @throws IllegalStateException if a cycle is detected
     */
    private static void validateNoCycles(Map<Integer, List<Integer>> edges) {
        if (edges.isEmpty()) {
            return;
        }
        Set<Integer> allNodes = new HashSet<>(edges.keySet());
        for (List<Integer> deps : edges.values()) {
            allNodes.addAll(deps);
        }
        Set<Integer> visited = new HashSet<>();
        Set<Integer> inStack = new HashSet<>();
        for (int node : allNodes) {
            if (!visited.contains(node)) {
                if (hasCycleDfs(node, edges, visited, inStack)) {
                    throw new IllegalStateException(
                            "Stage DAG contains a cycle involving stageId=" + node
                                    + ". This indicates a bug in plan fragmentation.");
                }
            }
        }
    }

    private static boolean hasCycleDfs(int node, Map<Integer, List<Integer>> edges,
                                       Set<Integer> visited, Set<Integer> inStack) {
        visited.add(node);
        inStack.add(node);
        for (int upstream : edges.getOrDefault(node, List.of())) {
            if (inStack.contains(upstream)) {
                return true;
            }
            if (!visited.contains(upstream) && hasCycleDfs(upstream, edges, visited, inStack)) {
                return true;
            }
        }
        inStack.remove(node);
        return false;
    }

    /**
     * Builds a reverse index: upstream stageId → list of downstream stageIds.
     * The edges map is downstream → [upstreams], so we invert it.
     */
    private static Map<Integer, List<Integer>> buildDownstreamIndex(
            Map<Integer, List<Integer>> edges) {
        if (edges == null || edges.isEmpty()) {
            return Map.of();
        }
        var map = new HashMap<Integer, List<Integer>>();
        for (var entry : edges.entrySet()) {
            int downstream = entry.getKey();
            for (int upstream : entry.getValue()) {
                map.computeIfAbsent(upstream, k -> new ArrayList<>())
                        .add(downstream);
            }
        }
        return map;
    }
}
