package org.pangrova.coordinator.scheduler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Topological scheduler for stage DAGs.
 * Produces leaf-first ordering with output stages last.
 */
public class StageScheduler {

    /**
     * Descriptor for a single stage in the DAG.
     */
    public record StageInfo(String stageId, List<String> dependencyStageIds, boolean isOutput) {
        public StageInfo {
            dependencyStageIds = List.copyOf(dependencyStageIds);
        }
    }

    /**
     * Returns stages in topological order (leaf-first, output-last).
     * Validates the DAG has no cycles as part of the single topological sort pass.
     */
    public List<String> scheduleStages(List<StageInfo> stages) {
        Map<String, Integer> inDegree = buildInDegreeMap(stages);
        List<String> sorted = topologicalSort(stages, inDegree);
        if (sorted.size() != stages.size()) {
            throw new IllegalArgumentException("Stage DAG contains a cycle");
        }
        return moveOutputStagesToEnd(sorted, stages);
    }

    /**
     * Validates that the stage graph contains no cycles.
     */
    public void validateDag(List<StageInfo> stages) {
        Map<String, Integer> inDegree = buildInDegreeMap(stages);
        List<String> sorted = topologicalSort(stages, inDegree);
        if (sorted.size() != stages.size()) {
            throw new IllegalArgumentException("Stage DAG contains a cycle");
        }
    }

    private Map<String, Integer> buildInDegreeMap(List<StageInfo> stages) {
        Map<String, Integer> inDegree = new HashMap<>();
        stages.forEach(s -> inDegree.put(s.stageId(), 0));
        stages.forEach(s -> s.dependencyStageIds().forEach(dep ->
                inDegree.merge(s.stageId(), 1, Integer::sum)));
        return inDegree;
    }

    private List<String> topologicalSort(List<StageInfo> stages, Map<String, Integer> inDegree) {
        Map<String, Integer> degree = new HashMap<>(inDegree);
        Map<String, List<String>> dependents = buildDependentsMap(stages);
        Queue<String> queue = new ArrayDeque<>();
        degree.forEach((id, d) -> { if (d == 0) queue.add(id); });

        List<String> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            result.add(current);
            for (String dep : dependents.getOrDefault(current, List.of())) {
                if (degree.merge(dep, -1, Integer::sum) == 0) {
                    queue.add(dep);
                }
            }
        }
        return result;
    }

    private Map<String, List<String>> buildDependentsMap(List<StageInfo> stages) {
        Map<String, List<String>> dependents = new HashMap<>();
        for (StageInfo stage : stages) {
            for (String dep : stage.dependencyStageIds()) {
                dependents.computeIfAbsent(dep, k -> new ArrayList<>()).add(stage.stageId());
            }
        }
        return dependents;
    }

    private List<String> moveOutputStagesToEnd(List<String> sorted, List<StageInfo> stages) {
        var outputIds = stages.stream()
                .filter(StageInfo::isOutput)
                .map(StageInfo::stageId)
                .collect(java.util.stream.Collectors.toSet());
        List<String> result = new ArrayList<>();
        sorted.stream().filter(id -> !outputIds.contains(id)).forEach(result::add);
        sorted.stream().filter(outputIds::contains).forEach(result::add);
        return result;
    }
}
