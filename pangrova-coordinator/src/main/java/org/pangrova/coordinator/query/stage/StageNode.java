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
package org.pangrova.coordinator.query.stage;

import org.pangrova.planner.PlanFragment;
import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;

import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Represents a single execution stage in a distributed query plan.
 * Each stage is a sub-tree of the logical plan that executes on one or more Worker partitions.
 *
 * @param estimatedShuffleBytes estimated output data size from planner statistics; -1 if unavailable
 * @param estimatedShuffleRows  estimated output row count from planner statistics; -1 if unavailable
 */
public record StageNode(
        int stageId,
        PlanFragment planFragment,
        int parallelism,
        String taskType,
        List<ExchangeSourceDescriptor> exchangeSources,
        long estimatedShuffleBytes,
        long estimatedShuffleRows) {

    public StageNode {
        checkNotNull(planFragment, "planFragment must not be null");
        checkNotNull(taskType, "taskType must not be null");
        exchangeSources = exchangeSources == null ? List.of() : List.copyOf(exchangeSources);
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1, got " + parallelism);
        }
    }

    /**
     * Convenience constructor without planner statistics.
     * Estimated values default to -1 (unavailable).
     */
    public StageNode(int stageId, PlanFragment planFragment, int parallelism,
                     String taskType, List<ExchangeSourceDescriptor> exchangeSources) {
        this(stageId, planFragment, parallelism, taskType, exchangeSources, -1L, -1L);
    }

    /**
     * Returns a copy with adjusted parallelism for adaptive dynamic parallelism.
     */
    public StageNode withParallelism(int newParallelism) {
        return new StageNode(stageId, planFragment, newParallelism, taskType,
                exchangeSources, estimatedShuffleBytes, estimatedShuffleRows);
    }

    /**
     * Returns a copy with a reoptimized plan fragment for sub-plan re-optimization.
     */
    public StageNode withPlanFragment(PlanFragment newPlanFragment) {
        return new StageNode(stageId, newPlanFragment, parallelism, taskType,
                exchangeSources, estimatedShuffleBytes, estimatedShuffleRows);
    }
}
