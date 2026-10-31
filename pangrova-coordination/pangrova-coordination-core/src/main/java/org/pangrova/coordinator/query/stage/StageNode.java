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

import org.pangrova.planner.StagePlanAccess;
import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;

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
        StagePlanAccess stagePlan,
        int parallelism,
        String taskType,
        List<ExchangeSourceDescriptor> exchangeSources,
        long estimatedShuffleBytes,
        long estimatedShuffleRows) {

    public StageNode {
        checkNotNull(stagePlan, "stagePlan must not be null");
        checkNotNull(taskType, "taskType must not be null");
        exchangeSources = exchangeSources == null ? List.of() : List.copyOf(exchangeSources);
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1, got " + parallelism);
        }
        if (stagePlan instanceof org.pangrova.planner.AuthorizedStageGraph.SerializedStage replay
                && (stageId != replay.stageId() || parallelism != replay.parallelism()
                        || !taskType.equals(replay.taskType()))) {
            throw new IllegalArgumentException("Durable stage coordinates differ from its persisted graph identity");
        }
        requirePersistedCoordinates(stagePlan, stageId, parallelism, taskType);
    }

    private static void requirePersistedCoordinates(StagePlanAccess plan, int stage, int parallelism, String type) {
        if (plan instanceof org.pangrova.coordinator.databuild.template.PersistedStagePlan persisted
                && (stage != persisted.stage().stageId() || parallelism != persisted.stage().parallelism()
                        || !type.equals(persisted.taskType()))) {
            throw new IllegalArgumentException("Durable stage coordinates differ from its immutable metadata graph");
        }
    }

    /**
     * Convenience constructor without planner statistics.
     * Estimated values default to -1 (unavailable).
     */
    public StageNode(int stageId, StagePlanAccess stagePlan, int parallelism,
                     String taskType, List<ExchangeSourceDescriptor> exchangeSources) {
        this(stageId, stagePlan, parallelism, taskType, exchangeSources, -1L, -1L);
    }

    /**
     * Returns a copy with adjusted parallelism for adaptive dynamic parallelism.
     */
    public StageNode withParallelism(int newParallelism) {
        if (stagePlan.mode() == StagePlanAccess.Mode.DURABLE_REPLAY && newParallelism != parallelism) {
            throw new IllegalStateException("Durable stage parallelism is part of its persisted graph identity");
        }
        return new StageNode(stageId, stagePlan, newParallelism, taskType,
                exchangeSources, estimatedShuffleBytes, estimatedShuffleRows);
    }

    /**
     * Returns a copy with a reoptimized plan fragment for sub-plan re-optimization.
     */
    public StageNode withStagePlan(StagePlanAccess newStagePlan) {
        if (stagePlan.mode() == StagePlanAccess.Mode.DURABLE_REPLAY) {
            throw new IllegalStateException("Durable stage plan cannot be adaptively replaced");
        }
        return new StageNode(stageId, newStagePlan, parallelism, taskType,
                exchangeSources, estimatedShuffleBytes, estimatedShuffleRows);
    }
}
