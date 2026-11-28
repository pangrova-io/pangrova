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

import org.pangrova.planner.enums.StagePlanMode;
import org.pangrova.planner.analysis.StagePlanAccess;
import org.pangrova.protocol.rest.task.exchange.ExchangeSourceDescriptor;

import java.util.List;

import static org.pangrova.common.validation.Preconditions.checkNotNull;

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
        var frozen = stagePlan.frozenCoordinates();
        if (stagePlan.mode() == StagePlanMode.DURABLE_REPLAY && frozen.isEmpty()) {
            throw new IllegalArgumentException("Durable stage plan has no frozen execution coordinates");
        }
        frozen.ifPresent(coordinates -> coordinates.requireMatch(stageId, parallelism, taskType));
    }

    /**
     * Convenience constructor without planner statistics.
     * Estimated values default to -1 (unavailable).
     */
    public StageNode(int stageId, StagePlanAccess stagePlan, int parallelism,
                     String taskType, List<ExchangeSourceDescriptor> exchangeSources) {
        this(stageId, stagePlan, parallelism, taskType, exchangeSources, -1L, -1L);
    }

}
