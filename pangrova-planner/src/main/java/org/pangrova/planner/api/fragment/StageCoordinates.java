package org.pangrova.planner.api.fragment;

import org.pangrova.common.api.annotation.Internal;

/** Exact identity frozen with a durable stage plan; zero parallelism is an empty, undispatchable cohort. */
@Internal
public record StageCoordinates(int stageId, int parallelism, String taskType) {
    public StageCoordinates {
        if (stageId < 0 || parallelism < 0 || taskType == null || taskType.isBlank()) {
            throw new IllegalArgumentException("Invalid frozen stage coordinates");
        }
    }

    /** Rejects any caller's attempt to reinterpret an immutable execution graph. */
    public void requireMatch(int actualStageId, int actualParallelism, String actualTaskType) {
        if (stageId != actualStageId || parallelism != actualParallelism || !taskType.equals(actualTaskType)) {
            throw new IllegalArgumentException("Durable stage coordinates differ from its immutable graph identity");
        }
    }
}
