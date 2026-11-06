package org.pangrova.execution.api.stage;

import java.util.Objects;

/** Immutable execution identity and purpose, independent of any planner tree or scheduling state. */
public record TaskStage(int stageId, TaskPurpose purpose) {
    public TaskStage {
        if (stageId < 0) throw new IllegalArgumentException("Stage identity must be non-negative");
        Objects.requireNonNull(purpose, "purpose");
    }
}
