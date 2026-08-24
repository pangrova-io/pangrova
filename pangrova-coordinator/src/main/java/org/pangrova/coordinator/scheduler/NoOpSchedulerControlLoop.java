// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.coordinator.scheduler;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.execution.scheduling.ClusterSnapshot;
import org.pangrova.spi.execution.scheduling.ControlLoopAction;
import org.pangrova.spi.execution.scheduling.SchedulerControlLoop;

/**
 * Default no-op implementation of {@link SchedulerControlLoop} that never
 * emits any actions.
 *
 * <p>This is the default control loop used when no custom implementation is
 * configured. It preserves the current behaviour where the coordinator does
 * not perform reactive cluster management — each query is scheduled
 * independently without cross-query coordination or auto-scaling.</p>
 *
 * <p>To enable reactive scheduling, configure a real implementation via
 * the {@code coordinator.scheduler.control-loop} configuration key.</p>
 */
@Experimental(reason = "G-SCHED-08: default no-op control loop preserving current behaviour")
public final class NoOpSchedulerControlLoop implements SchedulerControlLoop {

    @Override
    public List<ControlLoopAction> evaluate(ClusterSnapshot snapshot) {
        return List.of();
    }

    @Override
    public String name() {
        return "no-op";
    }
}
