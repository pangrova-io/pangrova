// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.scheduler.core.control;

import org.pangrova.scheduler.api.placement.ClusterSnapshot;
import org.pangrova.scheduler.api.placement.ControlLoopAction;
import org.pangrova.scheduler.spi.control.SchedulerControlLoop;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

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
