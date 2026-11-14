// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.scheduler.control;

import org.pangrova.scheduler.api.control.ClusterSnapshot;
import org.pangrova.scheduler.api.enums.ControlLoopAction;
import org.pangrova.scheduler.api.control.SchedulerControlLoop;

import java.util.List;

import org.pangrova.common.api.annotation.Experimental;

/** A control-loop implementation that always returns an empty action list. */
@Experimental(reason = "G-SCHED-08: experimental no-op control loop")
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
