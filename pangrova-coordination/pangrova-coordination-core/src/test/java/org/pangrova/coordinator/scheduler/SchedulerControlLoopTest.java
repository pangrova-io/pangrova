// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

package org.pangrova.coordinator.scheduler;

import org.pangrova.scheduler.api.placement.NoOpSchedulerControlLoop;


import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.pangrova.scheduler.api.placement.ClusterSnapshot;
import org.pangrova.scheduler.api.placement.ControlLoopAction;
import org.pangrova.scheduler.api.placement.SchedulerControlLoop;
import org.pangrova.scheduler.api.placement.SchedulingWorkerSnapshot;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SchedulerControlLoop} and its
 * {@link NoOpSchedulerControlLoop} default implementation.
 */
class SchedulerControlLoopTest {

    @Test
    void noOpReturnsEmptyActions() {
        SchedulerControlLoop loop = new NoOpSchedulerControlLoop();
        ClusterSnapshot snapshot = new ClusterSnapshot(
                List.of(new SchedulingWorkerSnapshot("w1", "host1", 8081, 1024, 2)),
                5, 0, Instant.now());

        List<ControlLoopAction> actions = loop.evaluate(snapshot);
        assertThat(actions).isEmpty();
    }

    @Test
    void noOpReturnsNoOpName() {
        SchedulerControlLoop loop = new NoOpSchedulerControlLoop();
        assertThat(loop.name()).isEqualTo("no-op");
    }

    @Test
    void noOpHandlesEmptyCluster() {
        SchedulerControlLoop loop = new NoOpSchedulerControlLoop();
        ClusterSnapshot emptyCluster = new ClusterSnapshot(
                List.of(), 0, 0, Instant.now());

        List<ControlLoopAction> actions = loop.evaluate(emptyCluster);
        assertThat(actions).isEmpty();
    }
}
