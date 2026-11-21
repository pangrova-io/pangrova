package org.pangrova.coordinator.streaming;

import org.pangrova.protocol.v2.TaskRequest;

/** Sends one planned CDC task through the Coordinator's real Worker transport. */
@FunctionalInterface
public interface CdcTaskDispatchPort {
    void dispatch(TaskRequest request);
}
