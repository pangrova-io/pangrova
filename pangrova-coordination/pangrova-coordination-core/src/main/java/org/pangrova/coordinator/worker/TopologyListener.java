package org.pangrova.coordinator.worker;

import java.util.List;

/**
 * Listener for worker topology changes (join, leave, health state transitions).
 * Implementations are notified whenever the set of active workers changes.
 */
public interface TopologyListener {

    /**
     * Called when the active worker set changes.
     *
     * @param activeWorkers current list of active workers after the change
     */
    void onTopologyChanged(List<WorkerInfo> activeWorkers);
}
