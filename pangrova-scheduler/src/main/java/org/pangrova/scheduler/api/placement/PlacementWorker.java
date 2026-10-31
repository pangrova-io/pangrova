package org.pangrova.scheduler.api.placement;

import java.util.List;

import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.common.value.MemoryPressure;

/**
 * Read-only view of a worker used by placement and selection.
 * Implemented by the coordinator's worker inventory so scheduling never depends
 * on the inventory owner.
 */
public interface PlacementWorker {

    String workerId();

    String host();

    int httpPort();

    HealthState healthState();

    MemoryPressure memoryPressure();

    double cpuUtilization();

    int runningTaskCount();

    long availableMemoryBytes();

    String resourcePoolId();

    String rackId();

    List<String> dataLocality();

    CapabilityInventory capabilityInventory();
}
