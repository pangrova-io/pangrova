package org.pangrova.query.core.recovery;

import java.time.Instant;

import org.pangrova.scheduler.api.placement.HealthState;

/** Worker-liveness observation used by journal recovery after failover. */
public interface RecoveredWorkerHealth {

    HealthState evaluateHealth(String workerId, Instant now);

    boolean tryRecoverFromRegistry(String workerId);
}
