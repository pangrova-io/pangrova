package org.pangrova.query.recovery;

import java.time.Instant;

import org.pangrova.scheduler.api.enums.HealthState;

/** Worker-liveness observation used by journal recovery after failover. */
public interface RecoveredWorkerHealth {

    HealthState evaluateHealth(String workerId, Instant now);

    boolean tryRecoverFromRegistry(String workerId);
}
