package org.pangrova.materialization.api.port;

import org.pangrova.common.api.annotation.Experimental;

import java.time.Duration;
import org.pangrova.materialization.api.domain.ExecutionFence;
import org.pangrova.materialization.api.domain.MaterializationId;

/**
 * Issues and validates execution leases.
 *
 * <p>Fences bind the coordinator execution anchor, leader control epoch, and a
 * bounded lease. Durable writes validate the fence; an expired fence rejects
 * publication even if the data-plane commit succeeded.
 */
@Experimental
public interface LeaseService {

    /**
     * Issues a fence for one materialization operation.
     *
     * @param object target object
     * @param leaseDuration lease duration
     * @return execution fence
     */
    ExecutionFence issueExecutionFence(MaterializationId object, Duration leaseDuration);
}
