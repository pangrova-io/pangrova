package org.pangrova.spi.resource.scaling;

import org.pangrova.spi.annotation.Experimental;

/**
 * SPI for executing scaling actions decided by an {@link AutoScaler}.
 *
 * <p>Implementations translate a {@link ScalingDecision} into actual infrastructure
 * changes (Kubernetes replica set adjustment, cloud VM provisioning, etc.).
 * The executor is expected to be idempotent: setting the target to N twice
 * in succession must be harmless.</p>
 *
 * <p>Security gate: implementations should enforce authorisation checks before
 * executing scaling (e.g. the caller of {@code CALL system.scale_out} must
 * hold the required privilege).</p>
 */
@Experimental(reason = "G-RES-03: auto-scaling SPI — KubernetesScalingExecutor not yet backed by real K8s client")
public interface ScalingExecutor {

    /**
     * Applies the given scaling decision to the cluster infrastructure.
     *
     * <p>The implementation is idempotent: scaling to the same target
     * repeatedly has no adverse effect.</p>
     *
     * @param decision the scaling decision to execute
     * @throws ScalingException if the scaling operation fails
     */
    void execute(ScalingDecision decision);

    /**
     * Returns the name of the backing infrastructure
     * (e.g. "kubernetes", "aws-autoscaling", "manual").
     *
     * @return executor backend name, never null or blank
     */
    String backend();
}
