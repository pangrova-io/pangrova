package org.pangrova.spi.resource.scaling;

import org.pangrova.spi.annotation.Experimental;

/**
 * Direction of a scaling action decided by an {@link AutoScaler}.
 */
@Experimental(reason = "G-RES-03: auto-scaling SPI")
public enum ScalingDirection {

    /** Add workers to the cluster. */
    SCALE_OUT,

    /** Remove workers from the cluster. */
    SCALE_IN,

    /** Suspend idle workers down to the configured floor. */
    SUSPEND,

    /** No scaling change required. */
    HOLD
}
