package org.pangrova.spi.resource.scaling;

import org.pangrova.spi.annotation.Experimental;

/**
 * Thrown when a {@link ScalingExecutor} fails to apply a scaling decision.
 */
@Experimental(reason = "G-RES-03: auto-scaling SPI")
public class ScalingException extends RuntimeException {

    public ScalingException(String message) {
        super(message);
    }

    public ScalingException(String message, Throwable cause) {
        super(message, cause);
    }
}
