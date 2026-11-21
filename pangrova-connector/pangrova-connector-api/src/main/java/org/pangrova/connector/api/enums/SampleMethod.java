package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Sampling method.
 */
@Experimental(reason = "G-CONN-06: sample pushdown SPI")
public enum SampleMethod {
    /** Bernoulli (row-level) sampling. */
    BERNOULLI,
    /** System (block-level) sampling -- faster but less uniform. */
    SYSTEM
}
