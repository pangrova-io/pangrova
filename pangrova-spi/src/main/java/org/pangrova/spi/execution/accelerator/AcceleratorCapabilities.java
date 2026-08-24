package org.pangrova.spi.execution.accelerator;

import java.util.Set;

import org.pangrova.spi.annotation.Experimental;

/**
 * Describes what a hardware accelerator session can do.
 *
 * <p>The scheduler and execution engine inspect these capabilities to decide
 * whether a particular operator (e.g. hash-join, aggregation, sort) can be
 * offloaded to the accelerator or must remain on the CPU path.</p>
 *
 * @param supportedOperators operator names this accelerator can execute
 *                           (e.g. {@code "hash_join"}, {@code "aggregation"})
 * @param maxBatchSizeBytes  maximum input batch size the accelerator accepts, in bytes
 * @param memoryLimitBytes   total device memory available for execution, in bytes
 */
@Experimental(reason = "G-EXEC-08 hardware accelerator offload SPI")
public record AcceleratorCapabilities(
        Set<String> supportedOperators,
        long maxBatchSizeBytes,
        long memoryLimitBytes) {

    public AcceleratorCapabilities {
        supportedOperators = supportedOperators == null
                ? Set.of()
                : Set.copyOf(supportedOperators);
        if (maxBatchSizeBytes < 0) {
            throw new IllegalArgumentException(
                    "maxBatchSizeBytes must be non-negative, got: " + maxBatchSizeBytes);
        }
        if (memoryLimitBytes < 0) {
            throw new IllegalArgumentException(
                    "memoryLimitBytes must be non-negative, got: " + memoryLimitBytes);
        }
    }

    /**
     * Returns whether this accelerator supports the given operator.
     *
     * @param operatorName the operator to check
     * @return true if the operator can be offloaded
     */
    public boolean supportsOperator(String operatorName) {
        return supportedOperators.contains(operatorName);
    }
}
