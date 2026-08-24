package org.pangrova.spi.execution;

import java.util.Set;

/**
 * Describes the execution capabilities of a task executor.
 * Used by the scheduler to match tasks to workers with sufficient resources.
 *
 * @param maxConcurrentTasks maximum number of tasks that can run concurrently
 * @param supportedFormats   set of supported data formats (e.g. "parquet", "orc", "arrow")
 * @param memoryLimitBytes   maximum memory available for task execution in bytes
 */
public record ExecutionCapabilities(
        int maxConcurrentTasks,
        Set<String> supportedFormats,
        long memoryLimitBytes) {

    public ExecutionCapabilities {
        if (maxConcurrentTasks < 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentTasks must be non-negative, got: " + maxConcurrentTasks);
        }
        supportedFormats = supportedFormats == null ? Set.of() : Set.copyOf(supportedFormats);
        if (memoryLimitBytes < 0) {
            throw new IllegalArgumentException(
                    "memoryLimitBytes must be non-negative, got: " + memoryLimitBytes);
        }
    }

    /**
     * Returns capabilities representing an unconstrained executor.
     * Used as the default when an executor does not report its limits.
     */
    public static ExecutionCapabilities unlimited() {
        return new ExecutionCapabilities(Integer.MAX_VALUE, Set.of(), Long.MAX_VALUE);
    }
}
