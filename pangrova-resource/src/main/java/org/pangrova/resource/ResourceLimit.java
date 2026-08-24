package org.pangrova.resource;

import java.nio.file.Path;

/**
 * Resource limits for a task: memory, threads, spill budget, and spill directory.
 */
public record ResourceLimit(
        long maxMemoryBytes,
        int maxThreads,
        long maxSpillBytes,
        String spillDirectory) {

    public static final ResourceLimit DEFAULT = new ResourceLimit(
            1L * 1024 * 1024 * 1024,
            4,
            10L * 1024 * 1024 * 1024,
            Path.of(System.getProperty("java.io.tmpdir"), "pangrova", "spill").toString());
}
