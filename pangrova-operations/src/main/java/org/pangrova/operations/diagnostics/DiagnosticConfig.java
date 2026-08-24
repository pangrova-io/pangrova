package org.pangrova.operations.diagnostics;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration for diagnostic operations.
 */
public record DiagnosticConfig(
        Path heapDumpOutputDir,
        boolean heapDumpLiveObjectsOnly,
        Path jfrOutputDir,
        Duration jfrDefaultDuration,
        String jfrDefaultSettings
) {

    public DiagnosticConfig {
        Objects.requireNonNull(heapDumpOutputDir, "heapDumpOutputDir");
        Objects.requireNonNull(jfrOutputDir, "jfrOutputDir");
        Objects.requireNonNull(jfrDefaultDuration, "jfrDefaultDuration");
        Objects.requireNonNull(jfrDefaultSettings, "jfrDefaultSettings");
        if (jfrDefaultDuration.isNegative() || jfrDefaultDuration.isZero()) {
            throw new IllegalArgumentException("jfrDefaultDuration must be positive");
        }
    }

    /**
     * Creates a config with sensible defaults for production use.
     */
    public static DiagnosticConfig withDefaults(Path baseOutputDir) {
        return new DiagnosticConfig(
                baseOutputDir.resolve("heapdumps"),
                true,
                baseOutputDir.resolve("jfr"),
                Duration.ofMinutes(1),
                "default"
        );
    }
}
