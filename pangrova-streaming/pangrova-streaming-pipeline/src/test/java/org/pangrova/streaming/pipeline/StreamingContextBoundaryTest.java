package org.pangrova.streaming.pipeline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that pangrova-streaming production code does not import
 * pangrova-coordinator or pangrova-worker internal classes.
 * Streaming module only depends on stable contract artifacts, pangrova-connector, pangrova-common.
 *
 * <p>Validates: Requirements 3.5, 3.7</p>
 */
class StreamingContextBoundaryTest {

    private static final Path STREAMING_SRC =
            Path.of("pangrova-streaming/src/main/java");

    @Test
    void streamingCode_doesNotImportCoordinatorInternals() throws IOException {
        List<String> violations = scanForViolations(STREAMING_SRC, "import org.pangrova.coordinator.");

        assertThat(violations)
                .as("Streaming module must not import pangrova-coordinator classes. "
                        + "Use stable contract artifacts instead.")
                .isEmpty();
    }

    @Test
    void streamingCode_doesNotImportWorkerInternals() throws IOException {
        List<String> violations = scanForViolations(STREAMING_SRC, "import org.pangrova.worker.");

        assertThat(violations)
                .as("Streaming module must not import pangrova-worker classes. "
                        + "Use stable contract artifacts instead.")
                .isEmpty();
    }

    private static List<String> scanForViolations(Path sourceRoot, String importPrefix)
            throws IOException {
        if (!Files.exists(sourceRoot)) {
            return List.of();
        }
        try (Stream<Path> files = Files.walk(sourceRoot)) {
            return files
                    .filter(p -> p.toString().endsWith(".java"))
                    .flatMap(p -> findImportViolations(p, importPrefix).stream())
                    .toList();
        }
    }

    private static List<String> findImportViolations(Path javaFile, String importPrefix) {
        try {
            return Files.readAllLines(javaFile).stream()
                    .filter(line -> line.trim().startsWith(importPrefix))
                    .map(line -> javaFile.getFileName() + ": " + line.trim())
                    .toList();
        } catch (IOException e) {
            return List.of("Failed to read: " + javaFile);
        }
    }
}
