package org.pangrova.streaming.cdc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the streaming family (pangrova-streaming-runtime and the
 * format/maintenance/jdbc leaves) does not import pangrova-coordinator or
 * pangrova-worker internal classes. The runtime module depends only on
 * pangrova-spi, pangrova-streaming-maintenance and pangrova-common.
 *
 * <p>Validates: Requirements 3.5, 3.7</p>
 */
class StreamingContextBoundaryTest {

    /** Repository root relative to this module's Maven basedir (two levels up). */
    private static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();

    private static final List<Path> STREAMING_SRC = List.of(
            REPO_ROOT.resolve("pangrova-streaming/pangrova-streaming-runtime/src/main/java"),
            REPO_ROOT.resolve("pangrova-streaming/pangrova-streaming-format/src/main/java"),
            REPO_ROOT.resolve("pangrova-streaming/pangrova-streaming-maintenance/src/main/java"),
            REPO_ROOT.resolve("pangrova-streaming/pangrova-streaming-jdbc/src/main/java"));

    @Test
    void streamingCode_doesNotImportCoordinatorInternals() throws IOException {
        for (Path root : requireExisting(STREAMING_SRC)) {
            List<String> violations = scanForViolations(root, "import org.pangrova.coordinator.");
            assertThat(violations)
                    .as("Streaming leaves must not import pangrova-coordinator classes. "
                            + "Use pangrova-spi contracts instead.")
                    .isEmpty();
        }
    }

    @Test
    void streamingCode_doesNotImportWorkerInternals() throws IOException {
        for (Path root : requireExisting(STREAMING_SRC)) {
            List<String> violations = scanForViolations(root, "import org.pangrova.worker.");
            assertThat(violations)
                    .as("Streaming leaves must not import pangrova-worker classes. "
                            + "Use pangrova-spi contracts instead.")
                    .isEmpty();
        }
    }

    private static List<Path> requireExisting(List<Path> roots) {
        for (Path root : roots) {
            assertThat(Files.isDirectory(root))
                    .as("Streaming source root must exist so boundary checks cannot pass vacuously: %s",
                            root)
                    .isTrue();
        }
        return roots;
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
