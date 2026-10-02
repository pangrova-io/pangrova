package org.pangrova.coordinator.scheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that pangrova-coordinator production code does not import
 * pangrova-worker internal classes. Coordinator and Worker interact
 * exclusively via pangrova-protocol REST DTOs and pangrova-spi contracts.
 *
 * <p>Per pangrova-rules, the only allowed compile-scope worker import
 * is {@code org.pangrova.worker.transport.*}. The standalone dev-mode
 * launcher ({@code PangrovaStandaloneMain}) is excluded because it
 * intentionally bridges both processes in a single JVM.</p>
 */
class CoordinatorWorkerSpiAbstractionTest {

    private static final Path COORDINATOR_SRC =
            Path.of("pangrova-coordinator/src/main/java");

    private static final String WORKER_IMPORT_PREFIX = "import org.pangrova.worker.";

    /** Worker transport package is the only allowed compile-scope dependency (pangrova-rules). */
    private static final String ALLOWED_TRANSPORT_PREFIX = "import org.pangrova.worker.transport.";

    @Test
    void coordinatorProductionCode_doesNotImportWorkerInternals() throws IOException {
        List<String> violations = scanForViolations(COORDINATOR_SRC, WORKER_IMPORT_PREFIX);

        assertThat(violations)
                .as("Coordinator production code must not import pangrova-worker classes "
                        + "(except org.pangrova.worker.transport.* per pangrova-rules). "
                        + "Use pangrova-protocol DTOs or pangrova-spi contracts instead.")
                .isEmpty();
    }

    @Test
    void submitTaskRequestConverter_isSelfContained() {
        // Verify the converter does not delegate to worker's TaskRequestConverter
        // by checking it can convert without any worker dependency at runtime
        var request = org.pangrova.coordinator.CanonicalTaskRequestTestFixture.request(
                "t-1", "q-1", 0, 0,
                org.pangrova.protocol.v2.TaskKind.TASK_KIND_READ,
                "plan".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        var descriptor = SubmitTaskRequestConverter.toTaskDescriptor(request);

        assertThat(descriptor.queryId().toString()).isEqualTo("q-1");
        assertThat(descriptor.taskType()).isEqualTo("COMPUTE");
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
                    .filter(line -> !line.trim().startsWith(ALLOWED_TRANSPORT_PREFIX))
                    .map(line -> javaFile.getFileName() + ": " + line.trim())
                    .toList();
        } catch (IOException e) {
            return List.of("Failed to read: " + javaFile);
        }
    }
}
