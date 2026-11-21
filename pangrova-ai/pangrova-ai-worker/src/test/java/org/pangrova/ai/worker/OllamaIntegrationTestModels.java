package org.pangrova.ai.worker;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Reuses real Ollama model bytes while keeping each integration server isolated. */
public final class OllamaIntegrationTestModels {
    private OllamaIntegrationTestModels() { }

    public static void mountCache(GenericContainer<?> container) throws IOException {
        String configured = System.getProperty("pangrova.test.ollamaModelCache");
        Path cache = configured == null
                ? Path.of(System.getProperty("user.dir"), "target", "ollama-models")
                : Path.of(configured);
        Files.createDirectories(cache);
        container.withFileSystemBind(cache.toAbsolutePath().normalize().toString(), "/root/.ollama/models");
    }

    public static void requireModel(GenericContainer<?> container, String model) throws Exception {
        if (container.execInContainer("ollama", "show", model).getExitCode() != 0) {
            var pull = container.execInContainer("ollama", "pull", model);
            assertThat(pull.getExitCode()).as("Actual model pull for %s: %s", model, pull.getStderr()).isZero();
        }
        var loaded = container.execInContainer("ollama", "show", model);
        assertThat(loaded.getExitCode()).as("Actual Ollama model inventory for %s: %s", model,
                loaded.getStderr()).isZero();
    }
}
