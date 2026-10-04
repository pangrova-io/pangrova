package org.pangrova.testing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Executes the actual generator with only the early Linux Docker bootstrap's declared files. */
class NativeBuildBootstrapContractTest {
    private static final Path ROOT = repositoryRoot();
    private static final String TOOLS = "pangrova-protocol/tools/function-contracts/";
    private static final String DECLARATION =
            "pangrova-protocol/src/main/resources/function-contracts/execution-selection.json";

    @Test
    void earlyDockerBootstrapContainsTheActualGeneratorImportClosure(@TempDir Path stage) throws Exception {
        copyEarlyBootstrap(stage);
        var result = generate(stage);
        assertThat(result.exit()).as(result.output()).isZero();
        assertThat(Files.readString(stage.resolve("ExecutionSelection.h")))
                .contains("selectExecution", "executionSelectionSha256");
    }

    @Test
    void missingActualImportMustFailInsteadOfProducingAnUnverifiedHeader(@TempDir Path stage) throws Exception {
        copyEarlyBootstrap(stage);
        Files.deleteIfExists(stage.resolve(TOOLS + "registered_signature.py"));
        var result = generate(stage);
        assertThat(result.exit()).isNotZero();
        assertThat(result.output()).contains("ModuleNotFoundError", "registered_signature");
        assertThat(stage.resolve("ExecutionSelection.h")).doesNotExist();
    }

    private static void copyEarlyBootstrap(Path stage) throws Exception {
        var source = Files.readString(ROOT.resolve("deploy/docker/Dockerfile.native-build"));
        int build = source.indexOf("RUN GIT_CONFIG_COUNT=1");
        assertThat(build).as("Actual SDK bootstrap must be present").isPositive();
        var copy = Pattern.compile("^COPY (pangrova-protocol/\\S+\\.(?:py|json)) (/workspace/\\S+)$");
        for (var line : source.substring(0, build).split("\\R")) {
            var match = copy.matcher(line);
            if (!match.matches()) continue;
            var target = stage.resolve(match.group(2).substring("/workspace/".length()));
            Files.createDirectories(target.getParent());
            Files.copy(ROOT.resolve(match.group(1)), target);
        }
    }

    private static Generation generate(Path stage) throws Exception {
        var output = stage.resolve("generator.log");
        var process = new ProcessBuilder(System.getProperty("pangrova.function-contract.python", "python3"),
                stage.resolve(TOOLS + "generate_execution_selection.py").toString(),
                "--declaration", stage.resolve(DECLARATION).toString(),
                "--cpp-output", stage.resolve("ExecutionSelection.h").toString())
                .directory(stage.toFile()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("Generator must actually exit").isTrue();
            return new Generation(process.exitValue(), Files.readString(output));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static Path repositoryRoot() {
        var current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.isDirectory(current.resolve(".pangrova"))) {
            current = current.getParent();
        }
        if (current == null) throw new IllegalStateException("Repository root not found");
        return current;
    }

    private record Generation(int exit, String output) {}
}
