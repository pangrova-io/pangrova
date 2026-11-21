package org.pangrova.testing.testkit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The release gate must reject success claims that do not observe execution. */
class E2eTruthfulnessGateTest {
    @TempDir
    Path sources;

    @ParameterizedTest
    @ValueSource(strings = {
            "assertThat(true).isTrue();",
            "Assumptions.assumeTrue(runtimeAvailable);",
            "try { execute(); } catch (AssertionError | RuntimeException e) { }",
            "assertThat(error).doesNotContainIgnoringCase(\"syntax error\");",
            "assertThat(rows.size())\n    .isGreaterThanOrEqualTo(0);",
            "try { execute(); } catch (RuntimeException e) { return List.of(); }",
            "@Tag(\"multi-worker\") class SampleTest extends SqlE2ETestBase {}"
    })
    void rejectsMaskedFailuresAndSimulatedWorkers(String source) throws Exception {
        assertGateRejects(source);
    }

    @Test
    void acceptsResultAssertionsAndExplicitNegativeContracts() throws Exception {
        assertThat(runGate("assertThat(executeQuery(sql)).hasSize(2);\n"
                + "assertThat(executeExpectingError(badSql)).contains(\"division by zero\");"))
                .isZero();
    }

    @Test
    void rejectsFailureMaskingInBaseClasses() throws Exception {
        Files.writeString(sources.resolve("SampleTestBase.java"),
                "try { execute(); } catch (RuntimeException e) { return List.of(); }");
        assertGateRejects("assertThat(executeQuery(sql)).hasSize(2);");
    }

    @Test
    void passesWhenRipgrepIsUnavailable() throws Exception {
        int exitCode = runGate("assertThat(executeQuery(sql)).hasSize(2);", "/bin:/usr/bin");
        assertThat(exitCode)
                .withFailMessage("truthfulness gate output: %s", Files.readString(sources.resolve("gate.log")))
                .isZero();
        assertThat(Files.readString(sources.resolve("gate.log")))
                .contains("E2E truthfulness gate passed");
    }

    private void assertGateRejects(String source) throws Exception {
        int exitCode = runGate(source);
        assertThat(exitCode)
                .withFailMessage("truthfulness gate output: %s", Files.readString(sources.resolve("gate.log")))
                .isEqualTo(1);
    }

    private int runGate(String source) throws Exception {
        return runGate(source, null);
    }

    private int runGate(String source, String path) throws Exception {
        Files.writeString(sources.resolve("SampleTest.java"), source);
        var builder = new ProcessBuilder("/bin/bash", TestProjectPaths.findProjectRoot()
                .resolve(".pangrova/scripts/check-e2e-truthfulness.sh").toString());
        builder.environment().put("E2E_TRUTHFULNESS_ROOT", sources.toString());
        if (path != null) {
            builder.environment().put("PATH", path);
        }
        builder.redirectErrorStream(true);
        builder.redirectOutput(sources.resolve("gate.log").toFile());
        Process process = builder.start();
        try {
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).as("gate must terminate").isTrue();
            return process.exitValue();
        } finally {
            process.destroyForcibly();
        }
    }
}
