package org.pangrova.testing;

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
        assertThat(runGate(source)).isEqualTo(1);
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
        assertThat(runGate("assertThat(executeQuery(sql)).hasSize(2);")).isEqualTo(1);
    }

    private int runGate(String source) throws Exception {
        Files.writeString(sources.resolve("SampleTest.java"), source);
        var builder = new ProcessBuilder("bash", TestProjectPaths.findProjectRoot()
                .resolve(".pangrova/scripts/check-e2e-truthfulness.sh").toString());
        builder.environment().put("E2E_TRUTHFULNESS_ROOT", sources.toString());
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
