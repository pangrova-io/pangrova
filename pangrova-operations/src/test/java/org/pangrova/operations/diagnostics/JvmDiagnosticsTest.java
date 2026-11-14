package org.pangrova.operations.diagnostics;

import org.pangrova.operations.enums.JfrState;
import org.pangrova.operations.exception.DiagnosticException;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for JvmDiagnostics covering threaddump, heapdump, and JFR operations.
 */
class JvmDiagnosticsTest {

    @TempDir
    Path tempDir;

    private static final CallerContext OPERATOR = new CallerContext("operator1", Set.of("operator"));

    private DiagnosticConfig config;
    private JvmDiagnostics endpoint;

    @BeforeEach
    void setUp() {
        config = new DiagnosticConfig(
                tempDir.resolve("heapdumps"),
                true,
                tempDir.resolve("jfr"),
                Duration.ofSeconds(5),
                "default"
        );
        endpoint = new JvmDiagnostics(config);
    }

    @AfterEach
    void closeDiagnostics() {
        endpoint.close();
    }

    @Test
    void heapDumpRequiresAuthorizationBeforeCreatingAnyFiles() {
        assertThatThrownBy(() -> endpoint.heapDump(null))
                .isInstanceOf(DiagnosticException.class).hasMessageContaining("Authentication required");
        assertThatThrownBy(() -> endpoint.heapDump(new CallerContext("reader", Set.of("reader"))))
                .isInstanceOf(DiagnosticException.class).hasMessageContaining("Access denied");
        assertThat(config.heapDumpOutputDir()).doesNotExist();
    }

    @Test
    void threadDumpReturnsNonEmptyResult() {
        ThreadDumpResult result = endpoint.threadDump();

        assertThat(result.threadCount()).isGreaterThan(0);
        assertThat(result.dump()).contains("Thread dump at");
        assertThat(result.dump()).contains("Total threads:");
        assertThat(result.timestamp()).isNotNull();
    }

    @Test
    void threadDumpContainsCurrentThread() {
        ThreadDumpResult result = endpoint.threadDump();

        assertThat(result.dump()).contains("main");
    }

    @Test
    void heapDumpWritesFileToConfiguredDirectory() throws DiagnosticException {
        HeapDumpResult result = endpoint.heapDump(OPERATOR);

        assertThat(result.filePath()).startsWith(tempDir.resolve("heapdumps").toString());
        assertThat(result.filePath()).endsWith(".hprof");
        assertThat(result.fileSizeBytes()).isGreaterThan(0);
        assertThat(result.timestamp()).isNotNull();
        assertThat(Path.of(result.filePath())).exists();
    }

    @Test
    void heapDumpCreatesOutputDirectoryIfMissing() throws DiagnosticException {
        Path nestedDir = tempDir.resolve("deep/nested/heapdumps");
        DiagnosticConfig nestedConfig = new DiagnosticConfig(
                nestedDir, true, tempDir.resolve("jfr"),
                Duration.ofSeconds(5), "default");
        try (var nestedEndpoint = new JvmDiagnostics(nestedConfig)) {
            HeapDumpResult result = nestedEndpoint.heapDump(OPERATOR);
            assertThat(Path.of(result.filePath())).exists();
            assertThat(nestedDir).exists();
        }
    }

    @Test
    void jfrStartAndStopProducesRecordingFile() throws DiagnosticException {
        JfrStartRequest startRequest = new JfrStartRequest(Duration.ofSeconds(30), "default");
        JfrResult startResult = endpoint.jfrStart(startRequest);

        assertThat(startResult.recordingId()).isGreaterThan(0);
        assertThat(startResult.state()).isEqualTo(JfrState.RUNNING);
        assertThat(startResult.outputPath()).endsWith(".jfr");
        assertThat(startResult.duration()).isEqualTo(Duration.ofSeconds(30));

        JfrStopRequest stopRequest = new JfrStopRequest(startResult.recordingId());
        JfrResult stopResult = endpoint.jfrStop(stopRequest);

        assertThat(stopResult.recordingId()).isEqualTo(startResult.recordingId());
        assertThat(stopResult.state()).isEqualTo(JfrState.STOPPED);
        assertThat(Path.of(stopResult.outputPath())).exists();
    }

    @Test
    void jfrStartUsesConfigDefaultsWhenRequestFieldsNull() throws DiagnosticException {
        JfrStartRequest request = JfrStartRequest.withDefaults();
        JfrResult result = endpoint.jfrStart(request);

        assertThat(result.state()).isEqualTo(JfrState.RUNNING);
        assertThat(result.duration()).isEqualTo(Duration.ofSeconds(5));

        endpoint.jfrStop(new JfrStopRequest(result.recordingId()));
    }

    @Test
    void jfrStopThrowsForUnknownRecordingId() {
        assertThatThrownBy(() -> endpoint.jfrStop(new JfrStopRequest(99999)))
                .isInstanceOf(DiagnosticException.class)
                .hasMessageContaining("99999");
    }

    @Test
    void diagnosticConfigRejectsNullFields() {
        assertThatThrownBy(() -> new DiagnosticConfig(null, true, tempDir, Duration.ofSeconds(1), "default"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DiagnosticConfig(tempDir, true, null, Duration.ofSeconds(1), "default"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DiagnosticConfig(tempDir, true, tempDir, null, "default"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void diagnosticConfigRejectsNonPositiveDuration() {
        assertThatThrownBy(() -> new DiagnosticConfig(tempDir, true, tempDir, Duration.ZERO, "default"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DiagnosticConfig(tempDir, true, tempDir, Duration.ofSeconds(-1), "default"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void jfrStopRequestRejectsNonPositiveId() {
        assertThatThrownBy(() -> new JfrStopRequest(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JfrStopRequest(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void diagnosticConfigWithDefaultsCreatesValidConfig() {
        DiagnosticConfig defaults = DiagnosticConfig.withDefaults(tempDir);

        assertThat(defaults.heapDumpOutputDir()).isEqualTo(tempDir.resolve("heapdumps"));
        assertThat(defaults.jfrOutputDir()).isEqualTo(tempDir.resolve("jfr"));
        assertThat(defaults.heapDumpLiveObjectsOnly()).isTrue();
        assertThat(defaults.jfrDefaultDuration()).isEqualTo(Duration.ofMinutes(1));
        assertThat(defaults.jfrDefaultSettings()).isEqualTo("default");
    }
}
