package org.pangrova.operations.diagnostics;

import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for JfrController verifying recording start/stop lifecycle.
 */
class JfrControllerTest {

    @TempDir
    Path tempDir;

    @Test
    void startAndStopRecordingCleanly() throws Exception {
        JfrController controller = new JfrController();
        Path output = tempDir.resolve("test-recording.jfr");
        long id = controller.start(Duration.ofSeconds(30), "default", output);

        assertThat(id).isGreaterThan(0);

        Path resultPath = controller.stop(id);
        assertThat(resultPath).isEqualTo(output);
    }

    @Test
    void stopUnknownRecordingThrows() {
        JfrController controller = new JfrController();
        assertThatThrownBy(() -> controller.stop(99999))
                .isInstanceOf(DiagnosticException.class)
                .hasMessageContaining("99999");
    }

    @Test
    void multipleRecordingsCanBeStartedAndStopped() throws Exception {
        JfrController controller = new JfrController();
        Path output1 = tempDir.resolve("recording1.jfr");
        Path output2 = tempDir.resolve("recording2.jfr");
        long id1 = controller.start(Duration.ofMinutes(5), "default", output1);
        long id2 = controller.start(Duration.ofMinutes(5), "default", output2);

        assertThat(id1).isNotEqualTo(id2);

        Path result1 = controller.stop(id1);
        Path result2 = controller.stop(id2);
        assertThat(result1).isEqualTo(output1);
        assertThat(result2).isEqualTo(output2);
    }

    @Test
    void stoppingAlreadyStoppedRecordingThrows() throws Exception {
        JfrController controller = new JfrController();
        Path output = tempDir.resolve("recording.jfr");
        long id = controller.start(Duration.ofMinutes(5), "default", output);

        controller.stop(id);

        // Stopping the same recording again should throw
        assertThatThrownBy(() -> controller.stop(id))
                .isInstanceOf(DiagnosticException.class);
    }
}
