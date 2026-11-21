package org.pangrova.worker.buffer;

import org.pangrova.worker.enums.BackpressureAction;
import org.pangrova.worker.buffer.BackpressureController.BackpressureResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests BackpressureController watermark-based decision logic.
 */
class BackpressureControllerTest {

    private final BackpressureController controller = new BackpressureController(1000, 800, 500);

    @Test
    void belowLowWaterMark_proceeds() {
        BackpressureResult result = controller.checkBackpressure(100);
        assertThat(result.action()).isEqualTo(BackpressureAction.PROCEED);
        assertThat(result.delay()).isEqualTo(Duration.ZERO);
    }

    @Test
    void atHighWaterMark_slowsDown() {
        BackpressureResult result = controller.checkBackpressure(800);
        assertThat(result.action()).isEqualTo(BackpressureAction.SLOW_DOWN);
        assertThat(result.delay().toMillis()).isGreaterThan(0);
    }

    @Test
    void atMaxPending_rejects() {
        BackpressureResult result = controller.checkBackpressure(1000);
        assertThat(result.action()).isEqualTo(BackpressureAction.REJECT);
        assertThat(result.delay()).isEqualTo(Duration.ZERO);
    }

    @Test
    void aboveMaxPending_rejects() {
        BackpressureResult result = controller.checkBackpressure(1500);
        assertThat(result.action()).isEqualTo(BackpressureAction.REJECT);
    }

    @Test
    void betweenLowAndHighWaterMark_proceeds() {
        BackpressureResult result = controller.checkBackpressure(600);
        assertThat(result.action()).isEqualTo(BackpressureAction.PROCEED);
    }

    @Test
    void zeroPending_proceeds() {
        BackpressureResult result = controller.checkBackpressure(0);
        assertThat(result.action()).isEqualTo(BackpressureAction.PROCEED);
    }

    @Test
    void defaultConstructor_usesDefaultThresholds() {
        BackpressureController defaultController = new BackpressureController();
        assertThat(defaultController.checkBackpressure(0).action()).isEqualTo(BackpressureAction.PROCEED);
        assertThat(defaultController.checkBackpressure(1000).action()).isEqualTo(BackpressureAction.REJECT);
    }
}
