package org.pangrova.worker.buffer;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.buffer.BackpressureController.Action;

import static org.assertj.core.api.Assertions.assertThat;

/** The former Worker watermark oracles belong to this actual policy, not a Native producer. */
class BackpressureWatermarkBoundaryTest {
    @Test
    void emptyBufferProceedsAtBothFormerWorkerThresholds() {
        assertThat(new BackpressureController(10, 8, 5).checkBackpressure(0).action())
                .isEqualTo(Action.PROCEED);
        assertThat(new BackpressureController(10, 8, 5).checkBackpressure(0).delay())
                .isEqualTo(java.time.Duration.ZERO);
    }

    @Test
    void fullBufferRejectsAtTheFormerThreeBatchBootstrapBoundary() {
        assertThat(new BackpressureController(3, 2, 1).checkBackpressure(3).action())
                .isEqualTo(Action.REJECT);
        assertThat(new BackpressureController(3, 2, 1).checkBackpressure(3).delay())
                .isEqualTo(java.time.Duration.ZERO);
    }

    @Test
    void formerWorkerHighWatermarkStillRequiresAPositiveSlowdownDelay() {
        var result = new BackpressureController(10, 5, 3).checkBackpressure(6);
        assertThat(result.action()).isEqualTo(Action.SLOW_DOWN);
        assertThat(result.delay().toMillis()).isGreaterThan(0);
    }
}
