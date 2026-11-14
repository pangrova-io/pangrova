package org.pangrova.worker.buffer;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.worker.buffer.BackpressureController.Action;
import org.pangrova.worker.buffer.BackpressureController.BackpressureResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for {@link BackpressureController} watermark semantics.
 */
class BackpressureControllerPropertyTest {

    private static final int LOW = 500;
    private static final int HIGH = 800;
    private static final int MAX = 1000;

    private final BackpressureController controller = new BackpressureController(MAX, HIGH, LOW);

    @Property(tries = 10)
    void belowLowWaterMark_alwaysProceeds(@ForAll @IntRange(min = 0, max = 499) int pending) {
        BackpressureResult result = controller.checkBackpressure(pending);
        assertThat(result.action()).isEqualTo(Action.PROCEED);
    }

    @Property(tries = 10)
    void atOrAboveMax_alwaysRejects(@ForAll @IntRange(min = 1000, max = 2000) int pending) {
        BackpressureResult result = controller.checkBackpressure(pending);
        assertThat(result.action()).isEqualTo(Action.REJECT);
    }

    @Property(tries = 10)
    void betweenHighAndMax_slowsDown(@ForAll @IntRange(min = 800, max = 999) int pending) {
        BackpressureResult result = controller.checkBackpressure(pending);
        assertThat(result.action()).isEqualTo(Action.SLOW_DOWN);
        assertThat(result.delay()).isNotNull();
        assertThat(result.delay().toMillis()).isGreaterThanOrEqualTo(0);
    }

    @Property(tries = 10)
    void invalidWatermarks_throwsIllegalArgument(
            @ForAll @IntRange(min = 1, max = 100) int same) {
        // highWaterMark == lowWaterMark → division by zero guard
        assertThatThrownBy(() -> new BackpressureController(same + 1, same, same))
                .isInstanceOf(IllegalArgumentException.class);
        // maxPending <= highWaterMark
        assertThatThrownBy(() -> new BackpressureController(same, same, same - 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
