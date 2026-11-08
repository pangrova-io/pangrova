package org.pangrova.governance.lineage;

import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.governance.lineage.log.LogLineageBackend;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** Constructor validation precedes allocation of the owned consumer executor. */
class LineageEventEmitterConstructionTest {
    @Test
    void defaultConstructorRejectsMissingBackend() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineageEventEmitter(null))
                .withMessage("backend must not be null");
    }

    @Test
    void configuredConstructorRejectsMissingBackend() {
        assertThatIllegalArgumentException().isThrownBy(() -> new LineageEventEmitter(null, 1, 1))
                .withMessage("backend must not be null");
    }

    @Test
    void rejectsNonpositiveShutdownTimeout() {
        var backend = new LogLineageBackend();
        for (long timeout : new long[] {0, -1}) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new LineageEventEmitter(backend, timeout, 1))
                    .withMessage("shutdownTimeoutMs must be positive");
        }
    }

    @Test
    void rejectsNonpositiveAlertThreshold() {
        var backend = new LogLineageBackend();
        for (long threshold : new long[] {0, -1}) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new LineageEventEmitter(backend, 1, threshold))
                    .withMessage("droppedEventsAlertThreshold must be positive");
        }
    }
}
