package org.pangrova.worker.memory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link NativeSpillController}.
 * Task 204: validates the arbitrator-to-spill trigger controller.
 */
class NativeSpillControllerTest {

    @Test
    void spillEnabledByDefault() {
        var controller = new NativeSpillController(bytes -> 0);
        assertThat(controller.isSpillEnabled()).isTrue();
    }

    @Test
    void requestSpillRecordsWhenEnabled() {
        // Deterministic reclaim: frees exactly what was requested.
        var controller = new NativeSpillController(bytes -> bytes);
        assertThat(controller.requestSpill(1024)).isTrue();
        assertThat(controller.spillRequestCount()).isEqualTo(1);
        assertThat(controller.spillSuccessCount()).isEqualTo(1);
        assertThat(controller.totalBytesSpilled()).isEqualTo(1024);
    }

    @Test
    void requestSpillReportsFailureWhenReclaimFreesNothing() {
        var controller = new NativeSpillController(bytes -> 0L);
        assertThat(controller.requestSpill(1024)).isFalse();
        assertThat(controller.spillRequestCount()).isEqualTo(1);
        assertThat(controller.spillFailureCount()).isEqualTo(1);
        assertThat(controller.totalBytesSpilled()).isZero();
    }

    @Test
    void unavailableNativeReclaimFailsClosed() {
        var controller = new NativeSpillController(bytes -> {
            throw new UnsatisfiedLinkError("native reclaim unavailable");
        });
        assertThat(controller.requestSpill(1024)).isFalse();
        assertThat(controller.spillFailureCount()).isEqualTo(1);
        assertThat(controller.totalBytesSpilled()).isZero();
    }

    @Test
    void requestSpillDeniedWhenDisabled() {
        var controller = new NativeSpillController(bytes -> bytes);
        controller.setSpillEnabled(false);
        assertThat(controller.requestSpill(1024)).isFalse();
        assertThat(controller.spillRequestCount()).isZero();
    }

    @Test
    void reportSpillSuccess() {
        var controller = new NativeSpillController(bytes -> 0);
        controller.reportSpillResult(4096);
        assertThat(controller.spillSuccessCount()).isEqualTo(1);
        assertThat(controller.totalBytesSpilled()).isEqualTo(4096);
        assertThat(controller.spillFailureCount()).isZero();
    }

    @Test
    void reportSpillFailure() {
        var controller = new NativeSpillController(bytes -> 0);
        controller.reportSpillResult(0);
        assertThat(controller.spillFailureCount()).isEqualTo(1);
        assertThat(controller.spillSuccessCount()).isZero();
    }

    @Test
    void multipleSpillRequestsAndResults() {
        // Synchronous reclaim path (requestSpill) and async native report path
        // (reportSpillResult) each account independently — a real spill flows
        // through exactly one of them.
        var controller = new NativeSpillController(bytes -> bytes);
        controller.requestSpill(1024);
        controller.requestSpill(2048);
        controller.reportSpillResult(1024);
        controller.reportSpillResult(2048);
        controller.reportSpillResult(0);

        assertThat(controller.spillRequestCount()).isEqualTo(2);
        assertThat(controller.spillSuccessCount()).isEqualTo(4);
        assertThat(controller.spillFailureCount()).isEqualTo(1);
        assertThat(controller.totalBytesSpilled()).isEqualTo(6144);
    }

    @Test
    void toggleSpillEnabled() {
        var controller = new NativeSpillController(bytes -> 0);
        controller.setSpillEnabled(false);
        assertThat(controller.isSpillEnabled()).isFalse();
        controller.setSpillEnabled(true);
        assertThat(controller.isSpillEnabled()).isTrue();
    }
}
