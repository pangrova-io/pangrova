package org.pangrova.worker.memory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link NativeSpillBridge}.
 * Task 204: validates the arbitrator-to-spill trigger bridge.
 */
class NativeSpillBridgeTest {

    @Test
    void spillEnabledByDefault() {
        var bridge = new NativeSpillBridge();
        assertThat(bridge.isSpillEnabled()).isTrue();
    }

    @Test
    void requestSpillRecordsWhenEnabled() {
        // Deterministic reclaim: frees exactly what was requested.
        var bridge = new NativeSpillBridge(bytes -> bytes);
        assertThat(bridge.requestSpill(1024)).isTrue();
        assertThat(bridge.spillRequestCount()).isEqualTo(1);
        assertThat(bridge.spillSuccessCount()).isEqualTo(1);
        assertThat(bridge.totalBytesSpilled()).isEqualTo(1024);
    }

    @Test
    void requestSpillReportsFailureWhenReclaimFreesNothing() {
        var bridge = new NativeSpillBridge(bytes -> 0L);
        assertThat(bridge.requestSpill(1024)).isFalse();
        assertThat(bridge.spillRequestCount()).isEqualTo(1);
        assertThat(bridge.spillFailureCount()).isEqualTo(1);
        assertThat(bridge.totalBytesSpilled()).isZero();
    }

    @Test
    void requestSpillDeniedWhenDisabled() {
        var bridge = new NativeSpillBridge(bytes -> bytes);
        bridge.setSpillEnabled(false);
        assertThat(bridge.requestSpill(1024)).isFalse();
        assertThat(bridge.spillRequestCount()).isZero();
    }

    @Test
    void reportSpillSuccess() {
        var bridge = new NativeSpillBridge();
        bridge.reportSpillResult(4096);
        assertThat(bridge.spillSuccessCount()).isEqualTo(1);
        assertThat(bridge.totalBytesSpilled()).isEqualTo(4096);
        assertThat(bridge.spillFailureCount()).isZero();
    }

    @Test
    void reportSpillFailure() {
        var bridge = new NativeSpillBridge();
        bridge.reportSpillResult(0);
        assertThat(bridge.spillFailureCount()).isEqualTo(1);
        assertThat(bridge.spillSuccessCount()).isZero();
    }

    @Test
    void multipleSpillRequestsAndResults() {
        // Synchronous reclaim path (requestSpill) and async native report path
        // (reportSpillResult) each account independently — a real spill flows
        // through exactly one of them.
        var bridge = new NativeSpillBridge(bytes -> bytes);
        bridge.requestSpill(1024);
        bridge.requestSpill(2048);
        bridge.reportSpillResult(1024);
        bridge.reportSpillResult(2048);
        bridge.reportSpillResult(0);

        assertThat(bridge.spillRequestCount()).isEqualTo(2);
        assertThat(bridge.spillSuccessCount()).isEqualTo(4);
        assertThat(bridge.spillFailureCount()).isEqualTo(1);
        assertThat(bridge.totalBytesSpilled()).isEqualTo(6144);
    }

    @Test
    void toggleSpillEnabled() {
        var bridge = new NativeSpillBridge();
        bridge.setSpillEnabled(false);
        assertThat(bridge.isSpillEnabled()).isFalse();
        bridge.setSpillEnabled(true);
        assertThat(bridge.isSpillEnabled()).isTrue();
    }
}
