package org.pangrova.shuffle.api;

import org.pangrova.shuffle.api.ShuffleDrainHandler.DrainResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ShuffleDrainHandler} and its {@link DrainResult} (G-RES-08).
 */
class ShuffleDrainHandlerTest {

    // ── DrainResult ──

    @Test
    void allDrainedSentinelIsFullyDrained() {
        assertThat(DrainResult.ALL_DRAINED.isFullyDrained()).isTrue();
        assertThat(DrainResult.ALL_DRAINED.drained()).isZero();
        assertThat(DrainResult.ALL_DRAINED.atRisk()).isZero();
        assertThat(DrainResult.ALL_DRAINED.bytesAtRisk()).isZero();
    }

    @Test
    void partialDrainResultIsNotFullyDrained() {
        var result = new DrainResult(5, 2, 1024);
        assertThat(result.isFullyDrained()).isFalse();
        assertThat(result.drained()).isEqualTo(5);
        assertThat(result.atRisk()).isEqualTo(2);
        assertThat(result.bytesAtRisk()).isEqualTo(1024);
    }

    @Test
    void drainResultNegativeDrainedThrows() {
        assertThatThrownBy(() -> new DrainResult(-1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void drainResultNegativeAtRiskThrows() {
        assertThatThrownBy(() -> new DrainResult(0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void drainResultNegativeBytesAtRiskThrows() {
        assertThatThrownBy(() -> new DrainResult(0, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── ShuffleDrainHandler.persistent() factory ──

    @Test
    void persistentHandlerDrainsImmediately() {
        ShuffleDrainHandler handler = ShuffleDrainHandler.persistent();
        DrainResult result = handler.drain(Duration.ofSeconds(1));
        assertThat(result).isEqualTo(DrainResult.ALL_DRAINED);
        assertThat(result.isFullyDrained()).isTrue();
    }

    @Test
    void persistentHandlerReturnsPersistentPolicy() {
        ShuffleDrainHandler handler = ShuffleDrainHandler.persistent();
        assertThat(handler.retentionPolicy()).isEqualTo(ShuffleRetentionPolicy.PERSISTENT);
    }

    // ── Custom implementation ──

    @Test
    void ephemeralHandlerReportsAllDataAtRisk() {
        ShuffleDrainHandler handler = new ShuffleDrainHandler() {
            @Override
            public DrainResult drain(Duration timeout) {
                return new DrainResult(0, 10, 50_000);
            }

            @Override
            public ShuffleRetentionPolicy retentionPolicy() {
                return ShuffleRetentionPolicy.EPHEMERAL;
            }
        };

        DrainResult result = handler.drain(Duration.ofSeconds(5));
        assertThat(result.isFullyDrained()).isFalse();
        assertThat(result.atRisk()).isEqualTo(10);
        assertThat(result.bytesAtRisk()).isEqualTo(50_000);
        assertThat(handler.retentionPolicy()).isEqualTo(ShuffleRetentionPolicy.EPHEMERAL);
    }

    @Test
    void detachedHandlerDrainsSuccessfully() {
        ShuffleDrainHandler handler = new ShuffleDrainHandler() {
            @Override
            public DrainResult drain(Duration timeout) {
                return new DrainResult(8, 0, 0);
            }

            @Override
            public ShuffleRetentionPolicy retentionPolicy() {
                return ShuffleRetentionPolicy.DETACHED;
            }
        };

        DrainResult result = handler.drain(Duration.ofSeconds(30));
        assertThat(result.isFullyDrained()).isTrue();
        assertThat(result.drained()).isEqualTo(8);
        assertThat(handler.retentionPolicy()).isEqualTo(ShuffleRetentionPolicy.DETACHED);
    }
}
