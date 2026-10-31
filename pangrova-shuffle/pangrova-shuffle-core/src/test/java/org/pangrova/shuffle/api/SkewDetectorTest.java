package org.pangrova.shuffle.api;

import java.util.Map;

import org.pangrova.shuffle.api.SkewDetector.SkewDetectionConfig;
import org.pangrova.shuffle.api.SkewDetector.SkewDetectionResult;
import org.pangrova.shuffle.api.SkewDetector.SkewedPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link SkewDetector} SPI contracts, configuration validation,
 * and result types.
 */
class SkewDetectorTest {

    @Test
    void defaultConfigHasSaneValues() {
        SkewDetectionConfig config = SkewDetectionConfig.DEFAULT;
        assertThat(config.skewThresholdRatio()).isEqualTo(5.0);
        assertThat(config.minSkewBytes()).isEqualTo(256L * 1024 * 1024);
        assertThat(config.targetPartitionBytes()).isEqualTo(128L * 1024 * 1024);
    }

    @Test
    void configRejectsLowThresholdRatio() {
        assertThatThrownBy(() -> new SkewDetectionConfig(1.0, 0, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("skewThresholdRatio");
    }

    @Test
    void configRejectsNegativeMinSkewBytes() {
        assertThatThrownBy(() -> new SkewDetectionConfig(2.0, -1, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minSkewBytes");
    }

    @Test
    void configRejectsZeroTargetPartitionBytes() {
        assertThatThrownBy(() -> new SkewDetectionConfig(2.0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetPartitionBytes");
    }

    @Test
    void noSkewSentinelHasNoSkew() {
        assertThat(SkewDetectionResult.NO_SKEW.hasSkew()).isFalse();
        assertThat(SkewDetectionResult.NO_SKEW.skewedPartitions()).isEmpty();
    }

    @Test
    void skewedPartitionValidation() {
        var sp = new SkewedPartition(1, 1024, 100, 4);
        assertThat(sp.partitionId()).isEqualTo(1);
        assertThat(sp.dataBytes()).isEqualTo(1024);
        assertThat(sp.rowCount()).isEqualTo(100);
        assertThat(sp.recommendedSplits()).isEqualTo(4);
    }

    @Test
    void skewedPartitionRejectsNegativeDataBytes() {
        assertThatThrownBy(() -> new SkewedPartition(0, -1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dataBytes");
    }

    @Test
    void skewedPartitionRejectsNegativeRowCount() {
        assertThatThrownBy(() -> new SkewedPartition(0, 0, -1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount");
    }

    @Test
    void skewedPartitionRejectsZeroSplits() {
        assertThatThrownBy(() -> new SkewedPartition(0, 100, 10, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recommendedSplits");
    }

    @Test
    void detectionResultWithSkew() {
        var sp = new SkewedPartition(3, 5000, 500, 5);
        var result = new SkewDetectionResult(java.util.List.of(sp), 100, 5000);
        assertThat(result.hasSkew()).isTrue();
        assertThat(result.skewedPartitions()).hasSize(1);
        assertThat(result.medianBytes()).isEqualTo(100);
        assertThat(result.maxBytes()).isEqualTo(5000);
    }

    @Test
    void detectionResultDefensiveCopy() {
        var sp = new SkewedPartition(1, 1000, 100, 2);
        var mutable = new java.util.ArrayList<SkewedPartition>();
        mutable.add(sp);
        var result = new SkewDetectionResult(mutable, 50, 1000);
        mutable.clear(); // modify original list
        assertThat(result.skewedPartitions()).hasSize(1); // defensive copy
    }
}
