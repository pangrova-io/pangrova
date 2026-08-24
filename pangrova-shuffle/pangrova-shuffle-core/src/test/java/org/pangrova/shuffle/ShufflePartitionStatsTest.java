package org.pangrova.shuffle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.shuffle.ShufflePartitionStats;

/**
 * Unit tests for {@link ShufflePartitionStats} — validation and immutability.
 */
class ShufflePartitionStatsTest {

    @Test
    void validConstructionPreservesFields() {
        var stats = new ShufflePartitionStats(3, 1024L, 100L);

        assertThat(stats.partitionId()).isEqualTo(3);
        assertThat(stats.dataBytes()).isEqualTo(1024L);
        assertThat(stats.rowCount()).isEqualTo(100L);
    }

    @Test
    void zeroValuesAreAccepted() {
        var stats = new ShufflePartitionStats(0, 0L, 0L);

        assertThat(stats.partitionId()).isZero();
        assertThat(stats.dataBytes()).isZero();
        assertThat(stats.rowCount()).isZero();
    }

    @Test
    void negativeDataBytesThrows() {
        assertThatThrownBy(() -> new ShufflePartitionStats(0, -1L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dataBytes must be non-negative");
    }

    @Test
    void negativeRowCountThrows() {
        assertThatThrownBy(() -> new ShufflePartitionStats(0, 0L, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("rowCount must be non-negative");
    }

    @Test
    void equalityAndHashCodeFollowRecordSemantics() {
        var a = new ShufflePartitionStats(1, 500L, 50L);
        var b = new ShufflePartitionStats(1, 500L, 50L);

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void differentValuesAreNotEqual() {
        var a = new ShufflePartitionStats(1, 500L, 50L);
        var b = new ShufflePartitionStats(2, 500L, 50L);

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void toStringContainsAllFields() {
        var stats = new ShufflePartitionStats(7, 2048L, 200L);

        String str = stats.toString();
        assertThat(str).contains("7").contains("2048").contains("200");
    }
}
