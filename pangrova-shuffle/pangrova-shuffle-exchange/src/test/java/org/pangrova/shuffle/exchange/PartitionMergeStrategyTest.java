package org.pangrova.shuffle.exchange;

import org.junit.jupiter.api.Test;

import org.pangrova.shuffle.exchange.enums.PartitionMergeStrategy;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionMergeStrategyTest {

    @Test
    void allStrategiesExist() {
        assertThat(PartitionMergeStrategy.values())
                .containsExactly(
                        PartitionMergeStrategy.NONE,
                        PartitionMergeStrategy.APPEND,
                        PartitionMergeStrategy.SORT_MERGE);
    }
}
