package org.pangrova.shuffle.core;

import org.junit.jupiter.api.Test;

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
