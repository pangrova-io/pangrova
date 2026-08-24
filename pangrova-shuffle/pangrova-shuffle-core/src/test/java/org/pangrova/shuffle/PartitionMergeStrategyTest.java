package org.pangrova.shuffle;

import org.pangrova.shuffle.PartitionMergeStrategy;
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
