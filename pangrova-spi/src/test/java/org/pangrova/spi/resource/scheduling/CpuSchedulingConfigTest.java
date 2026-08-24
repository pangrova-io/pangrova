package org.pangrova.spi.resource.scheduling;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates G-RES-02: CpuSchedulingConfig SPI data model.
 */
class CpuSchedulingConfigTest {

    @Test
    void weightedConfigCreatesNonExclusiveEntry() {
        CpuSchedulingConfig config = CpuSchedulingConfig.weighted("pool-1", 20, 10);
        assertThat(config.groupId()).isEqualTo("pool-1");
        assertThat(config.exclusiveCpuCores()).isEqualTo(0);
        assertThat(config.cpuWeight()).isEqualTo(20);
        assertThat(config.maxConcurrency()).isEqualTo(10);
        assertThat(config.isExclusive()).isFalse();
    }

    @Test
    void exclusiveConfigCreatesDedicatedEntry() {
        CpuSchedulingConfig config = CpuSchedulingConfig.exclusive("pool-2", 4, 8);
        assertThat(config.groupId()).isEqualTo("pool-2");
        assertThat(config.exclusiveCpuCores()).isEqualTo(4);
        assertThat(config.cpuWeight()).isEqualTo(0);
        assertThat(config.maxConcurrency()).isEqualTo(8);
        assertThat(config.isExclusive()).isTrue();
    }

    @Test
    void defaultConfigUsesDefaultWeight() {
        CpuSchedulingConfig config = CpuSchedulingConfig.defaultConfig("default-pool");
        assertThat(config.cpuWeight()).isEqualTo(CpuSchedulingConfig.DEFAULT_CPU_WEIGHT);
        assertThat(config.exclusiveCpuCores()).isEqualTo(0);
        assertThat(config.maxConcurrency()).isEqualTo(0); // unlimited
        assertThat(config.isExclusive()).isFalse();
    }

    @Test
    void rejectsNullGroupId() {
        assertThatThrownBy(() -> CpuSchedulingConfig.defaultConfig(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("groupId");
    }

    @Test
    void rejectsBlankGroupId() {
        assertThatThrownBy(() -> CpuSchedulingConfig.defaultConfig("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("groupId");
    }

    @Test
    void rejectsNegativeExclusiveCores() {
        assertThatThrownBy(() -> new CpuSchedulingConfig("p1", -1, 10, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exclusiveCpuCores");
    }

    @Test
    void rejectsNegativeCpuWeight() {
        assertThatThrownBy(() -> new CpuSchedulingConfig("p1", 0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cpuWeight");
    }
}
