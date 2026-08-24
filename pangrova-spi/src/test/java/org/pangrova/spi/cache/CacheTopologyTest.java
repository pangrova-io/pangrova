package org.pangrova.spi.cache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CacheTopology} enum.
 * G-CACHE-08: distributed/detached cache layer SPI.
 */
class CacheTopologyTest {

    @Test
    void allValuesExist() {
        assertThat(CacheTopology.values()).containsExactly(
                CacheTopology.LOCAL_ONLY,
                CacheTopology.SHARED_NOTHING,
                CacheTopology.SHARED_STORE
        );
    }

    @Test
    void valueOfRoundTrip() {
        for (CacheTopology t : CacheTopology.values()) {
            assertThat(CacheTopology.valueOf(t.name())).isEqualTo(t);
        }
    }
}
