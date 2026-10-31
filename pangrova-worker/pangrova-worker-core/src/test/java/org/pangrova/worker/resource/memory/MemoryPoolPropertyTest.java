package org.pangrova.worker.resource.memory;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for {@link org.pangrova.worker.resource.memory.MemoryPool} CAS invariants
 * using {@link RootMemoryPool} as the concrete implementation.
 */
class MemoryPoolPropertyTest {

    @Property(tries = 10)
    void reserveAndFree_neverExceedsBudget(
            @ForAll @LongRange(min = 1, max = 1000) long maxBytes,
            @ForAll @LongRange(min = 1, max = 2000) long requestBytes) {

        RootMemoryPool pool = new RootMemoryPool("test", maxBytes);
        long reserved = pool.tryReserve(requestBytes);

        assertThat(reserved).isGreaterThanOrEqualTo(0);
        assertThat(reserved).isLessThanOrEqualTo(maxBytes);
        assertThat(pool.getUsedBytes()).isEqualTo(reserved);

        pool.free(reserved);
        assertThat(pool.getUsedBytes()).isEqualTo(0);
    }

    @Property(tries = 10)
    void freeMoreThanReserved_throwsDoubleReleaseException(
            @ForAll @LongRange(min = 1, max = 500) long maxBytes,
            @ForAll @LongRange(min = 1, max = 500) long reserveBytes,
            @ForAll @LongRange(min = 1, max = 1000) long freeBytes) {

        RootMemoryPool pool = new RootMemoryPool("test", maxBytes);
        long reserved = pool.tryReserve(reserveBytes);

        if (freeBytes > reserved) {
            assertThatThrownBy(() -> pool.free(freeBytes))
                    .isInstanceOf(DoubleReleaseException.class)
                    .hasMessageContaining("test")
                    .hasMessageContaining(String.valueOf(freeBytes));
            assertThat(pool.getDoubleReleaseCount()).isEqualTo(1);
        } else {
            pool.free(freeBytes);
            assertThat(pool.getUsedBytes()).isEqualTo(reserved - freeBytes);
        }
    }

    @Property(tries = 10)
    void utilization_alwaysBetweenZeroAndOne(
            @ForAll @LongRange(min = 1, max = 10000) long maxBytes,
            @ForAll @LongRange(min = 0, max = 20000) long requestBytes) {

        RootMemoryPool pool = new RootMemoryPool("test", maxBytes);
        pool.tryReserve(requestBytes);

        double util = pool.getUtilization();
        assertThat(util).isBetween(0.0, 1.0);
    }
}
