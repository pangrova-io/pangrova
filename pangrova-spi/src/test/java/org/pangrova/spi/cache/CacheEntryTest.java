package org.pangrova.spi.cache;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CacheEntry} SPI contract.
 * G-CACHE-08: distributed/detached cache layer SPI.
 */
class CacheEntryTest {

    @Test
    void constructorStoresFields() {
        byte[] data = {1, 2, 3, 4};
        Instant now = Instant.now();
        CacheEntry entry = new CacheEntry(data, now, "catalog.db.table1");

        assertThat(entry.data()).isEqualTo(new byte[]{1, 2, 3, 4});
        assertThat(entry.createdAt()).isEqualTo(now);
        assertThat(entry.tableId()).isEqualTo("catalog.db.table1");
        assertThat(entry.dataLength()).isEqualTo(4);
    }

    @Test
    void dataIsDefensivelyCopied() {
        byte[] original = {1, 2, 3};
        CacheEntry entry = new CacheEntry(original, Instant.now(), "t");

        // Mutate the original -- entry should not be affected
        original[0] = 99;
        assertThat(entry.data()[0]).isEqualTo((byte) 1);

        // Mutate the returned copy -- entry should not be affected
        byte[] returned = entry.data();
        returned[0] = 99;
        assertThat(entry.data()[0]).isEqualTo((byte) 1);
    }

    @Test
    void constructorRejectsNullData() {
        assertThatThrownBy(() -> new CacheEntry(null, Instant.now(), "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data");
    }

    @Test
    void constructorRejectsEmptyData() {
        assertThatThrownBy(() -> new CacheEntry(new byte[0], Instant.now(), "t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("data");
    }

    @Test
    void constructorRejectsNullCreatedAt() {
        assertThatThrownBy(() -> new CacheEntry(new byte[]{1}, null, "t"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("createdAt");
    }

    @Test
    void constructorRejectsNullTableId() {
        assertThatThrownBy(() -> new CacheEntry(new byte[]{1}, Instant.now(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tableId");
    }

    @Test
    void ofFactoryUsesCurrentTime() {
        Instant before = Instant.now();
        CacheEntry entry = CacheEntry.of(new byte[]{1, 2}, "catalog.db.t");
        Instant after = Instant.now();

        assertThat(entry.createdAt()).isBetween(before, after);
        assertThat(entry.tableId()).isEqualTo("catalog.db.t");
        assertThat(entry.dataLength()).isEqualTo(2);
    }

    @Test
    void toStringIncludesTableIdAndSize() {
        CacheEntry entry = CacheEntry.of(new byte[]{1, 2, 3}, "catalog.db.t");
        String s = entry.toString();
        assertThat(s).contains("catalog.db.t");
        assertThat(s).contains("3");
    }
}
