package org.pangrova.connector.spi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link CacheEvictionListener} SPI contract.
 * F-CHAIN-CACHE-03: cache eviction broadcast chain.
 */
class CacheEvictionListenerTest {

    @Test
    void noopListenerDoesNotThrow() {
        assertThatCode(() ->
                CacheEvictionListener.NOOP.onFilesRemoved(
                        List.of("s3://bucket/data/001.parquet"), "expire_snapshots"))
                .doesNotThrowAnyException();
    }

    @Test
    void noopListenerAcceptsNullCollection() {
        assertThatCode(() ->
                CacheEvictionListener.NOOP.onFilesRemoved(null, "compact"))
                .doesNotThrowAnyException();
    }

    @Test
    void noopListenerAcceptsEmptyCollection() {
        assertThatCode(() ->
                CacheEvictionListener.NOOP.onFilesRemoved(List.of(), "compact"))
                .doesNotThrowAnyException();
    }

    @Test
    void customListenerReceivesEvents() {
        List<String> captured = new ArrayList<>();
        CacheEvictionListener listener = (paths, reason) -> {
            if (paths != null) {
                captured.addAll(paths);
            }
        };

        listener.onFilesRemoved(
                List.of("a.parquet", "b.parquet"), "expire_snapshots");
        assertThat(captured).containsExactly("a.parquet", "b.parquet");

        listener.onFilesRemoved(
                List.of("c.parquet"), "compact");
        assertThat(captured).hasSize(3);
    }
}
