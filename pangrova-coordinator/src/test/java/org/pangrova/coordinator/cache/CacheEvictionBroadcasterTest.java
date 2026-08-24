package org.pangrova.coordinator.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CacheEvictionBroadcaster}.
 * F-CHAIN-CACHE-03: cache eviction broadcast chain.
 */
class CacheEvictionBroadcasterTest {

    @Test
    void emptyBroadcasterHasNoPending() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        assertThat(broadcaster.pendingSize()).isZero();
        assertThat(broadcaster.drainBatch(100)).isEmpty();
    }

    @Test
    void filesRecordedAndDrained() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(
                List.of("s3://bucket/t/data/001.parquet", "s3://bucket/t/data/002.parquet"),
                "expire_snapshots");
        assertThat(broadcaster.pendingSize()).isEqualTo(2);

        List<String> batch = broadcaster.drainBatch(10);
        assertThat(batch).containsExactly(
                "s3://bucket/t/data/001.parquet",
                "s3://bucket/t/data/002.parquet");
        assertThat(broadcaster.pendingSize()).isZero();
    }

    @Test
    void drainBatchRespectsMaxBatch() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(
                List.of("a.parquet", "b.parquet", "c.parquet"), "compact");
        List<String> batch = broadcaster.drainBatch(2);
        assertThat(batch).hasSize(2);
        assertThat(broadcaster.pendingSize()).isEqualTo(1);
    }

    @Test
    void duplicatePathsDeduplicated() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(List.of("same.parquet", "same.parquet"), "compact");
        assertThat(broadcaster.pendingSize()).isEqualTo(1);
    }

    @Test
    void nullAndBlankPathsIgnored() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        List<String> paths = new ArrayList<>();
        paths.add(null);
        paths.add("");
        paths.add("  ");
        paths.add("real.parquet");
        broadcaster.onFilesRemoved(paths, "expire_snapshots");
        assertThat(broadcaster.pendingSize()).isEqualTo(1);
        assertThat(broadcaster.drainBatch(10)).containsExactly("real.parquet");
    }

    @Test
    void nullOrEmptyCollectionIsNoop() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(null, "compact");
        broadcaster.onFilesRemoved(List.of(), "compact");
        assertThat(broadcaster.pendingSize()).isZero();
    }

    @Test
    void boundedBufferDropsOldestWhenFull() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster(3);
        broadcaster.onFilesRemoved(
                List.of("first.parquet", "second.parquet", "third.parquet"), "compact");
        assertThat(broadcaster.pendingSize()).isEqualTo(3);

        // Adding a 4th should drop "first.parquet"
        broadcaster.onFilesRemoved(List.of("fourth.parquet"), "compact");
        assertThat(broadcaster.pendingSize()).isEqualTo(3);
        assertThat(broadcaster.peekAll()).containsExactly(
                "second.parquet", "third.parquet", "fourth.parquet");
    }

    @Test
    void peekDoesNotDrain() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(List.of("a.parquet"), "compact");
        assertThat(broadcaster.peekAll()).hasSize(1);
        assertThat(broadcaster.pendingSize()).isEqualTo(1);
    }

    @Test
    void clearRemovesAll() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(List.of("a.parquet", "b.parquet"), "compact");
        broadcaster.clear();
        assertThat(broadcaster.pendingSize()).isZero();
    }

    @Test
    void drainBatchWithZeroOrNegativeReturnsEmpty() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(List.of("a.parquet"), "compact");
        assertThat(broadcaster.drainBatch(0)).isEmpty();
        assertThat(broadcaster.drainBatch(-1)).isEmpty();
        assertThat(broadcaster.pendingSize()).isEqualTo(1);
    }

    @Test
    void invalidMaxPendingThrows() {
        assertThatThrownBy(() -> new CacheEvictionBroadcaster(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CacheEvictionBroadcaster(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multipleBatchesFromMultipleReasons() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(List.of("expire1.parquet"), "expire_snapshots");
        broadcaster.onFilesRemoved(List.of("compact1.parquet"), "compact");
        broadcaster.onFilesRemoved(List.of("orphan1.parquet"), "remove_orphan_files");
        assertThat(broadcaster.pendingSize()).isEqualTo(3);

        List<String> all = broadcaster.drainBatch(100);
        assertThat(all).containsExactly(
                "expire1.parquet", "compact1.parquet", "orphan1.parquet");
    }

    @Test
    void drainedBatchIsImmutable() {
        CacheEvictionBroadcaster broadcaster = new CacheEvictionBroadcaster();
        broadcaster.onFilesRemoved(List.of("a.parquet"), "compact");
        List<String> batch = broadcaster.drainBatch(10);
        assertThatThrownBy(() -> batch.add("extra"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
