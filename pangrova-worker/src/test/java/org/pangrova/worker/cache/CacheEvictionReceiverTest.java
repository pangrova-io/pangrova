package org.pangrova.worker.cache;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CacheEvictionReceiver}.
 * F-CHAIN-CACHE-03: worker-side eviction hint processing.
 */
class CacheEvictionReceiverTest {

    @Test
    void processEvictionHintsRemovesFromTracker() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("s3://bucket/data/001.parquet");
        tracker.recordAccess("s3://bucket/data/002.parquet");
        tracker.recordAccess("s3://bucket/data/003.parquet");
        assertThat(tracker.size()).isEqualTo(3);

        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker);
        receiver.processEvictionHints(List.of(
                "s3://bucket/data/001.parquet",
                "s3://bucket/data/002.parquet"));

        assertThat(tracker.size()).isEqualTo(1);
        assertThat(tracker.contains("s3://bucket/data/001.parquet")).isFalse();
        assertThat(tracker.contains("s3://bucket/data/002.parquet")).isFalse();
        assertThat(tracker.contains("s3://bucket/data/003.parquet")).isTrue();
    }

    @Test
    void processEvictionHintsSendsNativeCommands() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("file1.parquet");
        tracker.recordAccess("file2.parquet");

        List<byte[]> nativeCommands = new ArrayList<>();
        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker, nativeCommands::add);
        receiver.processEvictionHints(List.of("file1.parquet", "file2.parquet"));

        assertThat(nativeCommands).hasSize(2);
        // Each command should be a valid protobuf with EVICT_CACHE type
        for (byte[] cmd : nativeCommands) {
            assertThat(cmd).isNotNull();
            assertThat(cmd.length).isGreaterThan(0);
        }
        assertThat(tracker.size()).isZero();
    }

    @Test
    void nullOrEmptyHintsAreNoop() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("keep.parquet");

        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker);
        receiver.processEvictionHints(null);
        receiver.processEvictionHints(List.of());

        assertThat(tracker.size()).isEqualTo(1);
    }

    @Test
    void blankPathsSkipped() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("real.parquet");

        List<String> paths = new ArrayList<>();
        paths.add(null);
        paths.add("");
        paths.add("  ");
        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker);
        receiver.processEvictionHints(paths);

        assertThat(tracker.size()).isEqualTo(1);
    }

    @Test
    void nativeCommandFailureDoesNotStopProcessing() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("file1.parquet");
        tracker.recordAccess("file2.parquet");
        tracker.recordAccess("file3.parquet");

        // First call throws, second and third succeed
        List<String> sentPaths = new ArrayList<>();
        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker, cmd -> {
            sentPaths.add("sent");
            if (sentPaths.size() == 1) {
                throw new RuntimeException("simulated native failure");
            }
        });

        receiver.processEvictionHints(List.of(
                "file1.parquet", "file2.parquet", "file3.parquet"));

        // file1.parquet native call failed, so it stays in tracker
        // file2 and file3 succeed
        assertThat(tracker.contains("file1.parquet")).isTrue();
        assertThat(tracker.contains("file2.parquet")).isFalse();
        assertThat(tracker.contains("file3.parquet")).isFalse();
    }

    @Test
    void evictingNonexistentPathIsHarmless() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker);
        // No exception — evicting a path that was never tracked is a no-op
        receiver.processEvictionHints(List.of("nonexistent.parquet"));
        assertThat(tracker.size()).isZero();
    }

    @Test
    void worksWithoutNativeSender() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("file.parquet");

        CacheEvictionReceiver receiver = new CacheEvictionReceiver(tracker);
        receiver.processEvictionHints(List.of("file.parquet"));

        assertThat(tracker.contains("file.parquet")).isFalse();
    }
}
