package org.pangrova.worker.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

class RecentFileAccessTrackerTest {

    @Test
    void recordAndContains() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker(8);
        tracker.recordAccess("s3://bucket/a.parquet");
        tracker.recordAccess("s3://bucket/b.parquet");
        assertThat(tracker.contains("s3://bucket/a.parquet")).isTrue();
        assertThat(tracker.contains("s3://bucket/c.parquet")).isFalse();
        assertThat(tracker.size()).isEqualTo(2);
    }

    @Test
    void boundedByCapacity() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker(3);
        tracker.recordAccess("a");
        tracker.recordAccess("b");
        tracker.recordAccess("c");
        tracker.recordAccess("d");
        assertThat(tracker.size()).isEqualTo(3);
        assertThat(tracker.contains("a")).isFalse();
        assertThat(tracker.contains("d")).isTrue();
    }

    @Test
    void accessPromotesToMru() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker(3);
        tracker.recordAccess("a");
        tracker.recordAccess("b");
        tracker.recordAccess("c");
        tracker.recordAccess("a");
        tracker.recordAccess("d");
        assertThat(tracker.contains("a")).isTrue();
        assertThat(tracker.contains("b")).isFalse();
        assertThat(tracker.contains("d")).isTrue();
    }

    @Test
    void heartbeatSampleBoundedByDefault() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker(500);
        for (int i = 0; i < 500; i++) {
            tracker.recordAccess("file-" + i);
        }
        List<String> sample = tracker.heartbeatSample();
        assertThat(sample).hasSize(RecentFileAccessTracker.DEFAULT_HEARTBEAT_SAMPLE_SIZE);
        assertThat(sample).contains("file-499");
        assertThat(sample).doesNotContain("file-0");
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatThrownBy(() -> new RecentFileAccessTracker(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void recordIgnoresNullAndBlank() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker(4);
        tracker.recordAccess((String) null);
        tracker.recordAccess("");
        tracker.recordAccess("   ");
        assertThat(tracker.size()).isZero();
    }

    @Test
    void heartbeatSampleSnapshotIsImmutable() {
        RecentFileAccessTracker tracker = new RecentFileAccessTracker();
        tracker.recordAccess("a");
        List<String> sample = tracker.heartbeatSample();
        assertThatThrownBy(() -> sample.add("b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
