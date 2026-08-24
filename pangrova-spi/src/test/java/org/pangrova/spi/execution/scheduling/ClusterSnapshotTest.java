package org.pangrova.spi.execution.scheduling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClusterSnapshot} record.
 */
class ClusterSnapshotTest {

    private static final SchedulingWorkerSnapshot WORKER_1 = new SchedulingWorkerSnapshot(
            "w1", "host1", 8081, 1024L * 1024 * 1024, 3);

    private static final SchedulingWorkerSnapshot WORKER_2 = new SchedulingWorkerSnapshot(
            "w2", "host2", 8082, 512L * 1024 * 1024, 5);

    @Test
    void constructsWithValidInputs() {
        Instant now = Instant.now();
        ClusterSnapshot snapshot = new ClusterSnapshot(
                List.of(WORKER_1, WORKER_2), 10, 2, now);

        assertThat(snapshot.workers()).containsExactly(WORKER_1, WORKER_2);
        assertThat(snapshot.activeQueries()).isEqualTo(10);
        assertThat(snapshot.queuedQueries()).isEqualTo(2);
        assertThat(snapshot.timestamp()).isEqualTo(now);
    }

    @Test
    void workersListIsDefensivelyCopied() {
        Instant now = Instant.now();
        ClusterSnapshot snapshot = new ClusterSnapshot(
                List.of(WORKER_1), 1, 0, now);

        // The returned list should be immutable
        assertThatThrownBy(() -> snapshot.workers().add(WORKER_2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void emptyWorkersListIsAllowed() {
        ClusterSnapshot snapshot = new ClusterSnapshot(
                List.of(), 0, 0, Instant.now());
        assertThat(snapshot.workers()).isEmpty();
    }

    @Test
    void rejectsNullWorkers() {
        assertThatThrownBy(() -> new ClusterSnapshot(null, 0, 0, Instant.now()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullTimestamp() {
        assertThatThrownBy(() -> new ClusterSnapshot(List.of(), 0, 0, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNegativeActiveQueries() {
        assertThatThrownBy(() -> new ClusterSnapshot(List.of(), -1, 0, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("activeQueries");
    }

    @Test
    void rejectsNegativeQueuedQueries() {
        assertThatThrownBy(() -> new ClusterSnapshot(List.of(), 0, -1, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queuedQueries");
    }
}
