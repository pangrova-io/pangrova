package org.pangrova.operations.dr;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MetadataReplicationServiceTest {

    @Test
    void replicate_success_callsTarget() {
        var target = new RecordingTarget(false);
        var service = new MetadataReplicationService(target);

        service.replicate(new byte[]{1, 2, 3});

        assertThat(target.replicateCount.get()).isEqualTo(1);
        assertThat(service.pendingCount()).isEqualTo(0);
    }

    @Test
    void replicate_failure_queuesForRetry() {
        var target = new RecordingTarget(true);
        var service = new MetadataReplicationService(target);

        service.replicate(new byte[]{1, 2, 3});

        assertThat(service.pendingCount()).isEqualTo(1);
    }

    @Test
    void replicate_overflow_silentlyDrops() {
        var target = new RecordingTarget(true);
        var service = new MetadataReplicationService(target);

        // Fill queue up to max
        for (int i = 0; i < 10_000; i++) {
            service.replicate(new byte[]{(byte) i});
        }
        assertThat(service.pendingCount()).isEqualTo(10_000);

        // Next replication attempt should be silently dropped
        service.replicate(new byte[]{99});
        assertThat(service.pendingCount()).isEqualTo(10_000);
    }

    @Test
    void retryPending_executesAfterFailure() {
        var target = new RecordingTarget(true);
        var service = new MetadataReplicationService(target);

        service.replicate(new byte[]{1});
        service.replicate(new byte[]{2});
        assertThat(service.pendingCount()).isEqualTo(2);

        // Now make target succeed and retry
        target.shouldFail.set(false);
        service.retryPending();

        assertThat(service.pendingCount()).isEqualTo(0);
        // 2 failed initial attempts + 2 successful retries = 4 total calls
        assertThat(target.replicateCount.get()).isEqualTo(4);
    }

    // --- Helper ---

    private static class RecordingTarget implements MetadataReplicationService.ReplicationTarget {
        final AtomicBoolean shouldFail;
        final AtomicInteger replicateCount = new AtomicInteger(0);

        RecordingTarget(boolean shouldFail) {
            this.shouldFail = new AtomicBoolean(shouldFail);
        }

        @Override
        public void replicate(byte[] data) {
            replicateCount.incrementAndGet();
            if (shouldFail.get()) {
                throw new RuntimeException("simulated replication failure");
            }
        }

        @Override
        public boolean isAvailable() {
            return !shouldFail.get();
        }
    }
}
