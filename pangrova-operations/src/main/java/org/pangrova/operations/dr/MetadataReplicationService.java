package org.pangrova.operations.dr;

import org.pangrova.common.annotation.ExperimentalImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Replicates metadata to a standby target with async retry on failure.
 * Uses a soft-bounded pending queue (ConcurrentLinkedQueue with AtomicInteger size check)
 * to limit memory growth under sustained replication failure. The bound is approximate
 * under high concurrency due to the non-atomic check-then-enqueue sequence.
 *
 * <p>This service has real replication logic (retry queue, bounded backpressure,
 * defensive copy) but no production {@link ReplicationTarget} implementation exists.
 * Not wired into any coordinator lifecycle.</p>
 */
@ExperimentalImplementation(trackingIssue = "https://github.com/pangrova/pangrova/issues/F-OPS-002-metadata-replication")
public final class MetadataReplicationService {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataReplicationService.class);
    private static final int MAX_PENDING = 10_000;

    private final ReplicationTarget target;
    private final Queue<byte[]> pendingQueue = new ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicInteger pendingSize =
            new java.util.concurrent.atomic.AtomicInteger(0);

    public MetadataReplicationService(ReplicationTarget target) {
        this.target = Objects.requireNonNull(target, "target");
    }

    /**
     * Replicates the given data to the standby target. On failure, queues for retry.
     * Makes a defensive copy of the input array to prevent external mutation.
     * Drops the event if the pending queue exceeds {@value MAX_PENDING} entries.
     *
     * @param data the metadata bytes to replicate (defensively copied)
     */
    public void replicate(byte[] data) {
        Objects.requireNonNull(data, "data");
        byte[] copy = java.util.Arrays.copyOf(data, data.length);
        try {
            target.replicate(copy);
        } catch (Exception e) {
            if (pendingSize.get() >= MAX_PENDING) {
                LOG.warn("event=replication_queue_full droppedBytes={} queueSize={}",
                        copy.length, pendingSize.get());
                return;
            }
            LOG.warn("event=replication_failed_queued error={}", e.getMessage());
            pendingQueue.add(copy);
            pendingSize.incrementAndGet();
        }
    }

    /** Retries replication of queued pending data, stopping at the first failure. */
    public void retryPending() {
        int retried = 0;
        byte[] data;
        while ((data = pendingQueue.peek()) != null) {
            try {
                target.replicate(data);
                pendingQueue.poll();
                pendingSize.decrementAndGet();
                retried++;
            } catch (Exception e) {
                LOG.warn("event=replication_retry_failed remaining={}", pendingSize.get());
                break;
            }
        }
        if (retried > 0) {
            LOG.info("event=replication_retry_completed retried={}", retried);
        }
    }

    public int pendingCount() {
        return pendingSize.get();
    }

    /**
     * Target for metadata replication (e.g., standby cluster).
     */
    public interface ReplicationTarget {

        void replicate(byte[] data);

        boolean isAvailable();
    }
}
