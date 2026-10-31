package org.pangrova.worker.shuffle;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pangrova.shuffle.api.ShuffleByteEgress;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ShuffleByteEgress} (ADR-002 D2-egress).
 *
 * <p>Receives committed partition bytes from the native layer via JNI callback
 * and routes them to a {@link SpoolStore} for JVM-resident shuffle providers
 * (local-spill, object-store, celeborn).
 *
 * <p>Thread model: the native drain thread calls {@link #onPartitionCommitted}
 * synchronously (pull mode). This method opens a {@link SpoolWriter} for the
 * partition segment, writes the columnar IPC bytes, and commits atomically. Each
 * call is one complete segment (one batch from PartitionDemuxer).
 *
 * <p>Writer caching: for high-throughput scenarios where a single partition
 * receives many small batches, we cache open writers keyed by the full
 * 5-tuple. Writers are lazily opened and committed/closed when explicitly
 * flushed or when the query is finalized.
 */
public final class DefaultShuffleByteEgress implements ShuffleByteEgress {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultShuffleByteEgress.class);

    private final SpoolStore spoolStore;

    // Cache of open writers keyed by (queryId, stageId, partitionId, mapId, attemptId).
    // Each callback delivers one complete segment, so we open-write-commit in one shot.
    // If streaming multiple batches per segment is needed in the future, this cache
    // allows batching before commit.
    private final ConcurrentMap<SegmentKey, SpoolWriter> openWriters = new ConcurrentHashMap<>();

    public DefaultShuffleByteEgress(SpoolStore spoolStore) {
        this.spoolStore = Objects.requireNonNull(spoolStore, "spoolStore");
    }

    @Override
    public void onPartitionCommitted(String queryId, int stageId, int partitionId,
                                     int mapId, long attemptId, ByteBuffer arrowIpc) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("egress: queryId={} stage={} partition={} map={} attempt={} bytes={}",
                    queryId, stageId, partitionId, mapId, attemptId, arrowIpc.remaining());
        }

        // Open a writer, write the segment, and commit atomically.
        // Each JNI callback delivers one complete committed segment from the
        // native PartitionDemuxer — no multi-batch buffering needed at this layer.
        SpoolWriter writer = spoolStore.open(queryId, stageId, partitionId, mapId, attemptId);
        try {
            writer.write(arrowIpc);
            writer.commit();
        } catch (Exception e) {
            // Close without commit → abort semantics
            try {
                writer.close();
            } catch (Exception closeEx) {
                e.addSuppressed(closeEx);
            }
            throw new ShuffleDrainException(
                    "Egress write failed: queryId=" + queryId
                            + " stage=" + stageId + " partition=" + partitionId
                            + " map=" + mapId + " attempt=" + attemptId, e);
        }
    }

    /**
     * Returns the underlying SpoolStore (for diagnostics and lifecycle management).
     */
    public SpoolStore spoolStore() {
        return spoolStore;
    }

    // Key for open writer cache (for future batching support).
    private record SegmentKey(String queryId, int stageId, int partitionId, int mapId, long attemptId) {}
}
