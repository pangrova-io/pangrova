package org.pangrova.shuffle.api.spool;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.shuffle.api.capability.ShuffleDurability;

/**
 * Persistent-plane storage backend for committed shuffle partitions (ADR-002 D6).
 *
 * <p>A {@code SpoolStore} stores the output of a producer (mapper) task keyed by
 * {@code (queryId, stageId, partitionId, mapId, attemptId)} and serves it back to consumer (reduce)
 * tasks by partition over a map-index range. This is the storage primitive behind the in-tree
 * {@code local-spill} ({@link ShuffleDurability#LOCAL_SPILL}) and {@code object-store}
 * ({@link ShuffleDurability#DURABLE}) shuffle providers; it is <em>not</em> the native Flight
 * ephemeral path and <em>not</em> operator spill.
 */
@Experimental(reason = "ADR-002 D6 persistent-plane spool SPI; evolving alongside D4 providers")
public interface SpoolStore extends AutoCloseable {

    /**
     * Opens a writer for one producer attempt's output of a single partition. The written segment is
     * only visible to {@link #read} after {@link SpoolWriter#commit()}.
     *
     * @param queryId    the owning query
     * @param stageId    the producing stage
     * @param partitionId the target reduce partition
     * @param mapId      the producer (mapper) index within the stage
     * @param attemptId  the producer task attempt (distinguishes speculative / re-run attempts)
     */
    SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId);

    /**
     * Opens a reader over the committed segments of one partition across a half-open map-index range
     * {@code [startMapIndex, endMapIndex)}. When multiple attempts of the same {@code mapId} were
     * committed (speculative execution or task re-run), the reader yields the latest committed
     * attempt only — re-executed mappers must never be double-read.
     */
    SpoolReader read(String queryId, int stageId, int partitionId, int startMapIndex, int endMapIndex);

    /** Removes all spooled data for the query (best-effort cleanup on query completion). */
    void delete(String queryId);

    /** The durability level this store guarantees, used by the FTE scheduler's retry decision. */
    ShuffleDurability durability();
}
