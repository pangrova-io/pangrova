package org.pangrova.shuffle.spool;

import java.nio.ByteBuffer;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Writes one producer attempt's output for a single partition into a {@link SpoolStore} (ADR-002 D6).
 *
 * <p>Writes are buffered into a pending segment that becomes readable only after {@link #commit()}.
 * Closing without committing aborts the segment (e.g. a failed or speculative-loser attempt), leaving
 * no readable data — this is what lets {@link SpoolStore#read} dedup attempts safely.
 */
@Experimental(reason = "ADR-002 D6 persistent-plane spool SPI; evolving alongside D4 providers")
public interface SpoolWriter extends AutoCloseable {

    /** Appends an Arrow-IPC encoded chunk to the pending segment. The buffer is fully consumed. */
    void write(ByteBuffer arrowIpc);

    /** Atomically publishes the pending segment, making it visible to readers. */
    void commit();

    /** Aborts the pending segment if it was not committed; idempotent. */
    @Override
    void close();
}
