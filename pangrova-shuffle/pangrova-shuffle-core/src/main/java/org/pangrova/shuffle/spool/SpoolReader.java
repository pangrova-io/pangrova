package org.pangrova.shuffle.spool;

import java.nio.ByteBuffer;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Streams committed shuffle segments back from a {@link SpoolStore} (ADR-002 D6).
 *
 * <p>Iterates the committed segments of one partition over the requested map-index range in ascending
 * {@code mapId} order, one {@link ByteBuffer} per segment. The reader has already deduped speculative
 * / re-run attempts, so each {@code mapId} is yielded at most once.
 */
@Experimental(reason = "ADR-002 D6 persistent-plane spool SPI; evolving alongside D4 providers")
public interface SpoolReader extends AutoCloseable {

    /** Returns {@code true} if another segment is available. */
    boolean hasNext();

    /**
     * Returns the next segment's bytes.
     *
     * @throws java.util.NoSuchElementException if no further segment is available
     */
    ByteBuffer next();

    @Override
    void close();
}
