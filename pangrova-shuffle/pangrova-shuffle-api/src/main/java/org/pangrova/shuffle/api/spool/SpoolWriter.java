package org.pangrova.shuffle.api.spool;

import java.nio.ByteBuffer;
import org.pangrova.common.api.annotation.Experimental;

/** Exclusive synchronous storage writer for one attempt/partition, not an exchange commit owner. */
@Experimental(reason = "Local spool primitive; generic session and publication binding remain pending")
public interface SpoolWriter extends AutoCloseable {
    /** Appends one bounded encoded batch, consuming the buffer before return; retains no caller memory. */
    void write(ByteBuffer encodedBatch);

    /**
     * Completes the segment atomically, including an explicit empty segment if no batches were written.
     * Repeated success is idempotent. A failure may have an unknown completion outcome; it must not
     * authorize exchange publication or cause blind overwrite/deletion of a possibly completed segment.
     */
    void commit();

    /** Closes the writer, aborting its temporary segment only; never deletes a completed segment. */
    @Override
    void close();
}
