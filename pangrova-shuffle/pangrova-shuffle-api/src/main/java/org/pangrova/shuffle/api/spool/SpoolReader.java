package org.pangrova.shuffle.api.spool;

import java.nio.ByteBuffer;
import org.pangrova.common.api.annotation.Experimental;

/** Synchronous storage reader over exact selected attempts; this is not the asynchronous Provider API. */
@Experimental(reason = "Local spool primitive; generic session and publication binding remain pending")
public interface SpoolReader extends AutoCloseable {
    /** Checks for another encoded batch; missing/corrupt/truncated data throws instead of reporting EOF. */
    boolean hasNext();

    /** Returns one bounded batch in producer order. The returned buffer is owned by the caller. */
    ByteBuffer next();

    /** Releases the read resource and its cleanup protection; idempotent after successful close. */
    @Override
    void close();
}
