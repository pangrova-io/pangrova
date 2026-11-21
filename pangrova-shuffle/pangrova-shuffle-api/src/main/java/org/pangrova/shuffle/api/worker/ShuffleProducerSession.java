package org.pangrova.shuffle.api.worker;

import org.pangrova.common.api.annotation.Experimental;

/** Admits producer attempts against a complete, immutable request before any encoded batch callback. */
@Experimental(reason = "Exact producer admission and release")
public interface ShuffleProducerSession<A> extends ShuffleByteEgress, AutoCloseable {
    /** Returns false only for requests without an external binding. Rejects conflicting replays. */
    boolean installTask(A admission);

    /** Releases the exact original admission. May be retried after an interrupted drain. */
    void releaseTask(A admission);

    /** Stops admission and drains every producer, including unpublished candidates. */
    @Override void close();
}
