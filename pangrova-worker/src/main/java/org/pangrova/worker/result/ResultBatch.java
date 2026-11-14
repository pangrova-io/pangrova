package org.pangrova.worker.result;

import java.nio.ByteBuffer;

/**
 * Discriminated result batch from {@link ResultTransport#nextBatch}.
 *
 * <p>Three variants:</p>
 * <ul>
 *   <li>{@link Data} — valid batch data with a release callback for back-pressure</li>
 *   <li>{@link End} — end of stream, no more batches</li>
 *   <li>{@link Error} — streaming error with diagnostic message</li>
 * </ul>
 */
public sealed interface ResultBatch {

    /**
     * A data batch containing serialized columnar IPC bytes.
     *
     * @param buffer  the batch data (caller must not modify)
     * @param release callback to signal consumption complete (enables back-pressure)
     */
    record Data(ByteBuffer buffer, Runnable release) implements ResultBatch {
    }

    /** End-of-stream marker — no more batches will be produced. */
    record End() implements ResultBatch {
    }

    /**
     * Error marker — streaming failed.
     *
     * @param message diagnostic error message
     */
    record Error(String message) implements ResultBatch {
    }
}
