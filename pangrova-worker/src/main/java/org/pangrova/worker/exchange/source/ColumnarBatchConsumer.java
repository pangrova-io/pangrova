package org.pangrova.worker.exchange.source;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Callback for streaming Arrow VectorSchemaRoot batches without
 * intermediate byte[] serialization. Enables zero-copy exchange
 * by passing the in-memory columnar batch directly.
 *
 * <p>The batch is owned by the transport stream and must not be retained
 * beyond the accept() call. Consumers should export or process the
 * batch synchronously.</p>
 */
@FunctionalInterface
public interface ColumnarBatchConsumer {

    /**
     * Processes a single VectorSchemaRoot batch from the transport stream.
     * The batch reference is valid only for the duration of this call.
     *
     * @param batch the columnar batch (do not retain after return)
     * @throws Exception if processing fails
     */
    void accept(VectorSchemaRoot batch) throws Exception;
}
