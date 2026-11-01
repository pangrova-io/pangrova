package org.pangrova.client.api;

/**
 * Callback interface for streaming result batches from the data transport layer.
 * Implemented by clients that consume Arrow IPC batches from a result stream.
 */
@FunctionalInterface
public interface ResultBatchConsumer {

    /**
     * Called for each Arrow IPC batch received from the result stream.
     *
     * @param batchData Arrow IPC format byte array for a single batch
     */
    void accept(byte[] batchData);
}
