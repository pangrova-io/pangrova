package org.pangrova.client.rest;

/**
 * Callback interface for streaming result batches from the data transport layer.
 * Used by {@link PangrovaRestClient#fetchResultStream(
 * org.pangrova.protocol.rest.query.ResultEndpoint, ResultBatchConsumer)}.
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
