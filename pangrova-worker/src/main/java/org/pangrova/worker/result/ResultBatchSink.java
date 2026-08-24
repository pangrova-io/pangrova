package org.pangrova.worker.result;

/**
 * Callback interface for receiving result batches from the native engine.
 * Replaces the inner interface previously defined in ResultPuller.
 */
public interface ResultBatchSink {
    /**
     * Called when a new batch of IPC bytes is available.
     *
     * @param batchData raw columnar IPC stream bytes
     */
    void onBatch(byte[] batchData);

    /**
     * Called when the result stream is complete.
     */
    void complete();
}
