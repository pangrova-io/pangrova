package org.pangrova.worker.exchange.source;

/**
 * Callback interface for streaming columnar IPC batch consumption.
 * Each batch is delivered as serialized columnar IPC bytes, enabling
 * per-batch processing without full stream materialization.
 */
@FunctionalInterface
public interface ExchangeBatchConsumer {

    /**
     * Accepts a single columnar IPC serialized record batch.
     *
     * @param serializedBatchBytes serialized columnar IPC bytes for one batch
     */
    void accept(byte[] serializedBatchBytes);
}
