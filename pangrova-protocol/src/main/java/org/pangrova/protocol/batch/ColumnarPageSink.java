package org.pangrova.protocol.batch;

import org.pangrova.spi.connector.data.ConnectorPageSink;

/** Explicit synchronous columnar capability without changing the connector SPI. */
public interface ColumnarPageSink extends ConnectorPageSink {

    /**
     * Consumes a caller-owned batch before returning. Implementations must not retain
     * the batch or its vectors; detached cell values may be used in connector records.
     * Decoding still allocates column buffers; this is not an end-to-end zero-copy API.
     *
     * @param batch open batch, valid only for this call
     * @throws RuntimeException if conversion or writing fails; the caller must abort
     */
    void appendColumnarBatch(ColumnarBatch batch);
}
