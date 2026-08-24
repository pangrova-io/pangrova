package org.pangrova.spi.connector.metadata;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;

/**
 * A simple split source that returns all splits in a single batch.
 * Extracted to SPI to eliminate duplication across connector implementations.
 */
@Experimental
public final class FixedSplitSource implements ConnectorSplitSource {

    private List<ConnectorSplit> splits;

    public FixedSplitSource(List<ConnectorSplit> splits) {
        this.splits = splits;
    }

    @Override
    public List<ConnectorSplit> getNextBatch(int maxSize) {
        List<ConnectorSplit> batch = splits;
        splits = List.of();
        return batch;
    }

    @Override
    public boolean isFinished() {
        return splits.isEmpty();
    }

    @Override
    public void close() {
        splits = List.of();
    }
}
