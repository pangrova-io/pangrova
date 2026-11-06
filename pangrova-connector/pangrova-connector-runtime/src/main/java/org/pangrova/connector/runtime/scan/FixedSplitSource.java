package org.pangrova.connector.runtime.scan;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.ConnectorSplitSource;

import java.util.List;

/** In-memory split enumeration with bounded batches and idempotent close. */
@Experimental
public final class FixedSplitSource implements ConnectorSplitSource {
    private List<ConnectorSplit> splits;
    private int position;

    public FixedSplitSource(List<ConnectorSplit> splits) {
        this.splits = List.copyOf(splits);
    }

    @Override
    public synchronized List<ConnectorSplit> getNextBatch(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive");
        }
        int end = position + Math.min(maxSize, splits.size() - position);
        List<ConnectorSplit> batch = List.copyOf(splits.subList(position, end));
        position = end;
        if (position == splits.size()) {
            close();
        }
        return batch;
    }

    @Override
    public synchronized boolean isFinished() {
        return position == splits.size();
    }

    @Override
    public synchronized void close() {
        splits = List.of();
        position = 0;
    }
}
