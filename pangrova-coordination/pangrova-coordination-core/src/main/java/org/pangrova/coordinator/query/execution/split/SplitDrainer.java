package org.pangrova.coordinator.query.execution.split;

import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Drains all splits from a {@link ConnectorSplitSource} by looping until
 * {@link ConnectorSplitSource#isFinished()} returns true.
 *
 * <p>The SPI contract requires callers to call {@code getNextBatch()} repeatedly
 * until {@code isFinished()} — a single call is insufficient for paged sources.</p>
 */
public final class SplitDrainer {

    private static final int DEFAULT_BATCH_SIZE = 1024;

    private SplitDrainer() {}

    /**
     * Drains all splits from the source. Does NOT close the source — caller owns lifecycle.
     *
     * @param source the split source to drain
     * @return all splits collected across all batches
     */
    public static List<ConnectorSplit> drainAll(ConnectorSplitSource source) {
        return drainAll(source, DEFAULT_BATCH_SIZE);
    }

    /**
     * Drains all splits from the source with a configurable batch size.
     *
     * @param source    the split source to drain
     * @param batchSize maximum splits per batch request
     * @return all splits collected across all batches
     */
    public static List<ConnectorSplit> drainAll(ConnectorSplitSource source, int batchSize) {
        List<ConnectorSplit> allSplits = new ArrayList<>();
        while (!source.isFinished()) {
            List<ConnectorSplit> batch = source.getNextBatch(batchSize);
            allSplits.addAll(batch);
        }
        return allSplits;
    }
}
