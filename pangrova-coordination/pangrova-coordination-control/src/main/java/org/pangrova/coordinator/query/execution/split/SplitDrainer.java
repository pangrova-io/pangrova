package org.pangrova.coordinator.query.execution.split;

import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
        drainEach(source, batchSize, allSplits::addAll);
        return allSplits;
    }

    /**
     * Streams splits batch by batch without materializing the full enumeration.
     * Does NOT close the source — caller owns lifecycle.
     *
     * @param source    the split source to drain
     * @param batchSize maximum splits per batch request
     * @param consumer  receives each non-empty batch in enumeration order
     */
    public static void drainEach(ConnectorSplitSource source, int batchSize,
            Consumer<List<ConnectorSplit>> consumer) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        while (!source.isFinished()) {
            List<ConnectorSplit> batch = source.getNextBatch(batchSize);
            if (!batch.isEmpty()) {
                consumer.accept(batch);
            }
        }
    }
}
