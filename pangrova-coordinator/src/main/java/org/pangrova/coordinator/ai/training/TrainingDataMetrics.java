package org.pangrova.coordinator.ai.training;

import org.pangrova.spi.observability.LongCounter;
import org.pangrova.spi.observability.LongUpDownCounter;
import org.pangrova.spi.observability.MetricsProvider;

/**
 * Metrics for the Training Data Server (data streaming endpoint).
 *
 * <p>Tracks four key metrics:</p>
 * <ul>
 *   <li>{@code pangrova.training.requests_total} — total data requests received</li>
 *   <li>{@code pangrova.training.bytes_served} — total bytes streamed to clients</li>
 *   <li>{@code pangrova.training.active_streams} — currently active data streams</li>
 *   <li>{@code pangrova.training.errors} — total errors encountered</li>
 * </ul>
 */
public final class TrainingDataMetrics {

    private final LongCounter requestsTotal;
    private final LongCounter bytesServed;
    private final LongUpDownCounter activeStreams;
    private final LongCounter errors;

    /**
     * Creates training data metrics registered with the given provider.
     *
     * @param provider the metrics provider for instrument creation
     */
    public TrainingDataMetrics(MetricsProvider provider) {
        this.requestsTotal = provider.createCounter(
                "pangrova.training.requests_total",
                "Total training data Flight requests received",
                "requests");
        this.bytesServed = provider.createCounter(
                "pangrova.training.bytes_served",
                "Total bytes streamed to training data clients",
                "bytes");
        this.activeStreams = provider.createUpDownCounter(
                "pangrova.training.active_streams",
                "Currently active training data streams",
                "streams");
        this.errors = provider.createCounter(
                "pangrova.training.errors",
                "Total errors in training data serving",
                "errors");
    }

    /** Records a new request received. */
    public void recordRequest() {
        requestsTotal.add(1);
    }

    /**
     * Records bytes served to a client.
     *
     * @param bytes number of bytes served (must be non-negative)
     */
    public void recordBytesServed(long bytes) {
        bytesServed.add(bytes);
    }

    /** Records a new stream opened. */
    public void recordStreamOpened() {
        activeStreams.add(1);
    }

    /** Records a stream closed. */
    public void recordStreamClosed() {
        activeStreams.add(-1);
    }

    /** Records an error encountered during serving. */
    public void recordError() {
        errors.add(1);
    }
}
