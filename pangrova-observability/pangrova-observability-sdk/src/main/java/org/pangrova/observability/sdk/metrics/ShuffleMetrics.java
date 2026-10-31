package org.pangrova.observability.sdk.metrics;

import org.pangrova.observability.api.DoubleHistogram;
import org.pangrova.observability.api.LongCounter;
import org.pangrova.observability.api.LongUpDownCounter;
import org.pangrova.observability.api.MetricAttributes;
import org.pangrova.observability.api.MetricsProvider;

import java.util.List;
import java.util.Map;

/**
 * End-to-end shuffle observability metrics for the native Arrow Flight data plane.
 *
 * <p>Pangrova's production shuffle path runs entirely in C++ (FlightShuffleServer →
 * Arrow Flight → PangrovaExchangeSource); Java only orchestrates lifecycle commands
 * via JNI. As a result the JVM has no direct visibility into bytes/rows actually
 * exchanged. This class fills the gap by emitting metrics at the natural Java
 * boundary: when the worker collects per-stage shuffle statistics (after the
 * native server drains) and when the coordinator receives those reports.</p>
 *
 * <h3>Emitted metrics</h3>
 *
 * <table>
 *   <caption>Shuffle observability surface</caption>
 *   <tr><th>Name</th><th>Instrument</th><th>Unit</th><th>Attributes</th></tr>
 * <tr><td>{@code pangrova.shuffle.bytes.sent}</td><td>Counter</td><td>{@code By}</td><td>{@code
 * stage.id}</td></tr>
 * <tr><td>{@code pangrova.shuffle.bytes.received}</td><td>Counter</td><td>{@code By}</td><td>{@code
 * stage.id}</td></tr>
 * <tr><td>{@code pangrova.shuffle.partitions.served}</td><td>Counter</td><td>{@code {partition}}</td><td>{@code
 * stage.id}</td></tr>
 * <tr><td>{@code pangrova.shuffle.send.duration}</td><td>Histogram</td><td>{@code s}</td><td>{@code
 * stage.id}</td></tr>
 * <tr><td>{@code pangrova.shuffle.partition.size.bytes}</td><td>Histogram</td><td>{@code By}</td><td>{@code
 * stage.id}</td></tr>
 * <tr><td>{@code pangrova.shuffle.requests}</td><td>Counter</td><td>{@code {request}}</td><td>{@code
 * rpc.method}</td></tr>
 * <tr><td>{@code pangrova.shuffle.request.duration}</td><td>Histogram</td><td>{@code s}</td><td>{@code
 * rpc.method}</td></tr>
 * <tr><td>{@code pangrova.shuffle.backpressure.events}</td><td>Counter</td><td>{@code {event}}</td><td>{@code
 * rpc.method}</td></tr>
 * <tr><td>{@code pangrova.shuffle.active.connections}</td><td>UpDownCounter</td><td>{@code
 * {connection}}</td><td>&mdash;</td></tr>
 * </table>
 *
 * <p>Naming follows {@code docs/metrics-conventions.md}: dot-separated lowercase,
 * past-tense verbs on counters, "duration" not "latency" on histograms, OTel units
 * via the instrument {@code unit} parameter.</p>
 *
 * <p>{@code stage.id} is bounded by the active query DAG (low cardinality per query)
 * and is the key dimension AQE consumes to detect per-stage skew, partition
 * size distributions, and stage-level send latency. The per-partition size
 * histogram is the missing input that lets AQE see the real data path
 * distribution rather than planner estimates.</p>
 *
 * <p>Thread-safe: all underlying counter / histogram instruments are thread-safe
 * by SPI contract.</p>
 */
public final class ShuffleMetrics {

    /**
     * Recommended bucket boundaries for shuffle send duration in seconds.
     * Covers fast (< 100 ms) through long-running stages (5 min).
     */
    private static final List<Double> SEND_DURATION_BOUNDARIES_S = List.of(
            0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0);

    /**
     * Recommended bucket boundaries for partition data size in bytes.
     * Covers tiny partitions (1 KiB) through 1 GiB skewed partitions.
     */
    private static final List<Double> PARTITION_SIZE_BOUNDARIES_B = List.of(
            1024.0, 65_536.0, 1_048_576.0, 16_777_216.0,
            134_217_728.0, 1_073_741_824.0);

    /**
     * Recommended bucket boundaries for per-request duration in seconds.
     * Covers fast single-batch reads (< 1 ms) through slow streaming reads (30 s).
     */
    private static final List<Double> REQUEST_DURATION_BOUNDARIES_S = List.of(
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0, 30.0);

    private static final String STAGE_ID_KEY = "stage.id";
    private static final String RPC_METHOD_KEY = "rpc.method";

    // -- Stage-level metrics (populated post-stage from native stats) --

    private final LongCounter shuffleBytesSent;
    private final LongCounter shuffleBytesReceived;
    private final LongCounter shufflePartitionsServed;
    private final DoubleHistogram shuffleSendDuration;
    private final DoubleHistogram shufflePartitionSizeBytes;

    // -- Per-request / live data-plane metrics (F-SHUFFLE-07) --

    private final LongCounter shuffleRequests;
    private final DoubleHistogram shuffleRequestDuration;
    private final LongCounter shuffleBackpressureEvents;
    private final LongUpDownCounter shuffleActiveConnections;

    /**
     * Registers the full shuffle metric set (stage-level and per-request) with the provider.
     */
    public ShuffleMetrics(MetricsProvider provider) {
        // Stage-level
        this.shuffleBytesSent = provider.createCounter(
                "pangrova.shuffle.bytes.sent",
                "Total bytes sent across all native Flight shuffle producers",
                "By");
        this.shuffleBytesReceived = provider.createCounter(
                "pangrova.shuffle.bytes.received",
                "Total bytes received across all native Flight shuffle consumers",
                "By");
        this.shufflePartitionsServed = provider.createCounter(
                "pangrova.shuffle.partitions.served",
                "Total shuffle partitions served by upstream stages",
                "{partition}");
        this.shuffleSendDuration = provider.createHistogram(
                "pangrova.shuffle.send.duration",
                "Wall-clock duration of a shuffle stage's send phase",
                "s",
                SEND_DURATION_BOUNDARIES_S);
        this.shufflePartitionSizeBytes = provider.createHistogram(
                "pangrova.shuffle.partition.size.bytes",
                "Per-partition data size for AQE skew/coalesce decisions",
                "By",
                PARTITION_SIZE_BOUNDARIES_B);

        // Per-request / live data-plane (F-SHUFFLE-07)
        this.shuffleRequests = provider.createCounter(
                "pangrova.shuffle.requests",
                "Total DoGet/DoPut/streaming requests served by the shuffle server",
                "{request}");
        this.shuffleRequestDuration = provider.createHistogram(
                "pangrova.shuffle.request.duration",
                "Per-request wall-clock duration for shuffle read/write operations",
                "s",
                REQUEST_DURATION_BOUNDARIES_S);
        this.shuffleBackpressureEvents = provider.createCounter(
                "pangrova.shuffle.backpressure.events",
                "Times the shuffle server applied backpressure (RESOURCE_EXHAUSTED or spill)",
                "{event}");
        this.shuffleActiveConnections = provider.createUpDownCounter(
                "pangrova.shuffle.active.connections",
                "Current number of active shuffle server connections (DoGet/DoPut streams)",
                "{connection}");
    }

    /**
     * Records the send-side completion of a shuffle stage on the upstream worker.
     *
     * <p>Called when the worker has gathered the post-stage shuffle statistics
     * (typically when the native FlightShuffleServer's queue is drained).
     * Increments {@code pangrova.shuffle.bytes.sent} by the total stage bytes,
     * {@code pangrova.shuffle.partitions.served} by the partition count, records
     * the wall-clock duration of the send phase, and emits one
     * per-partition-size observation per partition for AQE.</p>
     *
     * @param stageId          the shuffle stage identifier
     * @param totalBytes       total bytes shuffled (must be non-negative)
     * @param partitionBytes   per-partition data sizes in bytes (may be empty)
     * @param sendDurationSec  wall-clock send duration in seconds
     *                         (must be non-negative; {@code 0} skips the histogram)
     */
    public void recordSendCompleted(String stageId,
                                    long totalBytes,
                                    Map<Integer, Long> partitionBytes,
                                    double sendDurationSec) {
        MetricAttributes attrs = stageAttributes(stageId);
        if (totalBytes > 0) {
            shuffleBytesSent.add(totalBytes, attrs);
        }
        if (partitionBytes != null && !partitionBytes.isEmpty()) {
            shufflePartitionsServed.add(partitionBytes.size(), attrs);
            for (Long size : partitionBytes.values()) {
                if (size != null && size >= 0) {
                    shufflePartitionSizeBytes.record(size.doubleValue(), attrs);
                }
            }
        }
        if (sendDurationSec > 0) {
            shuffleSendDuration.record(sendDurationSec, attrs);
        }
    }

    /**
     * Records the receive-side completion of a shuffle stage on the coordinator.
     *
     * <p>Called when the coordinator stores a {@code ShuffleStageStatsReport}
     * onto a {@code Stage} (the trip-wire indicating the downstream consumers
     * finished pulling the stage's shuffle data). Increments
     * {@code pangrova.shuffle.bytes.received} by the stage bytes total.</p>
     *
     * @param stageId    the shuffle stage identifier
     * @param totalBytes total bytes received (must be non-negative)
     */
    public void recordReceiveCompleted(String stageId, long totalBytes) {
        if (totalBytes <= 0) {
            return;
        }
        shuffleBytesReceived.add(totalBytes, stageAttributes(stageId));
    }

    // -- Per-request data-plane recording (F-SHUFFLE-07) ----------------------

    /**
     * Records a single shuffle request completion.
     *
     * <p>Called from the gRPC interceptor or transport layer each time a
     * DoGet, DoPut, readPartition, or writeBatch RPC completes. Increments
     * the request counter and records the per-request duration.</p>
     *
     * @param rpcMethod      the RPC method name (e.g. "readPartition", "writeBatch")
     * @param durationSec    wall-clock duration of the request in seconds
     * @param bytesTransferred bytes transferred in this request (sent or received)
     */
    public void recordRequest(String rpcMethod, double durationSec, long bytesTransferred) {
        MetricAttributes attrs = methodAttributes(rpcMethod);
        shuffleRequests.add(1, attrs);
        if (durationSec > 0) {
            shuffleRequestDuration.record(durationSec, attrs);
        }
        if (bytesTransferred > 0) {
            shuffleBytesSent.add(bytesTransferred, attrs);
        }
    }

    /**
     * Records a backpressure event (e.g. RESOURCE_EXHAUSTED, spill triggered).
     *
     * @param rpcMethod the RPC method during which backpressure was applied
     */
    public void recordBackpressureEvent(String rpcMethod) {
        shuffleBackpressureEvents.add(1, methodAttributes(rpcMethod));
    }

    /**
     * Increments the active connection count. Call when a new DoGet/DoPut
     * stream begins.
     */
    public void connectionOpened() {
        shuffleActiveConnections.add(1);
    }

    /**
     * Decrements the active connection count. Call when a DoGet/DoPut
     * stream completes or errors.
     */
    public void connectionClosed() {
        shuffleActiveConnections.add(-1);
    }

    private static MetricAttributes stageAttributes(String stageId) {
        return MetricAttributes.of(STAGE_ID_KEY, stageId == null ? "unknown" : stageId);
    }

    private static MetricAttributes methodAttributes(String rpcMethod) {
        return MetricAttributes.of(RPC_METHOD_KEY, rpcMethod == null ? "unknown" : rpcMethod);
    }

    // -- Test introspection accessors -----------------------------------------

    LongCounter getShuffleBytesSent() {
        return shuffleBytesSent;
    }

    LongCounter getShuffleBytesReceived() {
        return shuffleBytesReceived;
    }

    LongCounter getShufflePartitionsServed() {
        return shufflePartitionsServed;
    }

    DoubleHistogram getShuffleSendDuration() {
        return shuffleSendDuration;
    }

    DoubleHistogram getShufflePartitionSizeBytes() {
        return shufflePartitionSizeBytes;
    }

    LongCounter getShuffleRequests() {
        return shuffleRequests;
    }

    DoubleHistogram getShuffleRequestDuration() {
        return shuffleRequestDuration;
    }

    LongCounter getShuffleBackpressureEvents() {
        return shuffleBackpressureEvents;
    }

    LongUpDownCounter getShuffleActiveConnections() {
        return shuffleActiveConnections;
    }
}
