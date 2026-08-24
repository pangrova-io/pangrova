package org.pangrova.worker.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.shuffle.ShuffleEndpoint;
import org.pangrova.shuffle.ShuffleServerHandle;
import org.pangrova.shuffle.ShuffleTransport;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.worker.jni.NativeCommandType;
import org.pangrova.worker.jni.proto.ConnectExchangeSourcePayload;
import org.pangrova.worker.jni.proto.StartFlightServerPayload;
import org.pangrova.worker.jni.proto.StartFlightServerResponsePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native shuffle transport delegating to C++ via JNI.
 *
 * <p>Shuffle data flows entirely within C++: native engine →
 * SerializedShuffleQueue → FlightShuffleServer → data exchange →
 * downstream PangrovaExchangeSource. Java only issues control commands.</p>
 *
 * <p>JNI commands used:</p>
 * <ul>
 *   <li>{@code START_FLIGHT_SERVER} — starts C++ FlightShuffleServer,
 *       returns bound port</li>
 *   <li>{@code CONNECT_EXCHANGE_SOURCE} — tells C++ PangrovaExchangeSource
 *       to connect to an upstream Flight server</li>
 * </ul>
 *
 * @see ShuffleTransport
 */
public final class NativeShuffleTransport implements ShuffleTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NativeShuffleTransport.class);

    private final TaskExecutor taskExecutor;
    private final String localHost;

    /**
     * @param taskExecutor native task executor for issuing JNI commands
     * @param localHost    hostname to report in ShuffleServerHandle
     */
    public NativeShuffleTransport(TaskExecutor taskExecutor, String localHost) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.localHost = localHost != null ? localHost : "localhost";
    }

    /**
     * Starts the C++ FlightShuffleServer via JNI executeCommand.
     *
     * <p>The native side creates a FlightShuffleServer that reads from the
     * existing SerializedShuffleQueue and serves data via the native data exchange protocol.
     * The bound port is returned so the coordinator can distribute it to
     * downstream tasks.</p>
     */
    @Override
    public ShuffleServerHandle startServer(long executorHandle, int numPartitions) {
        if (executorHandle == 0) {
            throw new IllegalArgumentException("executorHandle must not be zero");
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive, got: " + numPartitions);
        }

        byte[] commandPayload = buildStartFlightServerPayload(numPartitions);
        byte[] command = NativeCommandBuilder.buildCommand(executorHandle, NativeCommandType.START_SHUFFLE_SERVER,
                commandPayload);
        byte[] response = taskExecutor.executeCommand(executorHandle, command);

        int boundPort = extractBoundPort(response);
        LOG.info("event=native_flight_server_started executorHandle={} numPartitions={} port={}",
                executorHandle, numPartitions, boundPort);

        return new ShuffleServerHandle(localHost, boundPort, executorHandle);
    }

    /**
     * Connects a C++ PangrovaExchangeSource to an upstream Flight server via JNI.
     *
     * <p>The native side calls setFlightEndpoint() on the PangrovaExchangeSource,
     * switching it from passive (Java push) to active (Flight pull) mode.</p>
     */
    @Override
    public void connectSource(long executorHandle, String sourceId, ShuffleEndpoint upstream) {
        checkNotNull(sourceId, "sourceId must not be null");
        checkNotNull(upstream, "upstream must not be null");

        byte[] commandPayload = buildConnectExchangeSourcePayload(
                sourceId, upstream.host(), upstream.port(),
                upstream.queryId(), upstream.stageId(), upstream.partitionId());
        byte[] command = NativeCommandBuilder.buildCommand(executorHandle, NativeCommandType.CONNECT_EXCHANGE_SOURCE,
                commandPayload);
        taskExecutor.executeCommand(executorHandle, command);

        LOG.debug("event=native_exchange_source_connected executorHandle={} sourceId={} upstream={}:{}",
                executorHandle, sourceId, upstream.host(), upstream.port());
    }

    @Override
    public void noMoreSources(long executorHandle) {
        if (executorHandle == 0) {
            LOG.debug("event=native_shuffle_no_more_sources_skipped reason=zero_handle");
            return;
        }
        byte[] command = NativeCommandBuilder.buildCommand(
                executorHandle, NativeCommandType.NO_MORE_EXCHANGE_SOURCES, new byte[0]);
        taskExecutor.executeCommand(executorHandle, command);
        LOG.debug("event=native_shuffle_no_more_sources executorHandle={}", executorHandle);
    }

    @Override
    public void close() {
        LOG.info("event=native_shuffle_transport_closed");
    }

    /**
     * Stops the per-executor shuffle server via JNI command.
     * Called on task completion or failure to prevent port and memory leaks.
     * Idempotent — safe to call if no server was started for this handle.
     *
     * @param executorHandle native executor handle that owns the shuffle server
     */
    public void stopServer(long executorHandle) {
        if (executorHandle == 0) {
            return;
        }
        byte[] command = NativeCommandBuilder.buildCommand(
                executorHandle, NativeCommandType.STOP_SHUFFLE_SERVER, new byte[0]);
        try {
            taskExecutor.executeCommand(executorHandle, command);
            LOG.info("event=native_shuffle_server_stopped executorHandle={}", executorHandle);
        } catch (Exception e) {
            LOG.warn("event=native_shuffle_server_stop_failed executorHandle={} error={}",
                    executorHandle, e.getMessage());
        }
    }

    // --- Proto-generated payload encoding ---

    /**
     * Builds StartFlightServerCommand payload using generated proto.
     */
    private static byte[] buildStartFlightServerPayload(int numPartitions) {
        return StartFlightServerPayload.newBuilder()
                .setNumPartitions(numPartitions)
                .build()
                .toByteArray();
    }

    /**
     * Builds ConnectExchangeSourceCommand payload using generated proto.
     */
    private static byte[] buildConnectExchangeSourcePayload(
            String sourceId, String host, int port,
            String queryId, int stageId, int partitionId) {
        return ConnectExchangeSourcePayload.newBuilder()
                .setSourceId(sourceId)
                .setHost(host)
                .setPort(port)
                .setQueryId(queryId)
                .setStageId(stageId)
                .setPartitionId(partitionId)
                .build()
                .toByteArray();
    }

    /**
     * Extracts bound_port from StartFlightServerResponse via generated proto.
     */
    private static int extractBoundPort(byte[] response) {
        byte[] payload = NativeCommandBuilder.extractResultPayload(response);
        if (payload == null || payload.length == 0) {
            throw new IllegalStateException(
                    "START_FLIGHT_SERVER command returned empty response");
        }
        try {
            return StartFlightServerResponsePayload.parseFrom(payload).getBoundPort();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to parse StartFlightServerResponse", e);
        }
    }
}
