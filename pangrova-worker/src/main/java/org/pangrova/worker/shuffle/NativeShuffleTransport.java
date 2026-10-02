package org.pangrova.worker.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.spi.execution.TaskExecutor;
import org.pangrova.shuffle.ShufflePartitionTicket;
import org.pangrova.shuffle.ShuffleServerHandle;
import org.pangrova.shuffle.ShuffleTransport;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.StartShuffleServerRequest;
import org.pangrova.protocol.v2.StartShuffleServerResponse;
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
 * </ul>
 *
 * @see ShuffleTransport
 */
public final class NativeShuffleTransport implements ShuffleTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NativeShuffleTransport.class);

    private final TaskExecutor taskExecutor;
    private final String localHost;
    private final String localTlsIdentity;

    /**
     * @param taskExecutor native task executor for issuing JNI commands
     * @param localHost    hostname to report in ShuffleServerHandle
     * @param localTlsIdentity certificate identity advertised to peer workers
     */
    public NativeShuffleTransport(
            TaskExecutor taskExecutor, String localHost, String localTlsIdentity) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.localHost = localHost != null ? localHost : "localhost";
        this.localTlsIdentity = localTlsIdentity == null ? "" : localTlsIdentity.trim();
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

        StartShuffleServerRequest commandPayload = buildStartFlightServerPayload(numPartitions);
        byte[] command = NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_START_SHUFFLE_SERVER,
                commandPayload);
        byte[] responseBytes = taskExecutor.executeCommand(executorHandle, command);
        StartShuffleServerResponse response = extractResponse(responseBytes);
        int boundPort = response.getBoundPort();
        LOG.info("event=native_flight_server_started executorHandle={} numPartitions={} port={}",
                executorHandle, numPartitions, boundPort);

        var tickets = response.getPartitionTicketsList().stream()
                .map(ticket -> new ShufflePartitionTicket(
                        ticket.getPartitionId(), ticket.getSignedTicket().toByteArray(),
                        ticket.getExpiresAtEpochMs()))
                .toList();
        return new ShuffleServerHandle(localHost, boundPort, localTlsIdentity, tickets);
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
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_STOP_SHUFFLE_SERVER,
                null);
        NativeCommandBuilder.requireSuccess(
                taskExecutor.executeCommand(executorHandle, command), "stop native shuffle server");
        LOG.info("event=native_shuffle_server_stopped executorHandle={}", executorHandle);
    }

    // --- Proto-generated payload encoding ---

    /**
     * Builds StartFlightServerCommand payload using generated proto.
     */
    private static StartShuffleServerRequest buildStartFlightServerPayload(int numPartitions) {
        return StartShuffleServerRequest.newBuilder()
                .setPartitionCount(numPartitions)
                .build();
    }

    /**
     * Extracts bound_port from StartFlightServerResponse via generated proto.
     */
    private static StartShuffleServerResponse extractResponse(byte[] response) {
        byte[] payload = NativeCommandBuilder.extractResultPayload(response);
        if (payload == null || payload.length == 0) {
            throw new IllegalStateException(
                    "START_FLIGHT_SERVER command returned empty response");
        }
        try {
            return StartShuffleServerResponse.parseFrom(payload);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to parse StartFlightServerResponse", e);
        }
    }
}
