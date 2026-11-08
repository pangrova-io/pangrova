package org.pangrova.worker.nativebridge.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.shuffle.api.ShufflePartitionTicket;
import org.pangrova.shuffle.api.ShuffleServerHandle;
import org.pangrova.worker.nativebridge.jni.NativeCommandBuilder;
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
 * <p>This adapter does not own an executor or a server registry. Task lifecycle code
 * starts/stops serving for its own handle, then releases that executor. It must supply
 * the original guarded command channel; this adapter never bypasses handle leases
 * by calling NativeRuntime directly. There is no process-wide close operation.</p>
 */
public final class NativeShuffleTransport {

    /** Borrowed, synchronous command path owned and validated by the task executor. */
    @FunctionalInterface
    public interface CommandChannel {
        /** Executes one canonical command without transferring or closing the borrowed handle. */
        byte[] execute(long executorHandle, byte[] commandProto);
    }

    private static final Logger LOG = LoggerFactory.getLogger(NativeShuffleTransport.class);

    private final CommandChannel commandChannel;
    private final String localHost;
    private final String localTlsIdentity;

    /**
     * @param commandChannel borrowed task command channel retaining the caller's handle admission and lease checks
     * @param localHost    hostname to report in ShuffleServerHandle
     * @param localTlsIdentity certificate identity advertised to peer workers
     */
    public NativeShuffleTransport(
            CommandChannel commandChannel, String localHost, String localTlsIdentity) {
        this.commandChannel = checkNotNull(commandChannel, "commandChannel must not be null");
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
        byte[] responseBytes = commandChannel.execute(executorHandle, command);
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
                commandChannel.execute(executorHandle, command), "stop native shuffle server");
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
