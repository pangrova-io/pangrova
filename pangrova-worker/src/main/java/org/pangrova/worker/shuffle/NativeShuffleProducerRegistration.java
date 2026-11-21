package org.pangrova.worker.shuffle;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import org.pangrova.shuffle.api.ShufflePartitionTicket;
import org.pangrova.shuffle.api.ShuffleServerHandle;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.StartShuffleServerRequest;
import org.pangrova.protocol.v2.StartShuffleServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers a task's Direct shuffle output with the process-wide native Flight server.
 * The guarded executor command borrows a handle for each call. C++ retains the
 * producer registration and serves encoded data directly to downstream Workers.
 */
public final class NativeShuffleProducerRegistration {

    /** Borrowed, synchronous command path owned and validated by the task executor. */
    @FunctionalInterface
    public interface CommandChannel {
        /** Executes one canonical command without transferring or closing the borrowed handle. */
        byte[] execute(long executorHandle, byte[] commandProto);
    }

    private static final Logger LOG = LoggerFactory.getLogger(NativeShuffleProducerRegistration.class);

    private final CommandChannel commandChannel;
    private final String localHost;
    private final boolean localDisk;
    private final java.util.concurrent.atomic.AtomicInteger processPort =
            new java.util.concurrent.atomic.AtomicInteger();
    private final String localTlsIdentity;

    /**
     * @param commandChannel borrowed task command channel retaining the caller's handle admission and lease checks
     * @param localHost    hostname to report in ShuffleServerHandle
     * @param localTlsIdentity certificate identity advertised to peer workers
     */
    public NativeShuffleProducerRegistration(
            CommandChannel commandChannel, String localHost, String localTlsIdentity) {
        this(commandChannel, localHost, localTlsIdentity, false);
    }

    /** Creates the registration, optionally backed by local disk spool storage. */
    public NativeShuffleProducerRegistration(CommandChannel commandChannel, String localHost, String localTlsIdentity,
            boolean localDisk) {
        this.localDisk = localDisk;
        this.commandChannel = checkNotNull(commandChannel, "commandChannel must not be null");
        this.localHost = localHost != null ? localHost : "localhost";
        this.localTlsIdentity = localTlsIdentity == null ? "" : localTlsIdentity.trim();
    }

    /** Validate task routing before allocating a Native executor or opening any Flight connection. */
    public void validateTask(org.pangrova.protocol.v2.TaskRequest request) {
        var options = request.getExecutionOptions();
        if (options.hasShuffle()) {
            boolean disk = options.getShuffle().getPersistenceMode()
                    == org.pangrova.protocol.v2.ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK;
            if (disk != localDisk) throw new IllegalArgumentException(
                    "Task shuffle storage differs from Worker deployment");
        }
        if (!localDisk) return;
        if ((options.hasShuffle() || request.getExchangeInputsCount() != 0)
                && request.getAttempt().getAttemptId() != 0) {
            throw new IllegalArgumentException("Local shuffle cannot execute a replacement attempt");
        }
        for (var input : request.getExchangeInputsList()) {
            if (!input.hasDirect() || !input.getNoMoreSources()
                    || input.getReadMode() != org.pangrova.protocol.v2.ExchangeReadMode.EXCHANGE_READ_MODE_BLOCKING) {
                throw new IllegalArgumentException("Local shuffle requires completed blocking producer bindings");
            }
            for (var source : input.getDirect().getSourcesList()) {
                if (source.getAttemptId() != 0 || !source.getHost().equals(localHost)
                        || source.getPort() <= 0 || source.getPort() != processPort.get()
                        || !source.getTlsIdentity().equals(localTlsIdentity)) {
                    throw new IllegalArgumentException("Local shuffle source was not registered on this Worker");
                }
            }
        }
    }

    /** Registers the producer and returns its signed tickets on the shared Flight port. */
    public ShuffleServerHandle registerProducer(long executorHandle, int numPartitions) {
        if (executorHandle == 0) {
            throw new IllegalArgumentException("executorHandle must not be zero");
        }
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive, got: " + numPartitions);
        }

        StartShuffleServerRequest commandPayload = buildStartShuffleServerPayload(numPartitions);
        byte[] command = NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_START_SHUFFLE_SERVER,
                commandPayload);
        byte[] responseBytes = commandChannel.execute(executorHandle, command);
        StartShuffleServerResponse response = extractResponse(responseBytes);
        int boundPort = response.getBoundPort();
        if (localDisk) processPort.set(boundPort);
        LOG.info("event=native_shuffle_producer_registered executorHandle={} numPartitions={} port={}",
                executorHandle, numPartitions, boundPort);

        var tickets = response.getPartitionTicketsList().stream()
                .map(ticket -> new ShufflePartitionTicket(
                        ticket.getPartitionId(), ticket.getSignedTicket().toByteArray(),
                        ticket.getExpiresAtEpochMs()))
                .toList();
        return new ShuffleServerHandle(localHost, boundPort, localTlsIdentity, tickets);
    }

    /**
     * Closes this handle's registration. Native retains any clean buffered output until
     * readers finish. The Worker runtime owns the shared server and the task owns its executor.
     */
    public void unregisterProducer(long executorHandle) {
        if (executorHandle == 0) {
            return;
        }
        byte[] command = NativeCommandBuilder.buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_STOP_SHUFFLE_SERVER,
                null);
        NativeCommandBuilder.requireSuccess(
                commandChannel.execute(executorHandle, command), "unregister native shuffle producer");
        LOG.info("event=native_shuffle_producer_unregistered executorHandle={}", executorHandle);
    }

    // --- Proto-generated payload encoding ---

    /**
     * Builds StartShuffleServerCommand payload using generated proto.
     */
    private static StartShuffleServerRequest buildStartShuffleServerPayload(int numPartitions) {
        return StartShuffleServerRequest.newBuilder()
                .setPartitionCount(numPartitions)
                .build();
    }

    /**
     * Extracts bound_port from StartShuffleServerResponse via generated proto.
     */
    private static StartShuffleServerResponse extractResponse(byte[] response) {
        byte[] payload = NativeCommandBuilder.extractResultPayload(response);
        if (payload == null || payload.length == 0) {
            throw new IllegalStateException(
                    "START_SHUFFLE_SERVER command returned empty response");
        }
        try {
            return StartShuffleServerResponse.parseFrom(payload);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to parse StartShuffleServerResponse", e);
        }
    }
}
