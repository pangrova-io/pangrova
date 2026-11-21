package org.pangrova.worker.result;

import static org.pangrova.common.util.Preconditions.checkNotNull;

import com.google.protobuf.Message;
import org.pangrova.worker.execution.TaskExecutor;
import org.pangrova.worker.jni.NativeCommandBuilder;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.StartResultServerRequest;
import org.pangrova.protocol.v2.StartResultServerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native result transport delegating to C++ NativeResultServer via JNI.
 *
 * <p>Result data flows entirely within C++: the native engine's PartitionedOutputNode serializes
 * into canonical typed batch streams held by its output buffer. The Native server pulls
 * the owned bytes, and the client-facing data-plane endpoint
 * fetches them directly. JVM components only issue lifecycle control commands and
 * publish the signed Native endpoint; they never relay result pages.</p>
 *
 *
 * @see ResultTransport
 */
public final class NativeResultTransport implements ResultTransport {

    private static final int MAX_PORT = 65_535;

    private static final Logger LOG = LoggerFactory.getLogger(NativeResultTransport.class);

    public static final RuntimeCommandType CMD_START_RESULT_SERVER =
            RuntimeCommandType.RUNTIME_COMMAND_TYPE_START_RESULT_SERVER;
    static final RuntimeCommandType CMD_STOP_RESULT_SERVER =
            RuntimeCommandType.RUNTIME_COMMAND_TYPE_STOP_RESULT_SERVER;

    private final TaskExecutor taskExecutor;
    private final String localHost;
    private final String bindHost;
    private volatile int boundPort = -1;

    /**
     * @param taskExecutor native task executor for issuing JNI commands
     * @param localHost    hostname to report in result server endpoint
     */
    public NativeResultTransport(TaskExecutor taskExecutor, String localHost) {
        this(taskExecutor, localHost, "0.0.0.0");
    }

    /** Separates the client-facing endpoint from the actual listener interface. */
    public NativeResultTransport(TaskExecutor taskExecutor, String localHost, String bindHost) {
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.localHost = localHost != null ? localHost : "localhost";
        if (bindHost == null || bindHost.isBlank()) {
            throw new IllegalArgumentException("bindHost must not be blank");
        }
        this.bindHost = bindHost.trim();
    }

    /**
     * Starts the C++ NativeResultServer via JNI executeCommand.
     *
     * <p>The native side creates a NativeResultServer that pulls pre-serialized
     * pages from the native OutputBufferManager and serves them via the data exchange protocol.
     * The bound port is returned so the control plane can publish a signed Native
     * result endpoint to the authorized client-facing data-plane consumer.</p>
     *
     * <p><b>Handle convention:</b> Uses {@code executorHandle=0} because this is a
     * global (per-worker) command, not scoped to a specific task executor. The
     * native dispatcher routes handle=0 commands to global handlers. This is safe
     * because {@link org.pangrova.worker.execution.NativeTaskExecutor#acquireReadLock}
     * only checks the {@code closed} flag, not the handle value.</p>
     *
     * @param preferredPort preferred port (0 for auto-assign)
     * @return the actual bound port
     * @throws IllegalStateException if the native command returns an invalid response
     */
    public int startServer(int preferredPort) {
        StartResultServerRequest commandPayload = buildStartResultServerPayload(preferredPort, bindHost);
        byte[] command = buildCommand(CMD_START_RESULT_SERVER, commandPayload);
        byte[] response = taskExecutor.executeCommand(0, command);

        int port = extractBoundPort(response);
        this.boundPort = port;
        LOG.info("event=native_result_server_started preferredPort={} boundPort={}",
                preferredPort, port);
        return port;
    }

    /**
     * Stops the C++ NativeResultServer via JNI executeCommand.
     *
     * <p>The native side gracefully shuts down the NativeResultServer,
     * draining in-flight data retrieval requests before termination.</p>
     */
    public void stopServer() {
        if (boundPort < 0) {
            LOG.debug("event=native_result_server_stop_skipped reason=not_started");
            return;
        }
        byte[] command = buildCommand(CMD_STOP_RESULT_SERVER, null);
        taskExecutor.executeCommand(0, command);
        boundPort = -1;
        LOG.info("event=native_result_server_stopped");
    }

    /**
     * Returns the bound port of the NativeResultServer, or -1 if not started.
     */
    public int boundPort() {
        return boundPort;
    }

    /**
     * Returns the local hostname for the result server endpoint.
     */
    public String localHost() {
        return localHost;
    }

    /**
     * Delegates to {@link #stopServer()} to gracefully shut down the
     * C++ NativeResultServer.
     */
    @Override
    public void close() {
        stopServer();
    }

    // --- Proto-generated payload encoding ---

    /**
     * Builds StartResultServerCommand payload using generated proto.
     * Port zero intentionally requests automatic port allocation.
     */
    public static StartResultServerRequest buildStartResultServerPayload(int preferredPort) {
        return buildStartResultServerPayload(preferredPort, "0.0.0.0");
    }

    /** Encodes the requested port and actual listener interface for the Native server. */
    public static StartResultServerRequest buildStartResultServerPayload(int preferredPort, String bindHost) {
        if (preferredPort < 0 || preferredPort > MAX_PORT) {
            throw new IllegalArgumentException("preferredPort must be in [0, 65535]");
        }
        if (bindHost == null || bindHost.isBlank()) {
            throw new IllegalArgumentException("bindHost must not be blank");
        }
        return StartResultServerRequest.newBuilder()
                .setPreferredPort(preferredPort)
                .setBindHost(bindHost.trim())
                .build();
    }

    /**
     * Builds a NativeCommand proto envelope.
     */
    public static byte[] buildCommand(
            RuntimeCommandType commandType, Message payload) {
        return NativeCommandBuilder.buildCommand(commandType, payload);
    }

    /**
     * Extracts bound_port from StartResultServerResponse via generated proto.
     */
    static int extractBoundPort(byte[] response) {
        byte[] payload = NativeCommandBuilder.extractResultPayload(response);
        if (payload == null || payload.length == 0) {
            throw new IllegalStateException(
                    "START_RESULT_SERVER command returned empty response");
        }
        try {
            return StartResultServerResponse.parseFrom(payload).getBoundPort();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to parse StartResultServerResponse", e);
        }
    }
}
