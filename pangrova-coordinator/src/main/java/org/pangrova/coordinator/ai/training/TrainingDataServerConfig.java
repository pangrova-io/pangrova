package org.pangrova.coordinator.ai.training;

/**
 * Immutable configuration for the Training Data Server (data streaming endpoint).
 *
 * <p>Task 162: the {@code enabled} flag defaults to {@code false} so the
 * training data server is not started unless explicitly opted-in. Running
 * a data-serving endpoint on the coordinator violates the read-write
 * separation design -- it exposes scan-path data on a control-plane node.</p>
 *
 * @param enabled             whether the training data server is active (default false)
 * @param port                gRPC listen port (default 9090)
 * @param maxConcurrentStreams maximum concurrent data streams (default 50)
 * @param authType            authentication type: "jwt", "mtls", or "none" (default "jwt")
 * @param maxBatchRows        maximum rows per columnar batch sent to client (default 65536)
 */
public record TrainingDataServerConfig(
        boolean enabled,
        int port,
        int maxConcurrentStreams,
        String authType,
        int maxBatchRows) {

    /** Default port for training data serving. */
    public static final int DEFAULT_PORT = 9090;

    /** Default maximum concurrent streams. */
    public static final int DEFAULT_MAX_CONCURRENT_STREAMS = 50;

    /** Default authentication type. */
    public static final String DEFAULT_AUTH_TYPE = "jwt";

    /** Default maximum rows per columnar batch. */
    public static final int DEFAULT_MAX_BATCH_ROWS = 65_536;

    /** Highest valid TCP port number. */
    private static final int MAX_PORT = 65_535;

    public TrainingDataServerConfig {
        if (port <= 0 || port > MAX_PORT) {
            throw new IllegalArgumentException("port must be in range [1, " + MAX_PORT + "], got: " + port);
        }
        if (maxConcurrentStreams <= 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentStreams must be positive, got: " + maxConcurrentStreams);
        }
        if (authType == null || authType.isBlank()) {
            throw new IllegalArgumentException("authType must not be blank");
        }
        if (maxBatchRows <= 0) {
            throw new IllegalArgumentException(
                    "maxBatchRows must be positive, got: " + maxBatchRows);
        }
    }

    /**
     * Creates a config with all defaults -- disabled by default.
     */
    public static TrainingDataServerConfig defaults() {
        return new TrainingDataServerConfig(
                false,
                DEFAULT_PORT,
                DEFAULT_MAX_CONCURRENT_STREAMS,
                DEFAULT_AUTH_TYPE,
                DEFAULT_MAX_BATCH_ROWS);
    }
}
