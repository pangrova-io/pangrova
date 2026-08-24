package org.pangrova.config;

/**
 * Determines the shuffle transport strategy for intermediate data exchange.
 *
 * <p>{@link #NATIVE} uses in-process memory-based shuffle (Arrow Flight transport).
 * {@link #REMOTE} delegates to an external Remote Shuffle Service via gRPC.</p>
 */
public enum ShuffleMode {

    /** In-process shuffle via Arrow Flight transport. */
    NATIVE,

    /** External Remote Shuffle Service via gRPC. */
    REMOTE
}
