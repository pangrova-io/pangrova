package org.pangrova.shuffle;

import java.util.List;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Handle returned by {@link ShuffleLifecycleManager#registerShuffle} identifying
 * a registered shuffle and the remote shuffle server endpoints holding its partitions.
 *
 * @param shuffleId the unique shuffle identifier assigned by the remote shuffle server
 * @param endpoints the remote shuffle server endpoints holding partition data
 */
public record ShuffleHandle(String shuffleId, List<ShuffleServerEndpoint> endpoints) {

    /** Sentinel handle for local (in-process) shuffle — no external remote shuffle involved. */
    public static final ShuffleHandle LOCAL = new ShuffleHandle("local", List.of());

    public ShuffleHandle {
        checkNotNull(shuffleId, "shuffleId must not be null");
        checkNotNull(endpoints, "endpoints must not be null");
        endpoints = List.copyOf(endpoints);
    }
}
