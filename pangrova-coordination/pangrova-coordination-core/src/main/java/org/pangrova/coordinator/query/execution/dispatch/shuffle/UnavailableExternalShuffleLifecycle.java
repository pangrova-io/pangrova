package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.api.ShuffleCommitResult;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleLifecycleManager;
import org.pangrova.shuffle.api.ShuffleRegistration;

import java.util.Objects;

/**
 * Explicit absence of an external lifecycle in Direct execution. This owns no data or resources.
 * Direct Native task/output owners perform their own cleanup. Calling the external registration
 * or publication path without an installed service is a configuration error, never a local commit.
 */
public final class UnavailableExternalShuffleLifecycle implements ShuffleLifecycleManager {
    public static final UnavailableExternalShuffleLifecycle INSTANCE = new UnavailableExternalShuffleLifecycle();

    private UnavailableExternalShuffleLifecycle() {}

    @Override
    public ShuffleHandle registerShuffle(ShuffleRegistration registration) {
        Objects.requireNonNull(registration, "registration");
        throw new IllegalStateException("External shuffle lifecycle is not configured");
    }

    @Override
    public ShuffleCommitResult commitShuffle(String queryId, int stageId) {
        requireQuery(queryId);
        if (stageId < 0) throw new IllegalArgumentException("stageId must be non-negative");
        throw new IllegalStateException("External shuffle lifecycle is not configured");
    }

    @Override
    public void cleanupShuffle(String queryId) {
        requireQuery(queryId);
        // No external allocation can be acquired through this binding.
    }

    private static void requireQuery(String queryId) {
        if (queryId == null || queryId.isBlank()) throw new IllegalArgumentException("queryId must not be blank");
    }
}
