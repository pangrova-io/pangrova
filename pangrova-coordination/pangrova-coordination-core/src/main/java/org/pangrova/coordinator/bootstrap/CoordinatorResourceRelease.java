package org.pangrova.coordinator.bootstrap;

import java.util.function.Consumer;

/** Releases every owned resource even if startup or an earlier release failed. */
final class CoordinatorResourceRelease {
    private CoordinatorResourceRelease() {}

    static <T> Runnable present(T resource, Consumer<? super T> release) {
        return () -> {
            if (resource != null) release.accept(resource);
        };
    }

    static void close(Throwable primary, Runnable... releases) {
        Throwable failure = primary;
        for (Runnable release : releases) {
            try {
                release.run();
            } catch (RuntimeException | Error cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else if (failure != cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
        }
        if (primary != null) return;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }
}
