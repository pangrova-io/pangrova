package org.pangrova.testing.integration.e2e;

import java.util.Objects;

/**
 * Fault injection framework for chaos testing.
 *
 * <p>Provides a simple API to inject and remove faults (worker crash,
 * network partition, disk full, etc.) via a pluggable {@link FaultInjector}.
 */
public final class ChaosTestFramework {

    /** Types of faults that can be injected during chaos tests. */
    public enum FaultType {
        WORKER_CRASH,
        COORDINATOR_CRASH,
        NETWORK_PARTITION,
        DISK_FULL,
        MEMORY_PRESSURE
    }

    /** Functional interface for injecting or removing a fault. */
    @FunctionalInterface
    public interface FaultInjector {
        /**
         * Inject or remove the specified fault type.
         *
         * @param type the fault type to inject or remove
         */
        void inject(FaultType type);
    }

    private ChaosTestFramework() {}

    /**
     * Inject a fault of the given type.
     *
     * @param type     the fault type; must not be null
     * @param injector the injector callback; must not be null
     */
    public static void injectFault(FaultType type, FaultInjector injector) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(injector, "injector must not be null");
        injector.inject(type);
    }

    /**
     * Remove a previously injected fault.
     *
     * @param type     the fault type; must not be null
     * @param injector the injector callback; must not be null
     */
    public static void removeFault(FaultType type, FaultInjector injector) {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(injector, "injector must not be null");
        injector.inject(type);
    }
}
