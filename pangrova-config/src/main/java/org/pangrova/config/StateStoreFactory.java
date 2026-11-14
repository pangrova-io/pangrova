package org.pangrova.config;

import org.pangrova.config.enums.StateStoreType;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified factory for creating state store SPI implementations based on {@link StateStoreType}.
 *
 * <p>Since {@code pangrova-config} cannot depend on SPI or implementation modules,
 * this factory uses a registration-based approach: each module registers its
 * InMemory and JDBC creators at bootstrap time via {@link #register}.
 * The {@link #create} method then dispatches to the correct creator based on the
 * configured {@link StateStoreType}.
 *
 * <p>Usage:
 * <pre>{@code
 * // At bootstrap (e.g. in CoordinatorBootstrap):
 * StateStoreFactory.register(WorkerRegistry.class,
 *         StateStoreType.IN_MEMORY, ds -> new InMemoryWorkerRegistry());
 * StateStoreFactory.register(WorkerRegistry.class,
 *         StateStoreType.JDBC, ds -> new JdbcWorkerRegistry(ds));
 *
 * // At creation time:
 * WorkerRegistry registry = StateStoreFactory.create(
 *         WorkerRegistry.class, StateStoreType.JDBC, dataSource);
 * }</pre>
 *
 * @see StateStoreType
 * @see CoordinatorOptions#STATE_STORE_TYPE
 */
public final class StateStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StateStoreFactory.class);

    /**
     * Registry key: (SPI class, store type) → creator function.
     * The creator receives a nullable {@link DataSource} and returns the SPI implementation.
     */
    private static final Map<RegistryKey<?>, Function<DataSource, ?>> REGISTRY =
            new ConcurrentHashMap<>();

    private StateStoreFactory() {
        // static utility
    }

    /**
     * Registers a creator function for a given SPI type and store type.
     *
     * @param spiType   the SPI interface class (e.g. {@code WorkerRegistry.class})
     * @param storeType the backend type this creator handles
     * @param creator   function that accepts a nullable {@link DataSource} and returns the implementation
     * @param <T>       the SPI type
     */
    public static <T> void register(Class<T> spiType, StateStoreType storeType,
                                    Function<DataSource, T> creator) {
        Objects.requireNonNull(spiType, "spiType must not be null");
        Objects.requireNonNull(storeType, "storeType must not be null");
        Objects.requireNonNull(creator, "creator must not be null");

        var key = new RegistryKey<>(spiType, storeType);
        Function<DataSource, ?> previous = REGISTRY.putIfAbsent(key, creator);
        if (previous != null) {
            if (previous == creator) {
                // Idempotent re-registration with the same creator — safe to ignore.
                LOG.debug("event=state_store_register_idempotent spiType={} storeType={}",
                        spiType.getSimpleName(), storeType);
                return;
            }
            LOG.error("event=state_store_register_conflict spiType={} storeType={}",
                    spiType.getSimpleName(), storeType);
            throw new IllegalStateException(
                    "Conflicting StateStore registration for type: " + spiType.getSimpleName()
                            + " storeType=" + storeType
                            + " (a different creator is already registered)");
        } else {
            LOG.info("event=state_store_registered spiType={} storeType={}",
                    spiType.getSimpleName(), storeType);
        }
    }

    /**
     * Creates a state store implementation for the given SPI type and backend type.
     *
     * @param spiType    the SPI interface class
     * @param storeType  the backend type (from config)
     * @param dataSource JDBC data source (required for {@link StateStoreType#JDBC}, may be null for IN_MEMORY)
     * @param <T>        the SPI type
     * @return the SPI implementation instance
     * @throws IllegalArgumentException if no creator is registered for the given combination
     * @throws IllegalStateException    if JDBC is requested but dataSource is null
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> spiType, StateStoreType storeType,
                               DataSource dataSource) {
        Objects.requireNonNull(spiType, "spiType must not be null");
        Objects.requireNonNull(storeType, "storeType must not be null");

        if (storeType == StateStoreType.JDBC && dataSource == null) {
            throw new IllegalStateException(
                    "DataSource is required for JDBC state store: " + spiType.getSimpleName());
        }

        var key = new RegistryKey<>(spiType, storeType);
        Function<DataSource, ?> creator = REGISTRY.get(key);
        if (creator == null) {
            throw new IllegalArgumentException(
                    "No state store registered for spiType=" + spiType.getSimpleName()
                            + " storeType=" + storeType);
        }

        T instance = (T) creator.apply(dataSource);
        LOG.info("event=state_store_created spiType={} storeType={} implClass={}",
                spiType.getSimpleName(), storeType, instance.getClass().getSimpleName());
        return instance;
    }

    /**
     * Clears all registered creators. Intended for test isolation only.
     */
    static void resetForTesting() {
        REGISTRY.clear();
    }

    /** Returns the number of registered creators. */
    @org.pangrova.common.VisibleForTesting
    static int registeredCount() {
        return REGISTRY.size();
    }

    /**
     * Composite key for the registry: (SPI class, store type).
     */
    private record RegistryKey<T>(Class<T> spiType, StateStoreType storeType) {
        RegistryKey {
            Objects.requireNonNull(spiType, "spiType must not be null");
            Objects.requireNonNull(storeType, "storeType must not be null");
        }
    }
}
