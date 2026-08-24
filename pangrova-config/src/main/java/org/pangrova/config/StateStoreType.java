package org.pangrova.config;

/**
 * Backend type for state store SPI implementations.
 *
 * <p>Drives backend selection via the global {@code state-store.type} config key.
 * Each SPI store (QueryStateJournal, TenantStore, QualityRuleStore, etc.)
 * resolves to an InMemory or JDBC implementation based on this enum.
 *
 * @see StateStoreFactory
 * @see CoordinatorOptions#STATE_STORE_TYPE
 */
public enum StateStoreType {

    /** In-memory implementation — zero external dependencies, suitable for dev/test. */
    IN_MEMORY,

    /** JDBC-backed implementation — persistent, suitable for production (PostgreSQL). */
    JDBC
}
