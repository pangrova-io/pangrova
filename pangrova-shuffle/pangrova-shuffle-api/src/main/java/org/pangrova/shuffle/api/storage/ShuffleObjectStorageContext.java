package org.pangrova.shuffle.api.storage;

import java.util.Map;
import java.util.Optional;

import org.pangrova.spi.annotation.Experimental;

/**
 * Construction context handed to {@link ShuffleObjectStorageFactory#create(ShuffleObjectStorageContext)}
 * (ADR-002 D4/D6).
 *
 * <p>Carries the selected store name and the resolved {@code object-storage.*} configuration so a
 * backend can read its own settings (base directory, bucket, region, endpoint, timeouts, ...) without
 * coupling the SPI to a concrete config type. Mirrors
 * {@link org.pangrova.shuffle.api.spool.SpoolStoreContext}.
 */
@Experimental(reason = "ADR-002 D4/D6 accepted-batch storage SPI; evolving alongside storage backends")
public final class ShuffleObjectStorageContext {

    private final String storeName;
    private final Map<String, String> properties;

    /**
     * Creates an object-storage construction context.
     *
     * @param storeName configured store name
     * @param properties resolved object-storage properties
     */
    public ShuffleObjectStorageContext(String storeName, Map<String, String> properties) {
        if (storeName == null || storeName.isBlank()) {
            throw new IllegalArgumentException("storeName must not be blank");
        }
        this.storeName = storeName;
        this.properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    /** The configured store name that selected this backend. */
    public String storeName() {
        return storeName;
    }

    /** Raw {@code object-storage.*} properties (implementation-specific keys). */
    public Map<String, String> properties() {
        return properties;
    }

    /**
     * Returns a configured property when present.
     *
     * @param key property key
     * @return configured property value
     */
    public Optional<String> get(String key) {
        return Optional.ofNullable(properties.get(key));
    }

    /**
     * Returns a configured property or the supplied default.
     *
     * @param key property key
     * @param defaultValue value returned when the property is absent
     * @return configured or default value
     */
    public String getOrDefault(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }
}
