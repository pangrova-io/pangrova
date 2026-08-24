package org.pangrova.shuffle.spool;

import java.util.Map;
import java.util.Optional;

import org.pangrova.spi.annotation.Experimental;

/**
 * Construction context handed to {@link SpoolStoreFactory#create(SpoolStoreContext)} (ADR-002 D6).
 *
 * <p>Carries the selected store name and the resolved {@code spool.*} configuration so a backend can
 * read its own settings (base directory, bucket, min-object-bytes, ...) without coupling the SPI to a
 * concrete config type. Mirrors {@link org.pangrova.shuffle.ShuffleContext}.
 */
@Experimental(reason = "ADR-002 D6 persistent-plane spool SPI; evolving alongside D4 providers")
public final class SpoolStoreContext {

    private final String storeName;
    private final Map<String, String> properties;

    /**
     * Creates a spool-store construction context.
     *
     * @param storeName configured store name
     * @param properties resolved spool properties
     */
    public SpoolStoreContext(String storeName, Map<String, String> properties) {
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

    /** Raw {@code spool.*} properties (implementation-specific keys). */
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
