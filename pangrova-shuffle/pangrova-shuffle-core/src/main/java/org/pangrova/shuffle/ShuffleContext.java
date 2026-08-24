package org.pangrova.shuffle;

import java.util.Map;
import java.util.Optional;

/**
 * Construction context handed to a {@link ShuffleManagerFactory#create(ShuffleContext)} (ADR-002 D1).
 *
 * <p>Carries the selected manager name and the resolved {@code shuffle.*} configuration, so a provider
 * can read its own settings (spill dirs, object-store bucket, min-object-bytes, retry-policy, ...)
 * without coupling the SPI to a concrete config type.
 */
public final class ShuffleContext {

    private final String managerName;
    private final Map<String, String> properties;

    /**
     * Creates a shuffle-manager construction context.
     *
     * @param managerName configured manager name
     * @param properties resolved shuffle properties
     */
    public ShuffleContext(String managerName, Map<String, String> properties) {
        if (managerName == null || managerName.isBlank()) {
            throw new IllegalArgumentException("managerName must not be blank");
        }
        this.managerName = managerName;
        this.properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    /** The configured {@code shuffle.manager} name that selected this provider. */
    public String managerName() {
        return managerName;
    }

    /** Raw {@code shuffle.*} properties (implementation-specific keys). */
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
