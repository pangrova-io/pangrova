package org.pangrova.config;

import java.util.Objects;

/**
 * Immutable typed configuration key with default value, description, and type class.
 * Inspired by Flink's ConfigOption pattern for type-safe configuration definitions.
 *
 * <p>Instances are created via the fluent builder API:
 * <pre>{@code
 * ConfigOption<Integer> port = ConfigOptions.key("server.port")
 *     .intType()
 *     .defaultValue(8080)
 *     .withDescription("HTTP server port")
 *     .build();
 * }</pre>
 *
 * @param <T> the value type of this configuration option
 */
public final class ConfigOption<T> {

    private final String key;
    private final T defaultValue;
    private final String description;
    private final Class<T> typeClass;

    ConfigOption(String key, T defaultValue, String description, Class<T> typeClass) {
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.defaultValue = defaultValue;
        this.description = Objects.requireNonNull(description, "description must not be null");
        this.typeClass = Objects.requireNonNull(typeClass, "typeClass must not be null");
    }

    /** Returns the configuration key string (e.g. "server.port"). */
    public String key() {
        return key;
    }

    /** Returns the default value, or {@code null} if no default is defined. */
    public T defaultValue() {
        return defaultValue;
    }

    /** Returns a human-readable description of this option. */
    public String description() {
        return description;
    }

    /** Returns the Java type class for this option's value. */
    public Class<T> typeClass() {
        return typeClass;
    }

    /** Returns true if this option has a non-null default value. */
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConfigOption<?> that)) {
            return false;
        }
        return key.equals(that.key)
                && Objects.equals(defaultValue, that.defaultValue)
                && description.equals(that.description)
                && typeClass.equals(that.typeClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, defaultValue, description, typeClass);
    }

    @Override
    public String toString() {
        return "ConfigOption{key='" + key + "'"
                + ", type=" + typeClass.getSimpleName()
                + ", default=" + defaultValue
                + ", description='" + description + "'}";
    }
}
