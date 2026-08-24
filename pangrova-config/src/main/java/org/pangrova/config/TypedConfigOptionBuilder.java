package org.pangrova.config;

import java.util.Objects;

/**
 * Intermediate builder for type-safe construction of {@link ConfigOption} instances.
 * Obtained via {@link ConfigOptions.KeyBuilder#intType()}, {@link ConfigOptions.KeyBuilder#stringType()}, etc.
 *
 * <p>Usage:
 * <pre>{@code
 * ConfigOption<Integer> port = ConfigOptions.key("server.port")
 *     .intType()
 *     .defaultValue(8080)
 *     .withDescription("HTTP server port")
 *     .build();
 * }</pre>
 *
 * @param <T> the value type of the configuration option being built
 */
public final class TypedConfigOptionBuilder<T> {

    private final String key;
    private final Class<T> typeClass;
    private T defaultValue;
    private String description = "";

    TypedConfigOptionBuilder(String key, Class<T> typeClass) {
        this.key = Objects.requireNonNull(key, "key must not be null");
        this.typeClass = Objects.requireNonNull(typeClass, "typeClass must not be null");
    }

    /**
     * Sets the default value for this configuration option.
     *
     * @param value the default value (may be null to indicate no default)
     * @return this builder for chaining
     */
    public TypedConfigOptionBuilder<T> defaultValue(T value) {
        this.defaultValue = value;
        return this;
    }

    /**
     * Sets a human-readable description for this configuration option.
     *
     * @param description the description text
     * @return this builder for chaining
     */
    public TypedConfigOptionBuilder<T> withDescription(String description) {
        this.description = Objects.requireNonNull(description, "description must not be null");
        return this;
    }

    /**
     * Builds an immutable {@link ConfigOption} from the current builder state.
     *
     * @return a new ConfigOption instance
     */
    public ConfigOption<T> build() {
        return new ConfigOption<>(key, defaultValue, description, typeClass);
    }
}
