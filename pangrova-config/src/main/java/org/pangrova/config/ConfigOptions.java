package org.pangrova.config;

import java.time.Duration;
import java.util.Objects;

/**
 * Fluent entry point for creating typed {@link ConfigOption} definitions.
 * Follows the Flink-style ConfigOption builder pattern.
 *
 * <p>Usage:
 * <pre>{@code
 * ConfigOption<Integer> port = ConfigOptions.key("server.port")
 *     .intType()
 *     .defaultValue(8080)
 *     .withDescription("HTTP server port")
 *     .build();
 *
 * ConfigOption<Boolean> enabled = ConfigOptions.key("security.enabled")
 *     .booleanType()
 *     .defaultValue(false)
 *     .withDescription("Enable security module")
 *     .build();
 * }</pre>
 */
public final class ConfigOptions {

    private ConfigOptions() {
        // static factory only
    }

    /**
     * Starts building a new ConfigOption with the given key.
     *
     * @param key the configuration key (e.g. "server.port")
     * @return a key builder for selecting the value type
     */
    public static KeyBuilder key(String key) {
        return new KeyBuilder(Objects.requireNonNull(key, "key must not be null"));
    }

    /**
     * Intermediate builder that selects the value type for a configuration option.
     */
    public static final class KeyBuilder {

        private final String key;

        KeyBuilder(String key) {
            this.key = key;
        }

        /** Creates a builder for an {@code int} configuration option. */
        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(key, Integer.class);
        }

        /** Creates a builder for a {@code long} configuration option. */
        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(key, Long.class);
        }

        /** Creates a builder for a {@code boolean} configuration option. */
        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(key, Boolean.class);
        }

        /** Creates a builder for a {@code String} configuration option. */
        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(key, String.class);
        }

        /** Creates a builder for a {@code double} configuration option. */
        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(key, Double.class);
        }

        /** Creates a builder for a {@link Duration} configuration option. */
        public TypedConfigOptionBuilder<Duration> durationType() {
            return new TypedConfigOptionBuilder<>(key, Duration.class);
        }

        /**
         * Creates a builder for an enum configuration option.
         *
         * @param enumClass the enum class
         * @param <E>       the enum type
         * @return a typed builder for the enum option
         */
        public <E extends Enum<E>> TypedConfigOptionBuilder<E> enumType(Class<E> enumClass) {
            return new TypedConfigOptionBuilder<>(key, enumClass);
        }
    }
}
