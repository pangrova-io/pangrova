package org.pangrova.config;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Immutable configuration holder wrapping a flat {@code Map<String, String>} of config values.
 * Provides typed access via {@link ConfigOption} definitions with fallback to default values.
 *
 * <p>Two factory methods are provided:
 * <ul>
 *   <li>{@link #fromYaml(Map)} — creates config from a pre-loaded flat map (no env override)</li>
 *   <li>{@link #fromYamlWithEnvOverride(Map)} — creates config with {@code PANGROVA_*} environment
 *       variable overrides applied on top of the flat map</li>
 * </ul>
 *
 * <p>Environment variable override logic:
 * <ol>
 *   <li>Select variables starting with {@code PANGROVA_}</li>
 *   <li>Strip the {@code PANGROVA_} prefix</li>
 *   <li>Replace {@code __} (double underscore) with a literal {@code _}</li>
 *   <li>Replace remaining {@code _} with {@code .}</li>
 *   <li>Lowercase the result</li>
 * </ol>
 * Example: {@code PANGROVA_SERVER_PORT} → {@code server.port},
 *          {@code PANGROVA_DB__CONNECTION_URL} → {@code db_connection.url}
 */
public final class PangrovaConfiguration {

    private static final String ENV_PREFIX = "PANGROVA_";
    // Sentinel for double-underscore preservation during conversion
    private static final String DOUBLE_UNDERSCORE_SENTINEL = "\u0000DU\u0000";

    private final Map<String, String> values;

    private PangrovaConfiguration(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(new HashMap<>(values));
    }

    /**
     * Creates an {@link PangrovaConfiguration} from a pre-loaded flat map.
     * No environment variable overrides are applied.
     *
     * @param flatMap flat key-value pairs (e.g. from YAML loading)
     * @return immutable configuration instance
     */
    public static PangrovaConfiguration fromYaml(Map<String, String> flatMap) {
        Objects.requireNonNull(flatMap, "flatMap must not be null");
        return new PangrovaConfiguration(flatMap);
    }

    /**
     * Creates an {@link PangrovaConfiguration} from a pre-loaded flat map with
     * {@code PANGROVA_*} environment variable overrides applied on top.
     *
     * @param flatMap flat key-value pairs (e.g. from YAML loading)
     * @return immutable configuration instance with env overrides
     */
    public static PangrovaConfiguration fromYamlWithEnvOverride(Map<String, String> flatMap) {
        Objects.requireNonNull(flatMap, "flatMap must not be null");
        return fromYamlWithEnvOverride(flatMap, System.getenv());
    }

    /**
     * Creates an {@link PangrovaConfiguration} from a pre-loaded flat map with
     * environment variable overrides applied. This overload accepts an explicit
     * environment map for testability.
     *
     * @param flatMap flat key-value pairs (e.g. from YAML loading)
     * @param env     environment variables (typically {@code System.getenv()})
     * @return immutable configuration instance with env overrides
     */
    static PangrovaConfiguration fromYamlWithEnvOverride(Map<String, String> flatMap,
                                                       Map<String, String> env) {
        Objects.requireNonNull(flatMap, "flatMap must not be null");
        Objects.requireNonNull(env, "env must not be null");

        Map<String, String> merged = new HashMap<>(flatMap);
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String envKey = entry.getKey();
            if (envKey.startsWith(ENV_PREFIX) && envKey.length() > ENV_PREFIX.length()) {
                String configKey = envKeyToConfigKey(envKey);
                merged.put(configKey, entry.getValue());
            }
        }
        return new PangrovaConfiguration(merged);
    }

    /**
     * Returns the typed value for the given option, falling back to the option's default value
     * if no explicit value is present in the configuration map.
     *
     * @param option the configuration option
     * @param <T>    the value type
     * @return the resolved value, or the default value (which may be {@code null})
     */
    public <T> T get(ConfigOption<T> option) {
        Objects.requireNonNull(option, "option must not be null");
        String raw = values.get(option.key());
        if (raw == null) {
            return option.defaultValue();
        }
        return parseValue(raw, option.typeClass());
    }

    /**
     * Returns an {@link Optional} containing the typed value for the given option,
     * or empty if neither the map nor the default provides a value.
     *
     * @param option the configuration option
     * @param <T>    the value type
     * @return optional typed value
     */
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return Optional.ofNullable(get(option));
    }

    /** Typed getter for {@link String} options. */
    public String getString(ConfigOption<String> option) {
        return get(option);
    }

    /** Typed getter for {@link Integer} options. */
    public int getInt(ConfigOption<Integer> option) {
        Integer value = get(option);
        if (value == null) {
            throw new IllegalStateException(
                    "No value for config key '" + option.key() + "' and no default defined");
        }
        return value;
    }

    /** Typed getter for {@link Long} options. */
    public long getLong(ConfigOption<Long> option) {
        Long value = get(option);
        if (value == null) {
            throw new IllegalStateException(
                    "No value for config key '" + option.key() + "' and no default defined");
        }
        return value;
    }

    /** Typed getter for {@link Boolean} options. */
    public boolean getBoolean(ConfigOption<Boolean> option) {
        Boolean value = get(option);
        if (value == null) {
            throw new IllegalStateException(
                    "No value for config key '" + option.key() + "' and no default defined");
        }
        return value;
    }

    /** Typed getter for {@link Double} options. */
    public double getDouble(ConfigOption<Double> option) {
        Double value = get(option);
        if (value == null) {
            throw new IllegalStateException(
                    "No value for config key '" + option.key() + "' and no default defined");
        }
        return value;
    }

    /** Typed getter for {@link Duration} options. */
    public Duration getDuration(ConfigOption<Duration> option) {
        return get(option);
    }

    /**
     * Returns an unmodifiable view of the underlying flat configuration map.
     *
     * @return unmodifiable map of all config key-value pairs
     */
    public Map<String, String> toMap() {
        return values;
    }

    // --- Internal helpers ---

    /**
     * Converts an environment variable name to a config key.
     * PANGROVA_SERVER_PORT → server.port
     * PANGROVA_DB__CONNECTION_URL → db_connection.url
     */
    static String envKeyToConfigKey(String envKey) {
        String stripped = envKey.substring(ENV_PREFIX.length());
        // Preserve double underscores as literal underscores
        String result = stripped.replace("__", DOUBLE_UNDERSCORE_SENTINEL);
        // Single underscores become dots
        result = result.replace('_', '.');
        // Restore double underscores as single underscores
        result = result.replace(DOUBLE_UNDERSCORE_SENTINEL, "_");
        return result.toLowerCase();
    }

    private static final Map<Class<?>, java.util.function.Function<String, ?>> PARSERS = Map.of(
            String.class, s -> s,
            Integer.class, Integer::valueOf,
            Long.class, Long::valueOf,
            Boolean.class, Boolean::valueOf,
            Double.class, Double::valueOf,
            Duration.class, PangrovaConfiguration::parseDuration
    );

    @SuppressWarnings("unchecked")
    private static <T> T parseValue(String raw, Class<T> typeClass) {
        var parser = PARSERS.get(typeClass);
        if (parser != null) {
            return (T) parser.apply(raw);
        }
        if (typeClass.isEnum()) {
            @SuppressWarnings("rawtypes")
            Class<? extends Enum> enumClass = (Class<? extends Enum>) typeClass;
            return (T) Enum.valueOf(enumClass, raw.trim().toUpperCase());
        }
        throw new IllegalArgumentException("Unsupported config type: " + typeClass.getName());
    }

    // Ordered longest-suffix-first to prevent "m" matching before "ms"
    private static final List<Map.Entry<String, Function<Long, Duration>>> DURATION_SUFFIXES =
            java.util.List.of(
                    java.util.Map.entry("ms", Duration::ofMillis),
                    java.util.Map.entry("s",  Duration::ofSeconds),
                    java.util.Map.entry("m",  Duration::ofMinutes),
                    java.util.Map.entry("h",  Duration::ofHours),
                    java.util.Map.entry("d",  Duration::ofDays));

    private static Duration parseDuration(String raw) {
        String trimmed = raw.trim();
        if (trimmed.toUpperCase().startsWith("PT") || trimmed.toUpperCase().startsWith("-PT")) {
            return Duration.parse(trimmed.toUpperCase());
        }
        for (var entry : DURATION_SUFFIXES) {
            if (trimmed.endsWith(entry.getKey())) {
                long value = Long.parseLong(trimmed.substring(0, trimmed.length() - entry.getKey().length()).trim());
                return entry.getValue().apply(value);
            }
        }
        return Duration.ofMillis(Long.parseLong(trimmed));
    }

    @Override
    public String toString() {
        return "PangrovaConfiguration{entries=" + values.size() + "}";
    }
}
