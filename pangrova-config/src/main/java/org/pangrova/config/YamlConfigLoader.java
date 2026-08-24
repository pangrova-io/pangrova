package org.pangrova.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;

/**
 * Loads YAML configuration files into a flat {@code Map<String, String>} suitable
 * for consumption by {@link PangrovaConfiguration}.
 *
 * <p>Features:
 * <ul>
 *   <li>Nested YAML keys are flattened to dot-separated paths
 *       (e.g. {@code server:\n  port: 8080} → {@code server.port=8080})</li>
 *   <li>{@code ${ENV_VAR}} placeholder substitution from {@link System#getenv()}</li>
 *   <li>{@code ${VAR_NAME:-default}} syntax for fallback values</li>
 *   <li>Classpath resource loading for bundled defaults</li>
 *   <li>Merge support: CLI YAML overrides classpath defaults</li>
 * </ul>
 *
 * <p>All returned maps are unmodifiable.
 */
public final class YamlConfigLoader {

    /** Pattern matching {@code ${VAR_NAME}} or {@code ${VAR_NAME:-default}}. */
    private static final Pattern PLACEHOLDER_PATTERN =
            Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?}");

    private YamlConfigLoader() {
        // static utility
    }

    /**
     * Loads a YAML file from the filesystem, flattens nested structure to dot-separated keys,
     * and substitutes {@code ${ENV_VAR}} placeholders.
     *
     * @param yamlPath path to the YAML file
     * @return unmodifiable flat map of configuration key-value pairs
     * @throws IOException if the file cannot be read
     * @throws IllegalArgumentException if a placeholder references an undefined env var with no default
     */
    public static Map<String, String> load(Path yamlPath) throws IOException {
        Objects.requireNonNull(yamlPath, "yamlPath must not be null");
        return load(yamlPath, System.getenv());
    }

    /**
     * Loads a YAML file with an explicit environment map (for testability).
     */
    static Map<String, String> load(Path yamlPath, Map<String, String> env) throws IOException {
        Objects.requireNonNull(yamlPath, "yamlPath must not be null");
        Objects.requireNonNull(env, "env must not be null");
        try (InputStream in = Files.newInputStream(yamlPath)) {
            return parseAndFlatten(in, env);
        }
    }

    /**
     * Loads a YAML resource from the classpath, flattens nested structure,
     * and substitutes placeholders.
     *
     * @param resourceName classpath resource name (e.g. "coordinator.yml")
     * @return unmodifiable flat map of configuration key-value pairs
     * @throws IOException if the resource cannot be read or is not found
     */
    public static Map<String, String> loadFromClasspath(String resourceName) throws IOException {
        Objects.requireNonNull(resourceName, "resourceName must not be null");
        return loadFromClasspath(resourceName, System.getenv());
    }

    /**
     * Loads a YAML resource from the classpath with an explicit environment map (for testability).
     */
    static Map<String, String> loadFromClasspath(String resourceName, Map<String, String> env)
            throws IOException {
        Objects.requireNonNull(resourceName, "resourceName must not be null");
        Objects.requireNonNull(env, "env must not be null");
        try (InputStream in = YamlConfigLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IOException("Classpath resource not found: " + resourceName);
            }
            return parseAndFlatten(in, env);
        }
    }

    /**
     * Loads CLI YAML from the filesystem and merges with classpath default YAML.
     * CLI values take priority over classpath defaults.
     *
     * @param yamlPath         path to the CLI-provided YAML file
     * @param classpathDefault classpath resource name for default values
     * @return unmodifiable flat map with CLI values overriding defaults
     * @throws IOException if either source cannot be read
     */
    public static Map<String, String> loadWithDefaults(Path yamlPath, String classpathDefault)
            throws IOException {
        Objects.requireNonNull(yamlPath, "yamlPath must not be null");
        Objects.requireNonNull(classpathDefault, "classpathDefault must not be null");
        return loadWithDefaults(yamlPath, classpathDefault, System.getenv());
    }

    /**
     * Loads CLI YAML merged with classpath defaults using an explicit environment map (for testability).
     */
    static Map<String, String> loadWithDefaults(Path yamlPath, String classpathDefault,
                                                Map<String, String> env) throws IOException {
        Map<String, String> defaults = loadFromClasspath(classpathDefault, env);
        Map<String, String> overrides = load(yamlPath, env);

        Map<String, String> merged = new LinkedHashMap<>(defaults);
        merged.putAll(overrides);
        return Collections.unmodifiableMap(merged);
    }

    // --- Internal helpers ---

    private static Map<String, String> parseAndFlatten(InputStream in, Map<String, String> env) {
        Yaml yaml = new Yaml();
        Object document = yaml.load(in);
        if (document == null) {
            return Collections.unmodifiableMap(new HashMap<>());
        }
        if (!(document instanceof Map)) {
            throw new IllegalArgumentException(
                    "YAML root must be a mapping, got: " + document.getClass().getSimpleName());
        }
        Map<?, ?> root = (Map<?, ?>) document;

        Map<String, String> flat = new LinkedHashMap<>();
        flatten("", root, flat);
        resolvePlaceholders(flat, env);
        return Collections.unmodifiableMap(flat);
    }

    private static void flatten(String prefix, Map<?, ?> map, Map<String, String> result) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // SnakeYAML parses YAML boolean keywords (true/false/yes/no/on/off)
            // as Boolean even when used as map keys. String.valueOf handles this
            // safely by converting any non-String key type to its string representation.
            String rawKey = String.valueOf(entry.getKey());
            String key = prefix.isEmpty() ? rawKey : prefix + "." + rawKey;
            Object value = entry.getValue();

            if (value instanceof Map) {
                flatten(key, (Map<?, ?>) value, result);
            } else if (value != null) {
                result.put(key, String.valueOf(value));
            }
        }
    }

    private static void resolvePlaceholders(Map<String, String> flat, Map<String, String> env) {
        for (Map.Entry<String, String> entry : flat.entrySet()) {
            String resolved = substitutePlaceholders(entry.getValue(), env);
            entry.setValue(resolved);
        }
    }

    /**
     * Substitutes all {@code ${VAR_NAME}} and {@code ${VAR_NAME:-default}} placeholders
     * in the given value string.
     *
     * @throws IllegalArgumentException if a variable is not found and no default is provided
     */
    static String substitutePlaceholders(String value, Map<String, String> env) {
        if (value == null || !value.contains("${")) {
            return value;
        }
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(value);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String replacement = resolvePlaceholder(matcher.group(1), matcher.group(2), env);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String resolvePlaceholder(String varName, String defaultValue, Map<String, String> env) {
        String envValue = env.get(varName);
        if (envValue != null) {
            return envValue;
        }
        if (defaultValue != null) {
            return defaultValue;
        }
        throw new IllegalArgumentException(
                "Environment variable '" + varName + "' is not set and no default provided "
                        + "in placeholder '${" + varName + "}'");
    }
}
