package org.pangrova.config.property;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.pangrova.config.YamlConfigLoader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link YamlConfigLoader}.
 *
 * <p>Uses only the public API. Env-dependent tests use {@code ${VAR:-default}} syntax
 * to avoid needing package-private overloads.
 *
 * <p><b>Validates: Requirements R1.1</b></p>
 */
class YamlConfigLoaderPropertyTest {

    private Path tempDir;

    @BeforeProperty
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("yaml-loader-pbt");
    }

    @AfterProperty
    void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            try (var stream = Files.walk(tempDir)) {
                stream.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                        });
            }
        }
    }

    /**
     * For any single flat key-value pair written as YAML, loading produces a map
     * containing exactly that key with the value as a string.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void flatKeyValueRoundTrips(@ForAll("flatEntries") FlatEntry entry) throws IOException {
        String yaml = entry.key + ": " + entry.value + "\n";
        Path file = writeYaml(yaml);

        Map<String, String> result = YamlConfigLoader.load(file);

        assertThat(result).containsEntry(entry.key, String.valueOf(entry.value));
    }

    /**
     * For any two-level nested YAML, loading produces a dot-separated key.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void nestedKeysFlattenWithDotSeparator(
            @ForAll("yamlSegments") String parent,
            @ForAll("yamlSegments") String child,
            @ForAll("safeValues") int value) throws IOException {

        String yaml = parent + ":\n  " + child + ": " + value + "\n";
        Path file = writeYaml(yaml);

        Map<String, String> result = YamlConfigLoader.load(file);

        assertThat(result).containsEntry(parent + "." + child, String.valueOf(value));
    }

    /**
     * Placeholder with default: when env var is absent, the default value is used.
     * Uses ${VAR:-default} syntax so no actual env var is needed.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void placeholderDefaultUsedWhenEnvAbsent(
            @ForAll("yamlSegments") String key,
            @ForAll("envVarNames") String varName,
            @ForAll("safeStringValues") String defaultVal) throws IOException {

        String yaml = key + ": ${" + varName + ":-" + defaultVal + "}\n";
        Path file = writeYaml(yaml);

        Map<String, String> result = YamlConfigLoader.load(file);

        assertThat(result).containsEntry(key, defaultVal);
    }

    /**
     * The returned map is always unmodifiable regardless of input.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void loadAlwaysReturnsUnmodifiableMap(@ForAll("flatEntries") FlatEntry entry) throws IOException {
        String yaml = entry.key + ": " + entry.value + "\n";
        Path file = writeYaml(yaml);

        Map<String, String> result = YamlConfigLoader.load(file);

        try {
            result.put("injected", "bad");
            assertThat(true).as("map should be unmodifiable").isFalse();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    /**
     * CLI YAML values override classpath defaults for the same key.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void loadWithDefaultsCliOverridesClasspath(@ForAll("safeValues") int cliValue) throws IOException {
        // test-defaults.yml has server.port=8080
        String yaml = "server:\n  port: " + cliValue + "\n";
        Path file = writeYaml(yaml);

        Map<String, String> result = YamlConfigLoader.loadWithDefaults(file, "test-defaults.yml");

        assertThat(result).containsEntry("server.port", String.valueOf(cliValue));
    }

    // --- Generators ---

    @Provide
    Arbitrary<String> yamlSegments() {
        // Filter YAML boolean literals (YES/NO/ON/OFF/TRUE/FALSE and case variants)
        // that SnakeYAML auto-converts to boolean values.
        return Arbitraries.strings()
                .ofMinLength(1)
                .ofMaxLength(20)
                .alpha()
                .withChars('_')
                .filter(s -> !isYamlBoolean(s));
    }

    private static boolean isYamlBoolean(String s) {
        String upper = s.toUpperCase();
        return upper.equals("YES") || upper.equals("NO")
                || upper.equals("ON") || upper.equals("OFF")
                || upper.equals("TRUE") || upper.equals("FALSE")
                || upper.equals("Y") || upper.equals("N");
    }

    @Provide
    Arbitrary<String> envVarNames() {
        return Arbitraries.strings()
                .ofMinLength(2)
                .ofMaxLength(20)
                .withChars('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
                        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                        'U', 'V', 'W', 'X', 'Y', 'Z', '_')
                .filter(s -> Character.isLetter(s.charAt(0)));
    }

    @Provide
    Arbitrary<String> safeStringValues() {
        return Arbitraries.strings()
                .ofMinLength(1)
                .ofMaxLength(50)
                .alpha()
                .numeric()
                .withChars('-', '_', '.');
    }

    @Provide
    Arbitrary<Integer> safeValues() {
        return Arbitraries.integers().between(1, 65535);
    }

    @Provide
    Arbitrary<FlatEntry> flatEntries() {
        Arbitrary<String> keys = yamlSegments();
        Arbitrary<Integer> values = safeValues();
        return Combinators.combine(keys, values).as(FlatEntry::new);
    }

    // --- Helper ---

    private Path writeYaml(String content) throws IOException {
        Path file = tempDir.resolve("config-" + System.nanoTime() + ".yml");
        Files.writeString(file, content);
        return file;
    }

    record FlatEntry(String key, int value) {}
}
