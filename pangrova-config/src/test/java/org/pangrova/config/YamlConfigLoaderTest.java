package org.pangrova.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link YamlConfigLoader}.
 */
class YamlConfigLoaderTest {

    @TempDir
    Path tempDir;

    // --- load(Path) ---

    @Test
    void loadFlattensNestedYaml() throws IOException {
        Path yaml = writeYaml("server:\n  port: 9090\n  host: 0.0.0.0\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result)
                .containsEntry("server.port", "9090")
                .containsEntry("server.host", "0.0.0.0");
    }

    @Test
    void loadPreservesFlatKeys() throws IOException {
        Path yaml = writeYaml("server.port: 7070\nserver.host: 127.0.0.1\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result)
                .containsEntry("server.port", "7070")
                .containsEntry("server.host", "127.0.0.1");
    }

    @Test
    void loadFlattensDeeplyNestedYaml() throws IOException {
        Path yaml = writeYaml("database:\n  pool:\n    size: 10\n    timeout: 30s\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result)
                .containsEntry("database.pool.size", "10")
                .containsEntry("database.pool.timeout", "30s");
    }

    @Test
    void loadReturnsEmptyMapForEmptyYaml() throws IOException {
        Path yaml = writeYaml("# empty\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result).isEmpty();
    }

    @Test
    void loadReturnsUnmodifiableMap() throws IOException {
        Path yaml = writeYaml("key: value\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThatThrownBy(() -> result.put("new", "val"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void loadRejectsNullPath() {
        assertThatThrownBy(() -> YamlConfigLoader.load(null, Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void loadThrowsForNonExistentFile() {
        Path missing = tempDir.resolve("missing.yml");
        assertThatThrownBy(() -> YamlConfigLoader.load(missing, Map.of()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void loadConvertsNonStringValuesToString() throws IOException {
        Path yaml = writeYaml("port: 8080\nenabled: true\nratio: 0.75\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result)
                .containsEntry("port", "8080")
                .containsEntry("enabled", "true")
                .containsEntry("ratio", "0.75");
    }

    @Test
    void loadSkipsNullValues() throws IOException {
        Path yaml = writeYaml("present: value\nabsent:\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result).containsEntry("present", "value");
        assertThat(result).doesNotContainKey("absent");
    }

    // --- Placeholder substitution ---

    @Test
    void loadSubstitutesEnvPlaceholder() throws IOException {
        Path yaml = writeYaml("db.password: ${DB_PASSWORD}\n");
        Map<String, String> env = Map.of("DB_PASSWORD", "secret123");
        Map<String, String> result = YamlConfigLoader.load(yaml, env);

        assertThat(result).containsEntry("db.password", "secret123");
    }

    @Test
    void loadSubstitutesPlaceholderWithDefault() throws IOException {
        Path yaml = writeYaml("db.url: ${DB_URL:-jdbc:h2:mem:fallback}\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result).containsEntry("db.url", "jdbc:h2:mem:fallback");
    }

    @Test
    void loadEnvValueOverridesPlaceholderDefault() throws IOException {
        Path yaml = writeYaml("db.url: ${DB_URL:-jdbc:h2:mem:fallback}\n");
        Map<String, String> env = Map.of("DB_URL", "jdbc:postgresql://prod:5432/db");
        Map<String, String> result = YamlConfigLoader.load(yaml, env);

        assertThat(result).containsEntry("db.url", "jdbc:postgresql://prod:5432/db");
    }

    @Test
    void loadThrowsForUndefinedPlaceholderWithoutDefault() throws IOException {
        Path yaml = writeYaml("db.password: ${MISSING_VAR}\n");

        assertThatThrownBy(() -> YamlConfigLoader.load(yaml, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MISSING_VAR");
    }

    @Test
    void loadSubstitutesMultiplePlaceholdersInSameValue() throws IOException {
        Path yaml = writeYaml("db.url: jdbc://${DB_HOST:-localhost}:${DB_PORT:-5432}/mydb\n");
        Map<String, String> env = Map.of("DB_HOST", "prod-server");
        Map<String, String> result = YamlConfigLoader.load(yaml, env);

        assertThat(result).containsEntry("db.url", "jdbc://prod-server:5432/mydb");
    }

    @Test
    void loadLeavesValuesWithoutPlaceholdersUnchanged() throws IOException {
        Path yaml = writeYaml("plain: no-placeholders-here\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result).containsEntry("plain", "no-placeholders-here");
    }

    // --- loadFromClasspath ---

    @Test
    void loadFromClasspathLoadsResource() throws IOException {
        Map<String, String> result = YamlConfigLoader.loadFromClasspath("test-config.yml", Map.of());

        assertThat(result)
                .containsEntry("server.port", "9090")
                .containsEntry("server.host", "0.0.0.0")
                .containsEntry("database.url", "jdbc:h2:mem:test")
                .containsEntry("database.pool.size", "10")
                .containsEntry("security.enabled", "false");
    }

    @Test
    void loadFromClasspathThrowsForMissingResource() {
        assertThatThrownBy(() -> YamlConfigLoader.loadFromClasspath("nonexistent.yml", Map.of()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("nonexistent.yml");
    }

    @Test
    void loadFromClasspathRejectsNullResourceName() {
        assertThatThrownBy(() -> YamlConfigLoader.loadFromClasspath(null, Map.of()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void loadFromClasspathReturnsUnmodifiableMap() throws IOException {
        Map<String, String> result = YamlConfigLoader.loadFromClasspath("test-config.yml", Map.of());

        assertThatThrownBy(() -> result.put("new", "val"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- loadWithDefaults ---

    @Test
    void loadWithDefaultsMergesCliOverClasspath() throws IOException {
        Path cliYaml = writeYaml("server:\n  port: 9090\ndatabase:\n  pool:\n    size: 20\n");
        Map<String, String> result = YamlConfigLoader.loadWithDefaults(
                cliYaml, "test-defaults.yml", Map.of());

        // CLI overrides
        assertThat(result).containsEntry("server.port", "9090");
        assertThat(result).containsEntry("database.pool.size", "20");
        // Defaults preserved
        assertThat(result).containsEntry("server.host", "localhost");
        assertThat(result).containsEntry("logging.level", "INFO");
        assertThat(result).containsEntry("database.url", "jdbc:h2:mem:default");
    }

    @Test
    void loadWithDefaultsCliWinsOverDefault() throws IOException {
        Path cliYaml = writeYaml("server:\n  host: 0.0.0.0\n");
        Map<String, String> result = YamlConfigLoader.loadWithDefaults(
                cliYaml, "test-defaults.yml", Map.of());

        assertThat(result).containsEntry("server.host", "0.0.0.0");
    }

    @Test
    void loadWithDefaultsReturnsUnmodifiableMap() throws IOException {
        Path cliYaml = writeYaml("key: value\n");
        Map<String, String> result = YamlConfigLoader.loadWithDefaults(
                cliYaml, "test-defaults.yml", Map.of());

        assertThatThrownBy(() -> result.put("new", "val"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- substitutePlaceholders (package-private) ---

    @Test
    void substitutePlaceholdersReturnsNullForNull() {
        assertThat(YamlConfigLoader.substitutePlaceholders(null, Map.of())).isNull();
    }

    @Test
    void substitutePlaceholdersNoOpForPlainString() {
        assertThat(YamlConfigLoader.substitutePlaceholders("plain", Map.of())).isEqualTo("plain");
    }

    @Test
    void substitutePlaceholdersEmptyDefault() throws IOException {
        Path yaml = writeYaml("key: ${MISSING:-}\n");
        Map<String, String> result = YamlConfigLoader.load(yaml, Map.of());

        assertThat(result).containsEntry("key", "");
    }

    // --- Helper ---

    private Path writeYaml(String content) throws IOException {
        Path file = tempDir.resolve("config.yml");
        Files.writeString(file, content);
        return file;
    }
}
