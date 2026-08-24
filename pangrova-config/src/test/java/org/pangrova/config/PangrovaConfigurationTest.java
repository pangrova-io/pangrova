package org.pangrova.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PangrovaConfiguration}.
 */
class PangrovaConfigurationTest {

    // --- Factory: fromYaml ---

    @Test
    void fromYamlCreatesConfigFromFlatMap() {
        Map<String, String> map = Map.of("server.port", "9090", "server.host", "0.0.0.0");
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        ConfigOption<Integer> port = ConfigOptions.key("server.port").intType().defaultValue(8080).build();
        assertThat(config.getInt(port)).isEqualTo(9090);
    }

    @Test
    void fromYamlRejectsNullMap() {
        assertThatThrownBy(() -> PangrovaConfiguration.fromYaml(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void fromYamlProducesUnmodifiableMap() {
        Map<String, String> map = new HashMap<>();
        map.put("k", "v");
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        assertThatThrownBy(() -> config.toMap().put("new", "val"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void fromYamlIsDefensiveCopy() {
        Map<String, String> map = new HashMap<>();
        map.put("server.port", "9090");
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        map.put("server.port", "1111");
        ConfigOption<Integer> port = ConfigOptions.key("server.port").intType().defaultValue(8080).build();
        assertThat(config.getInt(port)).isEqualTo(9090);
    }

    // --- Factory: fromYamlWithEnvOverride ---

    @Test
    void envOverrideAppliesPangrovaPrefixedVars() {
        Map<String, String> yaml = Map.of("server.port", "8080");
        Map<String, String> env = Map.of("PANGROVA_SERVER_PORT", "9999");

        PangrovaConfiguration config = PangrovaConfiguration.fromYamlWithEnvOverride(yaml, env);
        ConfigOption<Integer> port = ConfigOptions.key("server.port").intType().defaultValue(8080).build();
        assertThat(config.getInt(port)).isEqualTo(9999);
    }

    @Test
    void envOverrideIgnoresNonPangrovaVars() {
        Map<String, String> yaml = Map.of("server.port", "8080");
        Map<String, String> env = Map.of("OTHER_SERVER_PORT", "9999");

        PangrovaConfiguration config = PangrovaConfiguration.fromYamlWithEnvOverride(yaml, env);
        ConfigOption<Integer> port = ConfigOptions.key("server.port").intType().defaultValue(8080).build();
        assertThat(config.getInt(port)).isEqualTo(8080);
    }

    @Test
    void envOverrideDoubleUnderscoreBecomesLiteralUnderscore() {
        Map<String, String> yaml = Map.of();
        Map<String, String> env = Map.of("PANGROVA_DB__CONNECTION_URL", "jdbc:h2:mem");

        PangrovaConfiguration config = PangrovaConfiguration.fromYamlWithEnvOverride(yaml, env);
        ConfigOption<String> url = ConfigOptions.key("db_connection.url").stringType().build();
        assertThat(config.getString(url)).isEqualTo("jdbc:h2:mem");
    }

    @Test
    void envOverrideSingleUnderscoreBecomesDot() {
        Map<String, String> yaml = Map.of();
        Map<String, String> env = Map.of("PANGROVA_HEARTBEAT_INTERVAL", "5000");

        PangrovaConfiguration config = PangrovaConfiguration.fromYamlWithEnvOverride(yaml, env);
        ConfigOption<String> interval = ConfigOptions.key("heartbeat.interval").stringType().build();
        assertThat(config.getString(interval)).isEqualTo("5000");
    }

    @Test
    void envOverrideConvertsToLowercase() {
        Map<String, String> yaml = Map.of();
        Map<String, String> env = Map.of("PANGROVA_SECURITY_ENABLED", "true");

        PangrovaConfiguration config = PangrovaConfiguration.fromYamlWithEnvOverride(yaml, env);
        ConfigOption<Boolean> enabled = ConfigOptions.key("security.enabled").booleanType().defaultValue(false).build();
        assertThat(config.getBoolean(enabled)).isTrue();
    }

    @Test
    void envOverrideIgnoresBarePangrovaPrefix() {
        Map<String, String> yaml = Map.of("k", "v");
        Map<String, String> env = Map.of("PANGROVA_", "ignored");

        PangrovaConfiguration config = PangrovaConfiguration.fromYamlWithEnvOverride(yaml, env);
        assertThat(config.toMap()).containsOnlyKeys("k");
    }

    // --- envKeyToConfigKey ---

    @Test
    void envKeyToConfigKeyBasicConversion() {
        assertThat(PangrovaConfiguration.envKeyToConfigKey("PANGROVA_SERVER_PORT")).isEqualTo("server.port");
    }

    @Test
    void envKeyToConfigKeyDoubleUnderscore() {
        assertThat(PangrovaConfiguration.envKeyToConfigKey("PANGROVA_DB__CONNECTION_URL")).isEqualTo("db_connection.url");
    }

    @Test
    void envKeyToConfigKeyAllLowercase() {
        assertThat(PangrovaConfiguration.envKeyToConfigKey("PANGROVA_MY_KEY")).isEqualTo("my.key");
    }

    // --- Typed getters ---

    @Test
    void getStringReturnsValue() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("host", "example.com"));
        ConfigOption<String> opt = ConfigOptions.key("host").stringType().defaultValue("localhost").build();
        assertThat(config.getString(opt)).isEqualTo("example.com");
    }

    @Test
    void getIntReturnsValue() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("port", "9090"));
        ConfigOption<Integer> opt = ConfigOptions.key("port").intType().defaultValue(8080).build();
        assertThat(config.getInt(opt)).isEqualTo(9090);
    }

    @Test
    void getLongReturnsValue() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("budget", "1073741824"));
        ConfigOption<Long> opt = ConfigOptions.key("budget").longType().defaultValue(0L).build();
        assertThat(config.getLong(opt)).isEqualTo(1073741824L);
    }

    @Test
    void getBooleanReturnsValue() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("enabled", "true"));
        ConfigOption<Boolean> opt = ConfigOptions.key("enabled").booleanType().defaultValue(false).build();
        assertThat(config.getBoolean(opt)).isTrue();
    }

    @Test
    void getDoubleReturnsValue() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("ratio", "0.75"));
        ConfigOption<Double> opt = ConfigOptions.key("ratio").doubleType().defaultValue(0.5).build();
        assertThat(config.getDouble(opt)).isEqualTo(0.75);
    }

    @Test
    void getDurationReturnsValueWithSuffix() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "30s"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().defaultValue(Duration.ofSeconds(10)).build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void getDurationReturnsValueWithIsoFormat() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "PT5M"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().defaultValue(Duration.ofSeconds(10)).build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void getDurationMillisecondsSuffix() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "500ms"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void getDurationMinutesSuffix() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "5m"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void getDurationHoursSuffix() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "2h"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void getDurationDaysSuffix() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "1d"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofDays(1));
    }

    @Test
    void getDurationPlainNumberTreatedAsMilliseconds() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("timeout", "1000"));
        ConfigOption<Duration> opt = ConfigOptions.key("timeout").durationType().build();
        assertThat(config.getDuration(opt)).isEqualTo(Duration.ofMillis(1000));
    }

    // --- Default fallback ---

    @Test
    void getReturnDefaultWhenKeyMissing() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        ConfigOption<Integer> opt = ConfigOptions.key("missing").intType().defaultValue(42).build();
        assertThat(config.get(opt)).isEqualTo(42);
    }

    @Test
    void getReturnsNullWhenKeyMissingAndNoDefault() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        ConfigOption<String> opt = ConfigOptions.key("missing").stringType().build();
        assertThat(config.get(opt)).isNull();
    }

    @Test
    void getIntThrowsWhenNoValueAndNoDefault() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        ConfigOption<Integer> opt = ConfigOptions.key("missing").intType().build();
        assertThatThrownBy(() -> config.getInt(opt))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("missing");
    }

    // --- getOptional ---

    @Test
    void getOptionalReturnsPresentWhenValueExists() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("k", "v"));
        ConfigOption<String> opt = ConfigOptions.key("k").stringType().build();
        assertThat(config.getOptional(opt)).isPresent().contains("v");
    }

    @Test
    void getOptionalReturnsEmptyWhenNoValueAndNoDefault() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        ConfigOption<String> opt = ConfigOptions.key("missing").stringType().build();
        assertThat(config.getOptional(opt)).isEmpty();
    }

    @Test
    void getOptionalReturnsPresentFromDefault() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        ConfigOption<Integer> opt = ConfigOptions.key("missing").intType().defaultValue(42).build();
        assertThat(config.getOptional(opt)).isPresent().contains(42);
    }

    // --- Immutability ---

    @Test
    void configurationIsImmutable() {
        Map<String, String> map = new HashMap<>();
        map.put("a", "1");
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        // Mutating original map does not affect config
        map.put("b", "2");
        assertThat(config.toMap()).doesNotContainKey("b");

        // toMap() returns unmodifiable view
        assertThatThrownBy(() -> config.toMap().put("c", "3"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // --- toString ---

    @Test
    void toStringContainsEntryCount() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of("a", "1", "b", "2"));
        assertThat(config.toString()).contains("2");
    }

    // --- get rejects null option ---

    @Test
    void getRejectsNullOption() {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());
        assertThatThrownBy(() -> config.get(null))
                .isInstanceOf(NullPointerException.class);
    }
}
