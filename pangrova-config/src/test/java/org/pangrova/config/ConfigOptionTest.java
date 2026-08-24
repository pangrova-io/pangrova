package org.pangrova.config;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ConfigOption}, {@link ConfigOptions}, and {@link TypedConfigOptionBuilder}.
 */
class ConfigOptionTest {

    @Test
    void intOptionViaFluentBuilder() {
        ConfigOption<Integer> option = ConfigOptions.key("server.port")
                .intType()
                .defaultValue(8080)
                .withDescription("HTTP server port")
                .build();

        assertThat(option.key()).isEqualTo("server.port");
        assertThat(option.defaultValue()).isEqualTo(8080);
        assertThat(option.description()).isEqualTo("HTTP server port");
        assertThat(option.typeClass()).isEqualTo(Integer.class);
        assertThat(option.hasDefaultValue()).isTrue();
    }

    @Test
    void stringOptionViaFluentBuilder() {
        ConfigOption<String> option = ConfigOptions.key("server.host")
                .stringType()
                .defaultValue("localhost")
                .withDescription("Server bind host")
                .build();

        assertThat(option.key()).isEqualTo("server.host");
        assertThat(option.defaultValue()).isEqualTo("localhost");
        assertThat(option.typeClass()).isEqualTo(String.class);
    }

    @Test
    void booleanOptionViaFluentBuilder() {
        ConfigOption<Boolean> option = ConfigOptions.key("security.enabled")
                .booleanType()
                .defaultValue(false)
                .withDescription("Enable security module")
                .build();

        assertThat(option.key()).isEqualTo("security.enabled");
        assertThat(option.defaultValue()).isFalse();
        assertThat(option.typeClass()).isEqualTo(Boolean.class);
    }

    @Test
    void longOptionViaFluentBuilder() {
        ConfigOption<Long> option = ConfigOptions.key("memory.budget")
                .longType()
                .defaultValue(1073741824L)
                .withDescription("Total memory budget in bytes")
                .build();

        assertThat(option.defaultValue()).isEqualTo(1073741824L);
        assertThat(option.typeClass()).isEqualTo(Long.class);
    }

    @Test
    void doubleOptionViaFluentBuilder() {
        ConfigOption<Double> option = ConfigOptions.key("threshold.ratio")
                .doubleType()
                .defaultValue(0.8)
                .withDescription("Memory pressure threshold")
                .build();

        assertThat(option.defaultValue()).isEqualTo(0.8);
        assertThat(option.typeClass()).isEqualTo(Double.class);
    }

    @Test
    void durationOptionViaFluentBuilder() {
        ConfigOption<Duration> option = ConfigOptions.key("heartbeat.interval")
                .durationType()
                .defaultValue(Duration.ofSeconds(30))
                .withDescription("Heartbeat interval")
                .build();

        assertThat(option.defaultValue()).isEqualTo(Duration.ofSeconds(30));
        assertThat(option.typeClass()).isEqualTo(Duration.class);
    }

    @Test
    void nullDefaultValueAllowed() {
        ConfigOption<String> option = ConfigOptions.key("optional.key")
                .stringType()
                .withDescription("An optional config")
                .build();

        assertThat(option.defaultValue()).isNull();
        assertThat(option.hasDefaultValue()).isFalse();
    }

    @Test
    void emptyDescriptionByDefault() {
        ConfigOption<Integer> option = ConfigOptions.key("some.key")
                .intType()
                .defaultValue(1)
                .build();

        assertThat(option.description()).isEmpty();
    }

    @Test
    void nullKeyThrowsNullPointerException() {
        assertThatThrownBy(() -> ConfigOptions.key(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void nullDescriptionThrowsNullPointerException() {
        assertThatThrownBy(() -> ConfigOptions.key("k")
                .intType()
                .withDescription(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void equalityAndHashCode() {
        ConfigOption<Integer> a = ConfigOptions.key("server.port")
                .intType()
                .defaultValue(8080)
                .withDescription("port")
                .build();

        ConfigOption<Integer> b = ConfigOptions.key("server.port")
                .intType()
                .defaultValue(8080)
                .withDescription("port")
                .build();

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void inequalityOnDifferentKey() {
        ConfigOption<Integer> a = ConfigOptions.key("a").intType().defaultValue(1).build();
        ConfigOption<Integer> b = ConfigOptions.key("b").intType().defaultValue(1).build();

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void inequalityOnDifferentDefault() {
        ConfigOption<Integer> a = ConfigOptions.key("k").intType().defaultValue(1).build();
        ConfigOption<Integer> b = ConfigOptions.key("k").intType().defaultValue(2).build();

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void toStringContainsKeyAndType() {
        ConfigOption<Integer> option = ConfigOptions.key("server.port")
                .intType()
                .defaultValue(8080)
                .withDescription("port")
                .build();

        String str = option.toString();
        assertThat(str).contains("server.port");
        assertThat(str).contains("Integer");
        assertThat(str).contains("8080");
    }
}
