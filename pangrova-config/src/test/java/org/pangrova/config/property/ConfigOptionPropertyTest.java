package org.pangrova.config.property;

import java.time.Duration;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link ConfigOption} and the fluent builder API.
 *
 * <p><b>Validates: Requirements R1.1</b></p>
 */
class ConfigOptionPropertyTest {

    /**
     * For any valid key string, the built ConfigOption preserves the key exactly.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void builtOptionPreservesKey(@ForAll("validKeys") String key) {
        ConfigOption<String> option = ConfigOptions.key(key)
                .stringType()
                .defaultValue("v")
                .withDescription("desc")
                .build();

        assertThat(option.key()).isEqualTo(key);
    }

    /**
     * For any integer default value, the built ConfigOption preserves the default exactly.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void builtOptionPreservesIntDefault(@ForAll int defaultValue) {
        ConfigOption<Integer> option = ConfigOptions.key("test.key")
                .intType()
                .defaultValue(defaultValue)
                .withDescription("test")
                .build();

        assertThat(option.defaultValue()).isEqualTo(defaultValue);
        assertThat(option.typeClass()).isEqualTo(Integer.class);
        assertThat(option.hasDefaultValue()).isTrue();
    }

    /**
     * For any long default value, the built ConfigOption preserves the default exactly.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void builtOptionPreservesLongDefault(@ForAll long defaultValue) {
        ConfigOption<Long> option = ConfigOptions.key("test.key")
                .longType()
                .defaultValue(defaultValue)
                .withDescription("test")
                .build();

        assertThat(option.defaultValue()).isEqualTo(defaultValue);
        assertThat(option.typeClass()).isEqualTo(Long.class);
    }

    /**
     * For any boolean default value, the built ConfigOption preserves the default exactly.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void builtOptionPreservesBooleanDefault(@ForAll boolean defaultValue) {
        ConfigOption<Boolean> option = ConfigOptions.key("test.key")
                .booleanType()
                .defaultValue(defaultValue)
                .withDescription("test")
                .build();

        assertThat(option.defaultValue()).isEqualTo(defaultValue);
        assertThat(option.typeClass()).isEqualTo(Boolean.class);
    }

    /**
     * For any description string, the built ConfigOption preserves the description exactly.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void builtOptionPreservesDescription(@ForAll("descriptions") String description) {
        ConfigOption<String> option = ConfigOptions.key("test.key")
                .stringType()
                .defaultValue("v")
                .withDescription(description)
                .build();

        assertThat(option.description()).isEqualTo(description);
    }

    /**
     * Two ConfigOptions built with identical parameters are equal.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void identicalBuildersProduceEqualOptions(@ForAll("validKeys") String key,
                                              @ForAll int defaultValue) {
        ConfigOption<Integer> a = ConfigOptions.key(key)
                .intType()
                .defaultValue(defaultValue)
                .withDescription("desc")
                .build();

        ConfigOption<Integer> b = ConfigOptions.key(key)
                .intType()
                .defaultValue(defaultValue)
                .withDescription("desc")
                .build();

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    /**
     * ConfigOptions with different keys are never equal.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void differentKeysProduceUnequalOptions(@ForAll("validKeys") String key1,
                                            @ForAll("validKeys") String key2) {
        if (key1.equals(key2)) return;

        ConfigOption<Integer> a = ConfigOptions.key(key1)
                .intType()
                .defaultValue(1)
                .withDescription("desc")
                .build();

        ConfigOption<Integer> b = ConfigOptions.key(key2)
                .intType()
                .defaultValue(1)
                .withDescription("desc")
                .build();

        assertThat(a).isNotEqualTo(b);
    }

    // --- Generators ---

    @Provide
    Arbitrary<String> validKeys() {
        return Arbitraries.strings()
                .ofMinLength(1)
                .ofMaxLength(100)
                .alpha()
                .numeric()
                .withChars('.', '-', '_');
    }

    @Provide
    Arbitrary<String> descriptions() {
        return Arbitraries.strings()
                .ofMinLength(0)
                .ofMaxLength(200);
    }
}
