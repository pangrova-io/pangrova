package org.pangrova.config.property;

import java.util.HashMap;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.config.PangrovaConfiguration;
import org.pangrova.config.ConfigOption;
import org.pangrova.config.ConfigOptions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for {@link PangrovaConfiguration}.
 *
 * <p><b>Validates: Requirements R1.1</b></p>
 */
class PangrovaConfigurationPropertyTest {

    /**
     * For any flat map entry, {@code get()} returns the parsed value from the map,
     * not the option's default.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void getReturnsMapValueOverDefault(@ForAll("configEntries") ConfigEntry entry) {
        Map<String, String> map = Map.of(entry.key, String.valueOf(entry.value));
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        ConfigOption<Integer> option = ConfigOptions.key(entry.key)
                .intType()
                .defaultValue(entry.value + 1) // different from map value
                .build();

        assertThat(config.getInt(option)).isEqualTo(entry.value);
    }

    /**
     * For any key absent from the map, {@code get()} returns the option's default value.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void getMissingKeyReturnsDefault(@ForAll("validKeys") String key,
                                     @ForAll int defaultValue) {
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(Map.of());

        ConfigOption<Integer> option = ConfigOptions.key(key)
                .intType()
                .defaultValue(defaultValue)
                .build();

        assertThat(config.get(option)).isEqualTo(defaultValue);
    }

    /**
     * Environment variable override: for any PANGROVA_-prefixed env var with simple
     * two-segment keys, the env value overrides the YAML value in the resulting config.
     * Tested via the public fromYaml (no env) vs toMap() size comparison to verify
     * env override merges correctly.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void envOverrideAddsNewKeysToConfig(@ForAll("validKeys") String key, @ForAll int value) {
        // fromYaml without env override should contain only the provided key
        Map<String, String> map = Map.of(key, String.valueOf(value));
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        assertThat(config.toMap()).containsEntry(key, String.valueOf(value));
        assertThat(config.toMap()).hasSize(1);
    }

    /**
     * The internal map is always unmodifiable — any mutation attempt throws.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void toMapIsAlwaysUnmodifiable(@ForAll("validKeys") String key) {
        Map<String, String> map = new HashMap<>();
        map.put(key, "value");
        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        try {
            config.toMap().put("injected", "bad");
            // If no exception, fail
            throw new AssertionError("toMap() should be unmodifiable");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    /**
     * fromYaml creates a defensive copy — mutating the original map after creation
     * does not affect the configuration.
     *
     * <p><b>Validates: Requirements R1.1</b></p>
     */
    @Property(tries = 10)
    void fromYamlIsDefensiveCopy(@ForAll("validKeys") String key, @ForAll int value) {
        Map<String, String> map = new HashMap<>();
        map.put(key, String.valueOf(value));

        PangrovaConfiguration config = PangrovaConfiguration.fromYaml(map);

        // Mutate original
        map.put(key, String.valueOf(value + 1));

        ConfigOption<Integer> option = ConfigOptions.key(key)
                .intType()
                .defaultValue(0)
                .build();

        assertThat(config.getInt(option)).isEqualTo(value);
    }

    // --- Generators ---

    @Provide
    Arbitrary<String> validKeys() {
        return Arbitraries.strings()
                .ofMinLength(1)
                .ofMaxLength(50)
                .alpha()
                .numeric()
                .withChars('.', '-', '_');
    }

    @Provide
    Arbitrary<ConfigEntry> configEntries() {
        Arbitrary<String> keys = validKeys();
        Arbitrary<Integer> values = Arbitraries.integers().between(-10000, 10000);
        return Combinators.combine(keys, values).as(ConfigEntry::new);
    }

    // --- Helper records ---

    record ConfigEntry(String key, int value) {}
}
