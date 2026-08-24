package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link InteropMode}.
 */
class InteropModeTest {

    @Test
    void enumValuesExist() {
        assertThat(InteropMode.values()).containsExactly(
                InteropMode.LEGACY_JNI_PULL,
                InteropMode.NATIVE_C_STREAM);
    }

    @ParameterizedTest
    @CsvSource({
            "legacy-jni-pull, LEGACY_JNI_PULL",
            "LEGACY_JNI_PULL, LEGACY_JNI_PULL",
            "Legacy-Jni-Pull, LEGACY_JNI_PULL",
            "LEGACY-JNI-PULL, LEGACY_JNI_PULL",
            "native-c-stream, NATIVE_C_STREAM",
            "NATIVE_C_STREAM, NATIVE_C_STREAM",
            "Native-C-Stream, NATIVE_C_STREAM",
            "NATIVE-C-STREAM, NATIVE_C_STREAM",
    })
    void fromConfigValueAcceptsValidInputs(String input, InteropMode expected) {
        assertThat(InteropMode.fromConfigValue(input)).isEqualTo(expected);
    }

    @Test
    void fromConfigValueTrimsWhitespace() {
        assertThat(InteropMode.fromConfigValue("  legacy-jni-pull  "))
                .isEqualTo(InteropMode.LEGACY_JNI_PULL);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unknown", "jni", "arrow", "legacy", "stream"})
    void fromConfigValueRejectsInvalidValues(String invalid) {
        assertThatThrownBy(() -> InteropMode.fromConfigValue(invalid))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown interop mode")
                .hasMessageContaining(invalid);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void fromConfigValueRejectsNullBlankEmpty(String invalid) {
        assertThatThrownBy(() -> InteropMode.fromConfigValue(invalid))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toConfigValueProducesKebabCase() {
        assertThat(InteropMode.LEGACY_JNI_PULL.toConfigValue())
                .isEqualTo("legacy-jni-pull");
        assertThat(InteropMode.NATIVE_C_STREAM.toConfigValue())
                .isEqualTo("native-c-stream");
    }

    @Test
    void roundTripConfigValue() {
        for (InteropMode mode : InteropMode.values()) {
            assertThat(InteropMode.fromConfigValue(mode.toConfigValue()))
                    .isEqualTo(mode);
        }
    }
}
