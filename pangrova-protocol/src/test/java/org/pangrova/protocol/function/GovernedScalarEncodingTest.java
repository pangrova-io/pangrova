package org.pangrova.protocol.function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GovernedScalarEncodingTest {
    @ParameterizedTest
    @ValueSource(strings = {"0", "-0", "1", "-1", "1.0", "0.000", "1234567890.0987654321",
            "1e0", "1E+3", "1e-3", "-0.0E+10", "1.7976931348623157e308", "4.9e-324", "1e-324"})
    void decimalGrammarRetainsTheExactJavaFloatingBits(String value) {
        var decoded = (GovernedScalarValue.Floating) GovernedScalarEncoding.decode(value, "EXACT_FLOAT");
        assertThat(Double.doubleToRawLongBits(decoded.value()))
                .isEqualTo(Double.doubleToRawLongBits(Double.parseDouble(value)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-", "+1", ".1", "-.1", "1.", "00", "01", "-01", "01.0",
            "1e", "1E", "1e+", "1e-", "1e1e1", "1.0suffix", "1f", "1D", " 1", "1 ",
            "1\n", "1\t", "NaN", "Infinity", "-Infinity", "0x1.0p0", "1_000", "\u0661", "1\u0661"})
    void noncanonicalSyntaxIsRejectedBeforeFloatingConversion(String value) {
        assertThatThrownBy(() -> GovernedScalarEncoding.decode(value, "EXACT_FLOAT"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid decimal floating binding");
    }

    @ParameterizedTest
    @ValueSource(strings = {"1e309", "-1e309", "1.7976931348623159e308"})
    void syntacticallyValidNonfiniteValuesStillFailClosed(String value) {
        assertThatThrownBy(() -> GovernedScalarEncoding.decode(value, "EXACT_FLOAT"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Binding floating value must be finite");
    }

    @Test
    void longValidAndInvalidMantissasUseTheSameBoundedStackGrammar() {
        String fraction = "0." + "0".repeat(65536) + "1";
        assertThat(GovernedScalarEncoding.decode(fraction, "EXACT_FLOAT"))
                .isEqualTo(new GovernedScalarValue.Floating(0.0));
        String invalid = "1" + "0".repeat(65536) + "suffix";
        assertThatThrownBy(() -> GovernedScalarEncoding.decode(invalid, "EXACT_FLOAT"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid decimal floating binding");
    }

    @Test
    void absentBindingRemainsAnExplicitError() {
        assertThatThrownBy(() -> GovernedScalarEncoding.decode(null, "EXACT_FLOAT"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Binding value must not be NULL");
    }
}
