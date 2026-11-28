package org.pangrova.common.validation;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreconditionsTest {

    @Test
    void fieldChecksPreserveValuesAndReportArgumentErrorsWithFieldNames() {
        String text = "  label  ";
        assertThat(Preconditions.requireText(text, "name")).isSameAs(text);
        assertThat(Preconditions.requireNonNullField(text, "name")).isSameAs(text);
        assertThatThrownBy(() -> Preconditions.requireText("\u2003", "name"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("name must not be blank");
        assertThatThrownBy(() -> Preconditions.requireText(null, "name"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("name must not be blank");
        assertThatThrownBy(() -> Preconditions.requireNonNullField(null, "name"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("name must not be null");
    }

    @Test
    void optionalTextAndDurationsRetainTheirDistinctEmptyValueRules() {
        Preconditions.requireOptionalText(null, "description");
        Preconditions.requireOptionalText(" available ", "description");
        assertThatThrownBy(() -> Preconditions.requireOptionalText(" ", "description"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("description must be null or non-blank");

        Duration nanos = Duration.ofNanos(1);
        assertThat(Preconditions.requirePositiveDuration(nanos, "timeout")).isSameAs(nanos);
        for (Duration invalid : java.util.Arrays.asList(null, Duration.ZERO, nanos.negated())) {
            assertThatThrownBy(() -> Preconditions.requirePositiveDuration(invalid, "timeout"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessage("timeout must be positive");
        }
    }

    @Test
    void checkNotNull_withNonNull_returnsReference() {
        String value = "hello";
        assertThat(Preconditions.checkNotNull(value, "should not fail")).isSameAs(value);
    }

    @Test
    void checkNotNull_withNull_throwsNullPointerException() {
        assertThatThrownBy(() -> Preconditions.checkNotNull(null, "was null"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("was null");
    }

    @Test
    void checkArgument_withTrue_passes() {
        Preconditions.checkArgument(true, "should not fail");
    }

    @Test
    void checkArgument_withFalse_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkArgument(false, "bad arg"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad arg");
    }

    @Test
    void checkState_withTrue_passes() {
        Preconditions.checkState(true, "should not fail");
    }

    @Test
    void checkState_withFalse_throwsIllegalStateException() {
        assertThatThrownBy(() -> Preconditions.checkState(false, "bad state"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("bad state");
    }

    @Test
    void cannotInstantiate() throws Exception {
        var ctor = Preconditions.class.getDeclaredConstructor();
        assertThat(ctor.canAccess(null)).isFalse();
    }

    // --- checkNotBlank ---

    @Test
    void checkNotBlank_withValidString_returnsSameString() {
        String value = "hello";
        assertThat(Preconditions.checkNotBlank(value, "fail")).isSameAs(value);
    }

    @Test
    void checkNotBlank_withNull_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkNotBlank(null, "was blank"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("was blank");
    }

    @Test
    void checkNotBlank_withEmptyString_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkNotBlank("", "was blank"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("was blank");
    }

    @Test
    void checkNotBlank_withBlankSpaces_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkNotBlank("   ", "was blank"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("was blank");
    }

    // --- checkPositive ---

    @Test
    void checkPositive_withPositiveValue_passes() {
        Preconditions.checkPositive(1, "should not fail");
        Preconditions.checkPositive(Long.MAX_VALUE, "should not fail");
    }

    @Test
    void checkPositive_withZero_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkPositive(0, "must be positive"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must be positive");
    }

    @Test
    void checkPositive_withNegative_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkPositive(-1, "must be positive"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must be positive");
    }

    // --- checkNonNegative ---

    @Test
    void checkNonNegative_withZero_passes() {
        Preconditions.checkNonNegative(0, "should not fail");
    }

    @Test
    void checkNonNegative_withPositive_passes() {
        Preconditions.checkNonNegative(42, "should not fail");
    }

    @Test
    void checkNonNegative_withNegative_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkNonNegative(-1, "must not be negative"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("must not be negative");
    }

    // --- checkNotEmpty ---

    @Test
    void checkNotEmpty_withNonEmptyCollection_returnsSameCollection() {
        Collection<String> list = List.of("a", "b");
        assertThat(Preconditions.checkNotEmpty(list, "fail")).isSameAs(list);
    }

    @Test
    void checkNotEmpty_withNull_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkNotEmpty(null, "was empty"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("was empty");
    }

    @Test
    void checkNotEmpty_withEmptyCollection_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkNotEmpty(Collections.emptyList(), "was empty"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("was empty");
    }

    // --- checkPort ---

    @Test
    void checkPort_withValidPort1_returnsPort() {
        assertThat(Preconditions.checkPort(1, "bad port")).isEqualTo(1);
    }

    @Test
    void checkPort_withValidPort65535_returnsPort() {
        assertThat(Preconditions.checkPort(65535, "bad port")).isEqualTo(65535);
    }

    @Test
    void checkPort_withZero_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkPort(0, "bad port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad port");
    }

    @Test
    void checkPort_with65536_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkPort(65536, "bad port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad port");
    }

    @Test
    void checkPort_withNegative_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkPort(-1, "bad port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad port");
    }

    // --- checkBindPort ---

    @Test
    void checkBindPort_withZero_returnsPort() {
        assertThat(Preconditions.checkBindPort(0, "bad port")).isEqualTo(0);
    }

    @Test
    void checkBindPort_withValidPort65535_returnsPort() {
        assertThat(Preconditions.checkBindPort(65535, "bad port")).isEqualTo(65535);
    }

    @Test
    void checkBindPort_withNegative_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkBindPort(-1, "bad port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad port");
    }

    @Test
    void checkBindPort_with65536_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> Preconditions.checkBindPort(65536, "bad port"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad port");
    }
}
