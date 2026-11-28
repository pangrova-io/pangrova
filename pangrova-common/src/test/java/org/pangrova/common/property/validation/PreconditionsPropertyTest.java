package org.pangrova.common.property.validation;

import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.common.validation.Preconditions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for the extended {@link Preconditions} methods.
 *
 * <p>Verifies three universal properties:
 * <ol>
 *   <li>{@code checkNotBlank} returns the original value for any non-null, non-blank string</li>
 *   <li>{@code checkPositive} does not throw for any positive number</li>
 *   <li>{@code checkPort} does not throw for any port in [1, 65535]</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 3.1-3.7</b></p>
 */
class PreconditionsPropertyTest {

    /**
     * Property 1: checkNotBlank returns the original string for any non-null, non-blank input.
     *
     * <p>Uses {@link Assume} to filter out null and blank strings from the generated input space.
     */
    @Property(tries = 10)
    void checkNotBlankReturnsOriginalForNonBlankStrings(@ForAll String value) {
        Assume.that(value != null && !value.isBlank());

        String result = Preconditions.checkNotBlank(value, "must not be blank");
        assertThat(result).isSameAs(value);
    }

    /**
     * Property 2: checkPositive does not throw for any positive number.
     *
     * <p>Uses {@code @LongRange(min = 1)} to constrain generation to strictly positive longs.
     */
    @Property(tries = 10)
    void checkPositiveDoesNotThrowForPositiveNumbers(@ForAll @LongRange(min = 1) long value) {
        Preconditions.checkPositive(value, "must be positive");
    }

    /**
     * Property 3: checkPort does not throw for any port in the valid client-facing range [1, 65535].
     *
     * <p>Uses {@code @IntRange(min = 1, max = 65535)} to cover the full valid port space.
     */
    @Property(tries = 10)
    void checkPortDoesNotThrowForValidRange(@ForAll @IntRange(min = 1, max = 65535) int port) {
        int result = Preconditions.checkPort(port, "invalid port");
        assertThat(result).isEqualTo(port);
    }
}
