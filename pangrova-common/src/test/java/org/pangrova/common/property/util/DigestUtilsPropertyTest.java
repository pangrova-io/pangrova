package org.pangrova.common.property.util;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.pangrova.common.util.DigestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link DigestUtils}.
 *
 * <p>Verifies three universal properties of SHA-256 hex output:
 * <ol>
 *   <li>Output length is always exactly 64 characters</li>
 *   <li>Output contains only lowercase hex characters [0-9a-f]</li>
 *   <li>Same input always produces the same output (determinism)</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 5.1-5.6</b></p>
 */
class DigestUtilsPropertyTest {

    /**
     * Property 1: sha256Hex output length is always 64 characters for any string input.
     *
     * <p>SHA-256 produces a 256-bit (32-byte) digest. Hex encoding doubles the byte count,
     * so the output must always be exactly 64 characters regardless of input.
     */
    @Property(tries = 10)
    void sha256HexOutputLengthIsAlways64(@ForAll String input) {
        assertThat(DigestUtils.sha256Hex(input)).hasSize(64);
    }

    /**
     * Property 2: sha256Hex output contains only lowercase hex characters [0-9a-f].
     *
     * <p>The implementation uses {@code HexFormat.of().formatHex()} which produces
     * lowercase hex. This property ensures no uppercase or non-hex characters leak through.
     */
    @Property(tries = 10)
    void sha256HexOutputContainsOnlyLowercaseHex(@ForAll String input) {
        assertThat(DigestUtils.sha256Hex(input)).matches("[0-9a-f]{64}");
    }

    /**
     * Property 3: same input produces same output (determinism).
     *
     * <p>SHA-256 is a deterministic hash function. Calling sha256Hex twice with the
     * same input must yield identical results every time.
     */
    @Property(tries = 10)
    void sha256HexIsDeterministic(@ForAll String input) {
        String first = DigestUtils.sha256Hex(input);
        String second = DigestUtils.sha256Hex(input);
        assertThat(first).isEqualTo(second);
    }
}
