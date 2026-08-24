package org.pangrova.common.property.util;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.common.util.DataSize;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link DataSize}.
 *
 * <p>Verifies two universal properties:
 * <ol>
 *   <li>{@code megabytes(n) == n * 1024 * 1024} for any positive integer n</li>
 *   <li>{@code toHumanReadable(megabytes(n))} contains "MB" for n &gt;= 1</li>
 * </ol>
 *
 * <p><b>Validates: Requirements 4.1-4.4</b></p>
 */
class DataSizePropertyTest {

    /**
     * Property 1: megabytes(n) always equals n * 1024 * 1024 for any positive integer n.
     *
     * <p>Uses IntRange [1, 1000] to avoid long overflow while still covering a wide range.
     */
    @Property(tries = 10)
    void megabytesEqualsNTimes1024Squared(@ForAll @IntRange(min = 1, max = 1000) int n) {
        assertThat(DataSize.megabytes(n)).isEqualTo((long) n * 1024 * 1024);
    }

    /**
     * Property 2: toHumanReadable(megabytes(n)) always contains "MB" for n &gt;= 1.
     *
     * <p>For n in [1, 1023], the byte count is in the MB range (below 1 GB),
     * so the formatted output must include the "MB" suffix.
     */
    @Property(tries = 10)
    void toHumanReadableOfMegabytesContainsMB(@ForAll @IntRange(min = 1, max = 1023) int n) {
        String readable = DataSize.toHumanReadable(DataSize.megabytes(n));
        assertThat(readable).contains("MB");
    }
}
