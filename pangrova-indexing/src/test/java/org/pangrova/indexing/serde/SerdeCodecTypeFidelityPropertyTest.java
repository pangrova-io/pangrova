package org.pangrova.indexing.serde;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link SerdeCodec} type fidelity across round-trip serialization.
 *
 * <p><b>Validates: Requirements 13.1, 13.2, 13.3, 13.4</b></p>
 *
 * <p>Core properties: BigDecimal, BigInteger, and UUID values survive writeComparable/readComparable
 * round-trip with exact equality, correct type preservation, and comparison ordering.</p>
 */
class SerdeCodecTypeFidelityPropertyTest {

    private Comparable<?> roundTrip(Comparable<?> value) {
        byte[] bytes = SerdeCodec.toBytes(out -> SerdeCodec.writeComparable(out, value));
        return SerdeCodec.fromBytes(bytes, SerdeCodec::readComparable);
    }

    // ---------------------------------------------------------------
    // Property 1: BigDecimal round-trip preserves value and scale
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void bigDecimalRoundTripPreservesValueAndScale(@ForAll BigDecimal original) {
        Comparable<?> result = roundTrip(original);

        assertThat(result)
                .as("BigDecimal round-trip must return equal value")
                .isEqualTo(original);
        assertThat(result).isInstanceOf(BigDecimal.class);
        assertThat(((BigDecimal) result).scale())
                .as("BigDecimal round-trip must preserve scale")
                .isEqualTo(original.scale());
    }

    // ---------------------------------------------------------------
    // Property 2: BigInteger round-trip preserves value
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void bigIntegerRoundTripPreservesValue(@ForAll BigInteger original) {
        Comparable<?> result = roundTrip(original);

        assertThat(result)
                .as("BigInteger round-trip must return equal value")
                .isEqualTo(original);
        assertThat(result).isInstanceOf(BigInteger.class);
    }

    // ---------------------------------------------------------------
    // Property 3: UUID round-trip preserves value
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void uuidRoundTripPreservesValue(@ForAll("uuids") UUID original) {
        Comparable<?> result = roundTrip(original);

        assertThat(result)
                .as("UUID round-trip must return equal value")
                .isEqualTo(original);
        assertThat(result).isInstanceOf(UUID.class);
    }

    // ---------------------------------------------------------------
    // Property 4: Type preservation — each type comes back as itself
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void bigDecimalTypePreserved(@ForAll BigDecimal original) {
        assertThat(roundTrip(original)).isInstanceOf(BigDecimal.class);
    }

    @Property(tries = 10)
    void bigIntegerTypePreserved(@ForAll BigInteger original) {
        assertThat(roundTrip(original)).isInstanceOf(BigInteger.class);
    }

    @Property(tries = 10)
    void uuidTypePreserved(@ForAll("uuids") UUID original) {
        assertThat(roundTrip(original)).isInstanceOf(UUID.class);
    }

    // ---------------------------------------------------------------
    // Property 5: BigDecimal comparison ordering preserved
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void bigDecimalComparisonOrderPreserved(@ForAll BigDecimal a, @ForAll BigDecimal b) {
        int originalCmp = a.compareTo(b);
        BigDecimal aResult = (BigDecimal) roundTrip(a);
        BigDecimal bResult = (BigDecimal) roundTrip(b);
        int roundTripCmp = aResult.compareTo(bResult);

        assertThat(Integer.signum(roundTripCmp))
                .as("BigDecimal comparison order must be preserved: %s vs %s", a, b)
                .isEqualTo(Integer.signum(originalCmp));
    }

    // ---------------------------------------------------------------
    // Property 6: Existing types still round-trip correctly (regression)
    // ---------------------------------------------------------------

    @Property(tries = 10)
    void integerRoundTrip(@ForAll int original) {
        Comparable<?> result = roundTrip(original);
        assertThat(result).isEqualTo(original);
        assertThat(result).isInstanceOf(Integer.class);
    }

    @Property(tries = 10)
    void longRoundTrip(@ForAll long original) {
        Comparable<?> result = roundTrip(original);
        assertThat(result).isEqualTo(original);
        assertThat(result).isInstanceOf(Long.class);
    }

    @Property(tries = 10)
    void doubleRoundTrip(@ForAll double original) {
        Comparable<?> result = roundTrip(original);
        assertThat(result).isEqualTo(original);
        assertThat(result).isInstanceOf(Double.class);
    }

    @Property(tries = 10)
    void stringRoundTrip(@ForAll String original) {
        Comparable<?> result = roundTrip(original);
        assertThat(result).isEqualTo(original);
        assertThat(result).isInstanceOf(String.class);
    }

    // ---------------------------------------------------------------
    // Property 7: Null round-trip
    // ---------------------------------------------------------------

    @Property(tries = 1)
    void nullRoundTrip() {
        assertThat(roundTrip(null)).isNull();
    }

    // ---------------------------------------------------------------
    // Arbitraries
    // ---------------------------------------------------------------

    @Provide
    Arbitrary<UUID> uuids() {
        return Arbitraries.longs().tuple2().map(t -> new UUID(t.get1(), t.get2()));
    }
}
