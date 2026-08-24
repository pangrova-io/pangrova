package org.pangrova.spi.index.vector;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link QuantizationType}.
 */
class QuantizationTypeTest {

    @Test
    void hasFourValues() {
        assertEquals(4, QuantizationType.values().length);
    }

    @Test
    void valueOfRoundTrips() {
        for (QuantizationType qt : QuantizationType.values()) {
            assertEquals(qt, QuantizationType.valueOf(qt.name()));
        }
    }

    @Test
    void enumConstants() {
        assertNotNull(QuantizationType.FP32);
        assertNotNull(QuantizationType.FP16);
        assertNotNull(QuantizationType.INT8);
        assertNotNull(QuantizationType.BINARY);
    }

    // ── Stable IDs ─────────────────────────────────────────────────

    @Test
    void stableIds() {
        assertEquals(1, QuantizationType.FP32.getStableId());
        assertEquals(2, QuantizationType.FP16.getStableId());
        assertEquals(3, QuantizationType.INT8.getStableId());
        assertEquals(4, QuantizationType.BINARY.getStableId());
    }

    @Test
    void stableIdsAreUnique() {
        QuantizationType[] values = QuantizationType.values();
        for (int i = 0; i < values.length; i++) {
            for (int j = i + 1; j < values.length; j++) {
                assertNotNull(values[i]);
                assertNotNull(values[j]);
                if (values[i].getStableId() == values[j].getStableId()) {
                    throw new AssertionError("Duplicate stableId " + values[i].getStableId()
                            + " for " + values[i] + " and " + values[j]);
                }
            }
        }
    }

    @Test
    void fromStableIdRoundTrips() {
        for (QuantizationType qt : QuantizationType.values()) {
            assertSame(qt, QuantizationType.fromStableId(qt.getStableId()));
        }
    }

    @Test
    void fromStableIdRejectsUnknown() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> QuantizationType.fromStableId(999));
        assertEquals("Unknown QuantizationType stableId: 999", ex.getMessage());
    }

    @Test
    void fromStableIdRejectsZero() {
        assertThrows(IllegalArgumentException.class,
                () -> QuantizationType.fromStableId(0));
    }

    @Test
    void fromStableIdRejectsNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> QuantizationType.fromStableId(-1));
    }

    // ── Bytes/bits per dimension ───────────────────────────────────

    @Test
    void bytesPerDimension() {
        assertEquals(4, QuantizationType.FP32.bytesPerDimension());
        assertEquals(2, QuantizationType.FP16.bytesPerDimension());
        assertEquals(1, QuantizationType.INT8.bytesPerDimension());
        assertEquals(0, QuantizationType.BINARY.bytesPerDimension());
    }

    @Test
    void bitsPerDimension() {
        assertEquals(32, QuantizationType.FP32.bitsPerDimension());
        assertEquals(16, QuantizationType.FP16.bitsPerDimension());
        assertEquals(8, QuantizationType.INT8.bitsPerDimension());
        assertEquals(1, QuantizationType.BINARY.bitsPerDimension());
    }
}
