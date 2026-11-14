package org.pangrova.types;

import org.pangrova.types.enums.VectorElementType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link VectorType} validation, defaults, and derived methods.
 */
class VectorTypeTest {

    // ── Validation ──────────────────────────────────────────────────

    @Test
    void dimensionBelowMinThrows() {
        assertThrows(IllegalArgumentException.class, () -> new VectorType(0, VectorElementType.FLOAT));
    }

    @Test
    void dimensionAboveMaxThrows() {
        assertThrows(IllegalArgumentException.class, () -> new VectorType(65_536, VectorElementType.FLOAT));
    }

    @Test
    void negativeDimensionThrows() {
        assertThrows(IllegalArgumentException.class, () -> new VectorType(-1, VectorElementType.FLOAT));
    }

    // ── Defaults ────────────────────────────────────────────────────

    @Test
    void nullElementTypeDefaultsToFloat() {
        VectorType vt = new VectorType(128, null);
        assertEquals(VectorElementType.FLOAT, vt.elementType());
    }

    // ── Boundary values ─────────────────────────────────────────────

    @Test
    void minDimensionAccepted() {
        VectorType vt = new VectorType(1, VectorElementType.FLOAT);
        assertEquals(1, vt.dimension());
    }

    @Test
    void maxDimensionAccepted() {
        VectorType vt = new VectorType(65_535, VectorElementType.DOUBLE);
        assertEquals(65_535, vt.dimension());
    }

    // ── bytesPerVector ──────────────────────────────────────────────

    @Test
    void bytesPerVectorFloat() {
        assertEquals(768 * 4, new VectorType(768, VectorElementType.FLOAT).bytesPerVector());
    }

    @Test
    void bytesPerVectorDouble() {
        assertEquals(384 * 8, new VectorType(384, VectorElementType.DOUBLE).bytesPerVector());
    }

    // ── getDisplayName ──────────────────────────────────────────────

    @Test
    void displayNameFloatOmitsElementType() {
        assertEquals("VECTOR(128)", new VectorType(128, VectorElementType.FLOAT).getDisplayName());
    }

    @Test
    void displayNameDoubleIncludesElementType() {
        assertEquals("VECTOR(384,DOUBLE)", new VectorType(384, VectorElementType.DOUBLE).getDisplayName());
    }

    @Test
    void displayNameNullElementTypeShowsFloat() {
        assertEquals("VECTOR(256)", new VectorType(256, null).getDisplayName());
    }

    // ── getTypeSignature ────────────────────────────────────────────

    @Test
    void typeSignatureBase() {
        TypeSignature sig = new VectorType(768, VectorElementType.FLOAT).getTypeSignature();
        assertEquals("VECTOR", sig.base());
        assertEquals(2, sig.parameters().size());
        assertEquals(768L, sig.parameters().get(0).longValue());
        assertEquals("FLOAT", sig.parameters().get(1).typeSignature().base());
    }

    @Test
    void typeSignatureDouble() {
        TypeSignature sig = new VectorType(384, VectorElementType.DOUBLE).getTypeSignature();
        assertEquals("DOUBLE", sig.parameters().get(1).typeSignature().base());
    }

    // ── Record equality ─────────────────────────────────────────────

    @Test
    void equalityByValue() {
        VectorType a = new VectorType(128, VectorElementType.FLOAT);
        VectorType b = new VectorType(128, VectorElementType.FLOAT);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}
