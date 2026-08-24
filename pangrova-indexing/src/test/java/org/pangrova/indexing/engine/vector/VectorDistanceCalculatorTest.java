package org.pangrova.indexing.engine.vector;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for {@link VectorDistanceCalculator}.
 *
 * <p>Validates: Requirements 2.2, 2.3, 2.4, 2.5, 2.6, 2.8
 */
class VectorDistanceCalculatorTest {

    // --- l2Distance ---

    @Test
    void l2Distance_identicalVectors_returnsZero() {
        float[] v = {1.0f, 2.0f, 3.0f};
        assertThat(VectorDistanceCalculator.l2Distance(v, v)).isEqualTo(0.0);
    }

    @Test
    void l2Distance_knownValues() {
        float[] a = {1.0f, 0.0f};
        float[] b = {0.0f, 1.0f};
        // sqrt((1-0)^2 + (0-1)^2) = sqrt(2)
        assertThat(VectorDistanceCalculator.l2Distance(a, b)).isCloseTo(Math.sqrt(2.0), within(1e-9));
    }

    @Test
    void l2Distance_symmetry() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 5.0f, 6.0f};
        assertThat(VectorDistanceCalculator.l2Distance(a, b))
                .isEqualTo(VectorDistanceCalculator.l2Distance(b, a));
    }

    @Test
    void l2Distance_nonNegative() {
        float[] a = {-3.0f, 4.0f};
        float[] b = {1.0f, -2.0f};
        assertThat(VectorDistanceCalculator.l2Distance(a, b)).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void l2Distance_zeroVectors() {
        float[] zero = {0.0f, 0.0f, 0.0f};
        assertThat(VectorDistanceCalculator.l2Distance(zero, zero)).isEqualTo(0.0);
    }

    @Test
    void l2Distance_singleDimension() {
        float[] a = {3.0f};
        float[] b = {7.0f};
        assertThat(VectorDistanceCalculator.l2Distance(a, b)).isCloseTo(4.0, within(1e-9));
    }

    // --- cosineDistance ---

    @Test
    void cosineDistance_identicalDirection_returnsZero() {
        float[] v = {1.0f, 2.0f, 3.0f};
        assertThat(VectorDistanceCalculator.cosineDistance(v, v)).isCloseTo(0.0, within(1e-9));
    }

    @Test
    void cosineDistance_oppositeDirection_returnsTwo() {
        float[] a = {1.0f, 0.0f};
        float[] b = {-1.0f, 0.0f};
        assertThat(VectorDistanceCalculator.cosineDistance(a, b)).isCloseTo(2.0, within(1e-9));
    }

    @Test
    void cosineDistance_orthogonal_returnsOne() {
        float[] a = {1.0f, 0.0f};
        float[] b = {0.0f, 1.0f};
        assertThat(VectorDistanceCalculator.cosineDistance(a, b)).isCloseTo(1.0, within(1e-9));
    }

    @Test
    void cosineDistance_zeroVector_returnsOne() {
        float[] zero = {0.0f, 0.0f, 0.0f};
        float[] v = {1.0f, 2.0f, 3.0f};
        assertThat(VectorDistanceCalculator.cosineDistance(zero, v)).isEqualTo(1.0);
        assertThat(VectorDistanceCalculator.cosineDistance(v, zero)).isEqualTo(1.0);
    }

    @Test
    void cosineDistance_bothZeroVectors_returnsOne() {
        float[] zero = {0.0f, 0.0f};
        assertThat(VectorDistanceCalculator.cosineDistance(zero, zero)).isEqualTo(1.0);
    }

    @Test
    void cosineDistance_rangeZeroToTwo() {
        float[] a = {0.5f, -0.3f, 0.8f};
        float[] b = {-0.2f, 0.7f, 0.1f};
        double dist = VectorDistanceCalculator.cosineDistance(a, b);
        assertThat(dist).isBetween(0.0, 2.0);
    }

    @Test
    void cosineDistance_scaledVectors_sameDistance() {
        float[] a = {1.0f, 2.0f};
        float[] b = {3.0f, 4.0f};
        float[] aScaled = {2.0f, 4.0f};
        assertThat(VectorDistanceCalculator.cosineDistance(aScaled, b))
                .isCloseTo(VectorDistanceCalculator.cosineDistance(a, b), within(1e-6));
    }

    // --- innerProduct ---

    @Test
    void innerProduct_knownValues() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 5.0f, 6.0f};
        // dot = 1*4 + 2*5 + 3*6 = 32, innerProduct = -32
        assertThat(VectorDistanceCalculator.innerProduct(a, b)).isCloseTo(-32.0, within(1e-9));
    }

    @Test
    void innerProduct_zeroVector() {
        float[] zero = {0.0f, 0.0f};
        float[] v = {1.0f, 2.0f};
        assertThat(VectorDistanceCalculator.innerProduct(zero, v)).isCloseTo(0.0, within(1e-9));
    }

    @Test
    void innerProduct_negativesDot() {
        float[] a = {2.0f, 3.0f};
        float[] b = {4.0f, 5.0f};
        double dot = 2.0 * 4.0 + 3.0 * 5.0;
        assertThat(VectorDistanceCalculator.innerProduct(a, b)).isCloseTo(-dot, within(1e-9));
    }

    // --- dimension mismatch ---

    @Test
    void l2Distance_dimensionMismatch_throws() {
        float[] a = {1.0f, 2.0f};
        float[] b = {1.0f, 2.0f, 3.0f};
        assertThatThrownBy(() -> VectorDistanceCalculator.l2Distance(a, b))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dimension mismatch");
    }

    @Test
    void cosineDistance_dimensionMismatch_throws() {
        float[] a = {1.0f};
        float[] b = {1.0f, 2.0f};
        assertThatThrownBy(() -> VectorDistanceCalculator.cosineDistance(a, b))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dimension mismatch");
    }

    @Test
    void innerProduct_dimensionMismatch_throws() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {1.0f};
        assertThatThrownBy(() -> VectorDistanceCalculator.innerProduct(a, b))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dimension mismatch");
    }

    // --- utility class contract ---

    @Test
    void constructor_isPrivate() throws Exception {
        Constructor<VectorDistanceCalculator> ctor =
                VectorDistanceCalculator.class.getDeclaredConstructor();
        assertThat(Modifier.isPrivate(ctor.getModifiers())).isTrue();
    }

    @Test
    void class_isFinal() {
        assertThat(Modifier.isFinal(VectorDistanceCalculator.class.getModifiers())).isTrue();
    }
}
