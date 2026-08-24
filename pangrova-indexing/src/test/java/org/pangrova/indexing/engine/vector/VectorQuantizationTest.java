package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.QuantizationType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Task 167: validates FP16 quantization, scalar quantization, and
 * QuantizationType SPI enum.
 */
class VectorQuantizationTest {

    @Test
    @DisplayName("FP16 quantization reduces precision but preserves magnitude")
    void fp16QuantizationPreservesMagnitude() {
        List<float[]> vectors = List.of(
                new float[]{1.0f, 2.5f, -3.14159f, 0.0001f},
                new float[]{100.0f, -200.0f, 0.5f, 65504.0f}
        );

        List<float[]> quantized = VectorStrategyProvider.quantizeFp16(vectors);

        assertThat(quantized).hasSize(2);
        // FP16 should preserve values within representable range
        // but with reduced precision (10-bit mantissa)
        for (int i = 0; i < vectors.size(); i++) {
            for (int d = 0; d < vectors.get(i).length; d++) {
                float original = vectors.get(i)[d];
                float quant = quantized.get(i)[d];
                // Within 1% for values > 1, wider tolerance for small values
                if (Math.abs(original) > 1.0f) {
                    assertThat(Math.abs(quant - original) / Math.abs(original))
                            .as("FP16 relative error for dim %d", d)
                            .isLessThan(0.01f);
                }
            }
        }
    }

    @Test
    @DisplayName("FP16 round-trip clamps to FP16 max range")
    void fp16RoundTripClamps() {
        float clamped = VectorStrategyProvider.fp16RoundTrip(100000.0f);
        assertThat(clamped).isLessThanOrEqualTo(65504.0f);
    }

    @Test
    @DisplayName("FP16 round-trip preserves zero and NaN")
    void fp16RoundTripSpecialValues() {
        assertThat(VectorStrategyProvider.fp16RoundTrip(0.0f)).isEqualTo(0.0f);
        assertThat(Float.isNaN(VectorStrategyProvider.fp16RoundTrip(Float.NaN))).isTrue();
    }

    @Test
    @DisplayName("scalar quantization produces values within original range")
    void scalarQuantizationRange() {
        List<float[]> vectors = new ArrayList<>();
        vectors.add(new float[]{0.0f, 1.0f, -1.0f, 0.5f});
        vectors.add(new float[]{0.5f, 0.5f, 0.0f, -0.5f});

        List<float[]> quantized = VectorStrategyProvider.quantizeScalar(vectors, 8);

        assertThat(quantized).hasSize(2);
        for (int i = 0; i < vectors.size(); i++) {
            for (int d = 0; d < 4; d++) {
                assertThat(quantized.get(i)[d])
                        .isBetween(
                                Math.min(vectors.get(0)[d], vectors.get(1)[d]),
                                Math.max(vectors.get(0)[d], vectors.get(1)[d]));
            }
        }
    }

    @Test
    @DisplayName("QuantizationType SPI enum includes FP16 and INT8")
    void quantizationTypeSpiEnum() {
        assertThat(QuantizationType.FP16.getStableId()).isEqualTo(2);
        assertThat(QuantizationType.INT8.getStableId()).isEqualTo(3);
        assertThat(QuantizationType.FP16.bytesPerDimension()).isEqualTo(2);
        assertThat(QuantizationType.INT8.bytesPerDimension()).isEqualTo(1);

        // Round-trip from stable ID
        assertThat(QuantizationType.fromStableId(2)).isEqualTo(QuantizationType.FP16);
        assertThat(QuantizationType.fromStableId(3)).isEqualTo(QuantizationType.INT8);
    }

    @Test
    @DisplayName("binary quantization produces +/-1 values centered on mean")
    void binaryQuantization() {
        List<float[]> vectors = new ArrayList<>();
        vectors.add(new float[]{1.0f, 2.0f, 3.0f});
        vectors.add(new float[]{0.0f, 0.0f, 0.0f});

        List<float[]> quantized = VectorStrategyProvider.quantizeBinaryDequantized(vectors);

        assertThat(quantized).hasSize(2);
        for (float[] q : quantized) {
            for (float v : q) {
                assertThat(v).isIn(1.0f, -1.0f);
            }
        }
    }
}
