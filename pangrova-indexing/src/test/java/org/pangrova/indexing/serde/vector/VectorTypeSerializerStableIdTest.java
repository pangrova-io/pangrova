package org.pangrova.indexing.serde.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the wire format prefix byte to the stable ID (not the ordinal),
 * so reordering or inserting new {@link VectorAlgorithmType} constants
 * cannot misinterpret previously persisted vector indexes.
 */
class VectorTypeSerializerStableIdTest {

    @Test
    void firstByteIsStableIdNotOrdinal() {
        VectorTypeSerializer serializer = new VectorTypeSerializer();

        VectorIndexData empty = new VectorIndexData(
                VectorAlgorithmType.HNSW, 4, DistanceMetric.L2, 0,
                new byte[0], Map.of());
        byte[] bytes = serializer.serialize(empty);

        // HNSW stableId == 1, not ordinal (which also happens to be 0).
        // The previous ordinal-based encoding would have written 0; the stable
        // ID encoding writes 1.
        assertThat(bytes[0])
                .as("leading byte must be VectorAlgorithmType.HNSW.getStableId()")
                .isEqualTo((byte) VectorAlgorithmType.HNSW.getStableId());
        assertThat(VectorAlgorithmType.HNSW.getStableId()).isEqualTo(1);
        assertThat(VectorAlgorithmType.IVFPQ.getStableId()).isEqualTo(2);
    }

    @Test
    void deserializeRejectsUnknownStableId() {
        VectorTypeSerializer serializer = new VectorTypeSerializer();
        byte[] payload = new byte[] { (byte) 99, 0, 0, 0 };
        Throwable thrown = catchThrowable(() -> serializer.deserialize(payload));
        assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
        assertThat(thrown.getMessage()).contains("stableId");
    }

    private static Throwable catchThrowable(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
