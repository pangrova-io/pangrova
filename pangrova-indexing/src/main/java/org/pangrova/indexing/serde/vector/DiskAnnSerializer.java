package org.pangrova.indexing.serde.vector;

import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;
import org.pangrova.spi.index.vector.DistanceMetric;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * Serializer for DiskANN-specific {@link VectorIndexData}.
 *
 * <p>DiskANN indexes are SSD-resident; the {@code indexPayload} stores the
 * path reference to the index directory rather than the index itself.</p>
 *
 * <p>Format: [dimension:4B] [metric:1B] [vectorCount:4B] [pathLength:4B] [indexPath:NB]</p>
 */
final class DiskAnnSerializer {

    /** Mask to widen a signed byte to its unsigned ordinal. */
    private static final int BYTE_MASK = 0xFF;

    byte[] serialize(VectorIndexData data) {
        try {
            var baos = new ByteArrayOutputStream();
            var out = new DataOutputStream(baos);
            out.writeInt(data.dimension());
            out.writeByte(data.metric().ordinal());
            out.writeInt(data.vectorCount());
            byte[] pathBytes = data.indexPayload() != null ? data.indexPayload() : new byte[0];
            out.writeInt(pathBytes.length);
            out.write(pathBytes);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize DiskANN index data", e);
        }
    }

    VectorIndexData deserialize(byte[] payload) {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(payload));
            int dimension = in.readInt();
            int metricOrdinal = in.readByte() & BYTE_MASK;
            DistanceMetric metric = DistanceMetric.values()[metricOrdinal];
            int vectorCount = in.readInt();
            int pathLength = in.readInt();
            byte[] pathBytes = new byte[pathLength];
            in.readFully(pathBytes);
            return new VectorIndexData(
                    VectorAlgorithmType.DISK_ANN,
                    dimension,
                    metric,
                    vectorCount,
                    pathBytes,
                    Map.of());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize DiskANN index data", e);
        }
    }
}
