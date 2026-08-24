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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serializer for HNSW-specific {@link VectorIndexData}.
 *
 * <p>Format: [dimension:4B] [metric:UTF] [vectorCount:4B]
 * [paramsCount:4B] {[key:UTF] [value:UTF]}* [payloadLen:4B] [payload:NB]</p>
 *
 * <p>The {@code indexPayload} field already contains the HNSW graph binary
 * produced by {@code HnswGraphCodec} (with its own CRC32). This serializer
 * wraps the metadata envelope around it.</p>
 */
final class HnswSerializer {

    byte[] serialize(VectorIndexData data) {
        try {
            var baos = new ByteArrayOutputStream();
            var out = new DataOutputStream(baos);
            out.writeInt(data.dimension());
            out.writeUTF(data.metric().name());
            out.writeInt(data.vectorCount());
            writeParams(out, data.buildParams());
            writePayload(out, data.indexPayload());
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    VectorIndexData deserialize(byte[] bytes) {
        try {
            var in = new DataInputStream(new ByteArrayInputStream(bytes));
            int dimension = in.readInt();
            String metricName = in.readUTF();
            DistanceMetric metric = parseMetric(metricName);
            int vectorCount = in.readInt();
            Map<String, String> params = readParams(in);
            byte[] payload = readPayload(in);
            return new VectorIndexData(VectorAlgorithmType.HNSW, dimension, metric,
                    vectorCount, payload, params);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeParams(DataOutputStream out, Map<String, String> params) throws IOException {
        out.writeInt(params.size());
        for (var entry : params.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    private static Map<String, String> readParams(DataInputStream in) throws IOException {
        int count = in.readInt();
        var map = new LinkedHashMap<String, String>(count);
        for (int i = 0; i < count; i++) {
            map.put(in.readUTF(), in.readUTF());
        }
        return Map.copyOf(map);
    }

    private static void writePayload(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
    }

    private static byte[] readPayload(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] payload = new byte[len];
        in.readFully(payload);
        return payload;
    }

    private static DistanceMetric parseMetric(String name) {
        try {
            return DistanceMetric.valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new UncheckedIOException(new IOException(
                    "Unknown distance metric in HNSW index data: " + name, e));
        }
    }
}
