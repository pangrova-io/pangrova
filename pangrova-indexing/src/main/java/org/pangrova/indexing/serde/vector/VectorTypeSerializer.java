package org.pangrova.indexing.serde.vector;

import org.pangrova.indexing.serde.TypeSerializer;
import org.pangrova.spi.index.data.vector.VectorIndexData;
import org.pangrova.spi.index.data.vector.VectorIndexData.VectorAlgorithmType;

/**
 * {@link TypeSerializer} for {@link VectorIndexData}, dispatching to
 * algorithm-specific serializers based on {@link VectorAlgorithmType}.
 *
 * <p>Wire format: [algorithmStableId:1B] [delegatePayload:NB]</p>
 *
 * <p>The leading byte is the {@link VectorAlgorithmType#getStableId() stable ID}
 * (NOT {@link Enum#ordinal()}), so that reordering or inserting new algorithm
 * constants does not break previously persisted indexes.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class VectorTypeSerializer implements TypeSerializer<VectorIndexData> {

    /** Largest value encodable in the single-byte stable-ID wire field. */
    private static final int MAX_STABLE_ID = 255;

    private final HnswSerializer hnswSerializer = new HnswSerializer();
    private final IvfpqSerializer ivfpqSerializer = new IvfpqSerializer();
    private final DiskAnnSerializer diskAnnSerializer = new DiskAnnSerializer();

    @Override
    public byte[] serialize(VectorIndexData data) {
        byte[] delegatePayload = switch (data.algorithmType()) {
            case HNSW -> hnswSerializer.serialize(data);
            case IVFPQ -> ivfpqSerializer.serialize(data);
            case DISK_ANN -> diskAnnSerializer.serialize(data);
        };
        int stableId = data.algorithmType().getStableId();
        if (stableId < 0 || stableId > MAX_STABLE_ID) {
            throw new IllegalStateException(
                    "VectorAlgorithmType stableId out of byte range: " + stableId);
        }
        byte[] result = new byte[1 + delegatePayload.length];
        result[0] = (byte) stableId;
        System.arraycopy(delegatePayload, 0, result, 1, delegatePayload.length);
        return result;
    }

    @Override
    public VectorIndexData deserialize(byte[] payload) {
        if (payload == null || payload.length < 1) {
            throw new IllegalArgumentException("VectorIndexData payload is empty");
        }
        int algorithmStableId = Byte.toUnsignedInt(payload[0]);
        VectorAlgorithmType algorithm = VectorAlgorithmType.fromStableId(algorithmStableId);
        byte[] delegatePayload = new byte[payload.length - 1];
        System.arraycopy(payload, 1, delegatePayload, 0, delegatePayload.length);
        return switch (algorithm) {
            case HNSW -> hnswSerializer.deserialize(delegatePayload);
            case IVFPQ -> ivfpqSerializer.deserialize(delegatePayload);
            case DISK_ANN -> diskAnnSerializer.deserialize(delegatePayload);
        };
    }
}
