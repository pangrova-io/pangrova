package org.pangrova.worker.shuffle;

import org.pangrova.worker.buffer.ShuffleBufferKey;
import org.pangrova.worker.buffer.ShuffleBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Adapts ShuffleDrainThread output to ShuffleBufferManager.
 *
 * <p>Receives pre-serialized PrestoVectorSerde bytes from the native engine
 * and stores them in the shuffle buffer for cross-process transfer via
 * transport layer. The byte layout from JNI is:
 * {@code [4-byte partition_id (big-endian)] [serialized data]}</p>
 *
 * <p>Uses composite {@link ShuffleBufferKey} (queryId + stageId + partitionId)
 * to ensure query/stage isolation on the same worker (P0-FPV-7).</p>
 *
 * <p>Thread safety: the underlying {@link ShuffleBufferManager} is thread-safe.</p>
 */
public class ShuffleBufferSender implements ShuffleDrainThread.ShuffleSender {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferSender.class);
    private static final int HEADER_SIZE = 4;

    /** Mask to read a single unsigned byte. */
    private static final int BYTE_MASK = 0xFF;

    /** Bit shift of the most significant byte of the partition id header. */
    private static final int PARTITION_ID_MSB_SHIFT = 24;

    private final ShuffleBufferManager bufferManager;
    private final String queryId;
    private final String stageId;

    /**
     * @param bufferManager partition buffer manager (must not be null)
     * @param queryId       query identifier for buffer isolation
     * @param stageId       stage identifier for buffer isolation
     */
    public ShuffleBufferSender(ShuffleBufferManager bufferManager,
                               String queryId, String stageId) {
        this.bufferManager = checkNotNull(bufferManager, "bufferManager must not be null");
        this.queryId = queryId != null ? queryId : "_default";
        this.stageId = stageId != null ? stageId : "_default";
    }

    @Override
    public void send(byte[] serializedData) {
        if (serializedData.length < HEADER_SIZE) {
            LOG.error("event=shuffle_batch_too_short length={}", serializedData.length);
            return;
        }
        int partitionId = extractPartitionId(serializedData);
        byte[] payload = extractPayload(serializedData);
        ShuffleBufferKey key = new ShuffleBufferKey(queryId, stageId, partitionId);
        try {
            bufferManager.addBatch(key, payload);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int extractPartitionId(byte[] data) {
        return ((data[0] & BYTE_MASK) << PARTITION_ID_MSB_SHIFT)
                | ((data[1] & BYTE_MASK) << 16)
                | ((data[2] & BYTE_MASK) << 8)
                | (data[3] & BYTE_MASK);
    }

    private static byte[] extractPayload(byte[] data) {
        byte[] payload = new byte[data.length - HEADER_SIZE];
        System.arraycopy(data, HEADER_SIZE, payload, 0, payload.length);
        return payload;
    }
}
