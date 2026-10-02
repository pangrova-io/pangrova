package org.pangrova.worker.exchange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.MessageDigest;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.Checksum;

class CanonicalBatchChunksTest {

    @Test
    void splitsWideBatchWithCompleteAndPerChunkIntegrity() throws Exception {
        byte[] batch = new byte[CanonicalBatchChunks.MAX_CHUNK_BYTES + 17];
        for (int index = 0; index < batch.length; index++) {
            batch[index] = (byte) index;
        }
        Checksum batchChecksum = CanonicalBatchChunks.sha256(batch);

        var first = CanonicalBatchChunks.chunk(batch, 4, 0, 2, batchChecksum);
        var second = CanonicalBatchChunks.chunk(batch, 4, 1, 2, batchChecksum);

        assertThat(first.getData().size())
                .isEqualTo(CanonicalBatchChunks.MAX_CHUNK_BYTES);
        assertThat(second.getData().size()).isEqualTo(17);
        assertThat(first.getBatchSequence()).isEqualTo(4);
        assertThat(second.getChunkIndex()).isEqualTo(1);
        assertThat(first.getBatchSizeBytes()).isEqualTo(batch.length);
        assertThat(first.getBatchChecksum()).isEqualTo(batchChecksum);
        assertThat(second.getBatchChecksum()).isEqualTo(batchChecksum);
        assertThat(first.getChunkChecksum().getDigest().toByteArray())
                .isEqualTo(MessageDigest.getInstance("SHA-256")
                        .digest(first.getData().toByteArray()));
    }

    @Test
    void rejectsEmptyAndInconsistentChunkIdentity() {
        assertThatThrownBy(() -> CanonicalBatchChunks.chunkCount(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
        byte[] batch = new byte[]{1};
        assertThatThrownBy(() -> CanonicalBatchChunks.chunk(
                batch, 0, 1, 1, CanonicalBatchChunks.sha256(batch)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void chunksDirectBufferWithoutChangingCallerPosition() {
        int batchSize = CanonicalBatchChunks.MAX_CHUNK_BYTES + 11;
        ByteBuffer direct = ByteBuffer.allocateDirect(batchSize + 4);
        direct.position(4);
        for (int index = 0; index < batchSize; index++) {
            direct.put((byte) (index * 31));
        }
        direct.flip().position(4);
        int originalPosition = direct.position();
        int originalLimit = direct.limit();

        Checksum checksum = CanonicalBatchChunks.sha256(direct);
        var first = CanonicalBatchChunks.chunk(direct, 0, 0, 2, checksum);
        var second = CanonicalBatchChunks.chunk(direct, 0, 1, 2, checksum);

        assertThat(first.getData().size()).isEqualTo(CanonicalBatchChunks.MAX_CHUNK_BYTES);
        assertThat(second.getData().size()).isEqualTo(11);
        assertThat(direct.position()).isEqualTo(originalPosition);
        assertThat(direct.limit()).isEqualTo(originalLimit);
    }
}
