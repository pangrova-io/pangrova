package org.pangrova.worker.exchange.remote;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.MessageDigest;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.worker.exchange.CanonicalBatchChunks;

class StreamingBatchChunkValidatorTest {

    @Test
    void validatesWideBatchIncrementallyAndCompletesOnlyAfterLastChunk()
            throws Exception {
        byte[] batch = new byte[CanonicalBatchChunks.MAX_CHUNK_BYTES + 19];
        for (int index = 0; index < batch.length; index++) {
            batch[index] = (byte) index;
        }
        Checksum checksum = CanonicalBatchChunks.sha256(batch);
        StreamingBatchChunkValidator validator = new StreamingBatchChunkValidator();

        validator.accept(CanonicalBatchChunks.chunk(batch, 0, 0, 2, checksum), 7L);

        assertThat(validator.hasPartialBatch()).isTrue();
        assertThat(validator.nextBatchSequence()).isZero();
        assertThat(validator.totalBytes())
                .isEqualTo(CanonicalBatchChunks.MAX_CHUNK_BYTES);

        validator.accept(CanonicalBatchChunks.chunk(batch, 0, 1, 2, checksum), 7L);
        validator.validateOutputChecksum(checksum(
                org.pangrova.shuffle.api.commit.ShufflePartitionManifestChecksum.accumulator()
                        .add(0L, batch.length, 7L,
                                MessageDigest.getInstance("SHA-256").digest(batch))
                        .finish()));

        assertThat(validator.hasPartialBatch()).isFalse();
        assertThat(validator.nextBatchSequence()).isEqualTo(1L);
        assertThat(validator.totalBytes()).isEqualTo(batch.length);
        assertThat(validator.totalRows()).isEqualTo(7L);
    }

    @Test
    void rejectsOutOfOrderAndCorruptChunks() {
        byte[] batch = new byte[CanonicalBatchChunks.MAX_CHUNK_BYTES + 1];
        Checksum checksum = CanonicalBatchChunks.sha256(batch);
        StreamingBatchChunkValidator outOfOrder = new StreamingBatchChunkValidator();

        assertThatThrownBy(() -> outOfOrder.accept(
                CanonicalBatchChunks.chunk(batch, 0, 1, 2, checksum), 1L))
                .isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("corruption");

        var corrupt = CanonicalBatchChunks.chunk(batch, 0, 0, 2, checksum)
                .toBuilder()
                .setChunkChecksum(checksum(new byte[32]))
                .build();
        StreamingBatchChunkValidator badChecksum = new StreamingBatchChunkValidator();
        assertThatThrownBy(() -> badChecksum.accept(corrupt, 1L))
                .isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("corruption");
    }

    @Test
    void rejectsRowCountDriftAcrossChunks() {
        byte[] batch = new byte[CanonicalBatchChunks.MAX_CHUNK_BYTES + 1];
        Checksum checksum = CanonicalBatchChunks.sha256(batch);
        StreamingBatchChunkValidator validator = new StreamingBatchChunkValidator();

        validator.accept(CanonicalBatchChunks.chunk(batch, 0, 0, 2, checksum), 2L);

        assertThatThrownBy(() -> validator.accept(
                CanonicalBatchChunks.chunk(batch, 0, 1, 2, checksum), 3L))
                .isInstanceOf(RemoteShuffleClientException.class)
                .hasMessageContaining("corruption");
    }

    private static Checksum checksum(byte[] digest) {
        return Checksum.newBuilder()
                .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(com.google.protobuf.ByteString.copyFrom(digest))
                .build();
    }
}
