package org.pangrova.shuffle.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.shuffle.codec.ShufflePartitionManifestChecksum;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.shuffle.server.exception.ShuffleServiceException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.shuffle.server.ShufflePartitionManifest.BATCHES_PER_PAGE;
import static org.pangrova.shuffle.server.ShufflePartitionManifest.FANOUT;
import static org.pangrova.shuffle.server.ShufflePartitionManifest.INLINE_BATCHES;
import static org.pangrova.shuffle.server.ShuffleServiceModel.BatchIdentity;
import static org.pangrova.shuffle.server.ShuffleServiceModel.ReadBatch;
import static org.pangrova.shuffle.server.ShuffleServiceModel.StoredBatch;

class ShufflePartitionManifestTest {
    private static final String SHUFFLE = "test-shuffle";
    private static final int PARTITION = 6;
    private static final byte[] CHECKSUM = new byte[32];

    @Test
    void emptyAndSinglePageRootsUseNoObjectReadsEvenWhenResuming() {
        assertThat(new ShufflePartitionManifest.Reader(SHUFFLE, PARTITION, -1, new byte[0])
                .page(0, ShufflePartitionManifestTest::unexpectedGet, () -> {})).isEmpty();
        Map<String, byte[]> objects = new HashMap<>();
        var reader = build(objects, INLINE_BATCHES);
        assertThat(objects).isEmpty();
        assertThat(reader.page(254, ShufflePartitionManifestTest::unexpectedGet, () -> {}))
                .extracting(ReadBatch::outputSequence).containsExactly(254L, 255L);
    }

    @Test
    void seeksAcrossSeveralDirectoryLevelsAndReusesTheDirectoryWhileScanning() {
        int boundary = BATCHES_PER_PAGE * FANOUT;
        Map<String, byte[]> objects = new HashMap<>();
        var reader = build(objects, boundary + BATCHES_PER_PAGE + 8);
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger authorizations = new AtomicInteger();
        ShufflePartitionManifest.NodeReader fetch = (key, length) -> {
            reads.incrementAndGet();
            byte[] bytes = objects.get(key);
            assertThat(bytes).hasSize(length);
            return bytes;
        };
        assertThat(reader.page(boundary - 1, fetch, authorizations::incrementAndGet))
                .singleElement().satisfies(batch -> assertBatch(batch, boundary - 1));
        assertThat(reads.get()).isEqualTo(2);
        assertThat(reader.page(boundary, fetch, authorizations::incrementAndGet))
                .first().satisfies(batch -> assertBatch(batch, boundary));
        assertThat(reads.get()).isEqualTo(4);
        int lastPage = boundary + BATCHES_PER_PAGE;
        assertThat(reader.page(lastPage + 5, fetch, authorizations::incrementAndGet))
                .extracting(ReadBatch::outputSequence)
                .containsExactly(lastPage + 5L, lastPage + 6L, lastPage + 7L);
        assertThat(reads.get()).isEqualTo(5);
        assertThat(authorizations.get()).isEqualTo(10);
    }

    @Test
    void crossingTheInlineLimitUsesOneObjectAndBoundedDecodeWindows() {
        Map<String, byte[]> objects = new HashMap<>();
        var reader = build(objects, INLINE_BATCHES + 17);
        AtomicInteger reads = new AtomicInteger();
        ShufflePartitionManifest.NodeReader fetch = (key, length) -> {
            reads.incrementAndGet();
            return objects.get(key);
        };
        assertThat(reader.page(0, fetch, () -> {})).hasSize(INLINE_BATCHES);
        assertThat(reader.page(INLINE_BATCHES, fetch, () -> {})).hasSize(17);
        assertThat(reads.get()).isEqualTo(1);
    }

    @Test
    void publishesAbsoluteRangeDigestsWithoutChangingTheWholePartitionDigest() {
        long size = 40L * 1024 * 1024;
        var writer = new ShufflePartitionManifest.Writer((digest, bytes) -> {
            throw new AssertionError("Inline publication should not write an object");
        });
        var expected = ShufflePartitionManifestChecksum.accumulator();
        for (int sequence = 0; sequence < 5; sequence++) {
            var id = identity(sequence);
            writer.add(new StoredBatch(id, TaskAttemptId.getDefaultInstance(), id.objectKey(), size, 3, CHECKSUM));
            expected.add(sequence, size, 3, CHECKSUM);
        }
        var finalized = writer.finish(PARTITION).partition();
        assertThat(finalized.checksum()).isEqualTo(expected.finishNonEmpty());
        assertThat(finalized.readRanges()).hasSize(3);
        var middle = finalized.readRanges().get(1);
        assertThat(middle.startSequence()).isEqualTo(2);
        assertThat(middle.endSequenceExclusive()).isEqualTo(4);
        assertThat(middle.bytes()).isEqualTo(2 * size);
        assertThat(middle.rows()).isEqualTo(6);
        var absoluteDigest = ShufflePartitionManifestChecksum.accumulator(2)
                .add(2, size, 3, CHECKSUM).add(3, size, 3, CHECKSUM).finishNonEmpty();
        assertThat(middle.manifestSha256()).isEqualTo(java.util.HexFormat.of().formatHex(absoluteDigest));
    }

    @Test
    void rebuildingTheSameLogicalPageAllocatesAnIndependentObjectKey() {
        Map<String, byte[]> first = new HashMap<>();
        var firstReader = build(first, INLINE_BATCHES + 1);
        Map<String, byte[]> second = new HashMap<>();
        var secondReader = build(second, INLINE_BATCHES + 1);
        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
        assertThat(first.keySet()).doesNotContainAnyElementsOf(second.keySet());
        assertThat(firstReader.page(INLINE_BATCHES, (key, length) -> first.get(key), () -> {}))
                .singleElement().satisfies(batch -> assertBatch(batch, INLINE_BATCHES));
        assertThat(secondReader.page(INLINE_BATCHES, (key, length) -> second.get(key), () -> {}))
                .singleElement().satisfies(batch -> assertBatch(batch, INLINE_BATCHES));
    }

    @Test
    void refusesToReturnBatchesFromACorruptObjectPage() {
        Map<String, byte[]> objects = new HashMap<>();
        var reader = build(objects, BATCHES_PER_PAGE + 1);
        assertThatThrownBy(() -> reader.page(BATCHES_PER_PAGE, (key, length) -> {
            byte[] content = objects.get(key).clone();
            content[0] ^= 1;
            return content;
        }, () -> {})).isInstanceOf(ShuffleServiceException.class).hasMessageContaining("published digest");
    }

    private static byte[] unexpectedGet(String key, int length) {
        throw new AssertionError("inline manifest opened object " + key);
    }

    private static ShufflePartitionManifest.Reader build(Map<String, byte[]> objects, int count) {
        var writer = new ShufflePartitionManifest.Writer(
                (hash, content) -> objects.put(ShufflePartitionManifest.objectKey(SHUFFLE, hash), content));
        for (int index = 0; index < count; index++) {
            var id = identity(index);
            writer.add(new StoredBatch(id, TaskAttemptId.getDefaultInstance(),
                    id.objectKey(), 12, 3, CHECKSUM));
        }
        return new ShufflePartitionManifest.Reader(
                SHUFFLE, PARTITION, count - 1L, writer.finish(PARTITION).manifestRoot());
    }

    private static void assertBatch(ReadBatch batch, int index) {
        assertThat(batch.outputSequence()).isEqualTo(index);
        assertThat(batch.objectKey()).isEqualTo(identity(index).objectKey());
        assertThat(batch.sizeBytes()).isEqualTo(12);
        assertThat(batch.rowCount()).isEqualTo(3);
        assertThat(batch.checksum()).isEqualTo(CHECKSUM);
    }

    private static BatchIdentity identity(int index) {
        return new BatchIdentity(SHUFFLE, index / 135, 7, PARTITION, index % 135);
    }
}
