package org.pangrova.worker.property.buffer;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.worker.buffer.ShuffleBufferKey;
import org.pangrova.worker.buffer.ShuffleBufferManager;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for shuffle buffer query/stage isolation (P0-FPV-7).
 *
 * <p>Validates: Requirements A1.5</p>
 *
 * <p>Core property: data written to a composite key (queryId, stageId, partitionId)
 * is never visible to a different composite key, even when partitionId is the same.</p>
 */
class ShuffleBufferIsolationPropertyTest {

    @Provide
    Arbitrary<String> queryIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                .map(s -> "q-" + s);
    }

    @Provide
    Arbitrary<String> stageIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(8)
                .map(s -> "s-" + s);
    }

    /**
     * Two distinct composite keys sharing the same partitionId produce
     * isolated buffer spaces — data written to one is never read from the other.
     */
    @Property(tries = 50)
    void distinctKeysWithSamePartitionAreIsolated(
            @ForAll("queryIds") String queryA,
            @ForAll("queryIds") String queryB,
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 0, max = 15) int partitionId
    ) throws InterruptedException {
        // Ensure the two queries are actually different
        if (queryA.equals(queryB)) {
            return;
        }

        ShuffleBufferManager manager = new ShuffleBufferManager(100);
        try {
            ShuffleBufferKey keyA = new ShuffleBufferKey(queryA, stageId, partitionId);
            ShuffleBufferKey keyB = new ShuffleBufferKey(queryB, stageId, partitionId);

            manager.createPartition(keyA);
            manager.createPartition(keyB);

            byte[] dataA = ("data-from-" + queryA).getBytes();
            manager.addBatch(keyA, dataA);

            // keyB should have no data
            byte[] polledFromB = manager.pollBatch(keyB, 10);
            assertThat(polledFromB).isNull();

            // keyA should return its own data
            byte[] polledFromA = manager.pollBatch(keyA, 10);
            assertThat(polledFromA).isEqualTo(dataA);
        } finally {
            manager.close();
        }
    }

    /**
     * Same composite key always returns the data that was written to it,
     * preserving FIFO order.
     */
    @Property(tries = 50)
    void sameKeyReturnsFifoData(
            @ForAll("queryIds") String queryId,
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 0, max = 15) int partitionId,
            @ForAll @IntRange(min = 1, max = 10) int batchCount
    ) throws InterruptedException {
        ShuffleBufferManager manager = new ShuffleBufferManager(100);
        try {
            ShuffleBufferKey key = new ShuffleBufferKey(queryId, stageId, partitionId);
            manager.createPartition(key);

            byte[][] batches = new byte[batchCount][];
            for (int i = 0; i < batchCount; i++) {
                batches[i] = ("batch-" + i).getBytes();
                manager.addBatch(key, batches[i]);
            }

            for (int i = 0; i < batchCount; i++) {
                byte[] polled = manager.pollBatch(key, 100);
                assertThat(polled).isEqualTo(batches[i]);
            }
        } finally {
            manager.close();
        }
    }

    /**
     * ShuffleBufferKey equality is based on all three fields.
     * Two keys with identical (queryId, stageId, partitionId) are equal;
     * any field difference makes them unequal.
     */
    @Property(tries = 100)
    void keyEqualityReflectsAllFields(
            @ForAll("queryIds") String q1,
            @ForAll("queryIds") String q2,
            @ForAll("stageIds") String s1,
            @ForAll("stageIds") String s2,
            @ForAll @IntRange(min = 0, max = 31) int p1,
            @ForAll @IntRange(min = 0, max = 31) int p2
    ) {
        ShuffleBufferKey a = new ShuffleBufferKey(q1, s1, p1);
        ShuffleBufferKey b = new ShuffleBufferKey(q2, s2, p2);

        boolean allFieldsEqual = q1.equals(q2) && s1.equals(s2) && p1 == p2;
        assertThat(a.equals(b)).isEqualTo(allFieldsEqual);

        if (allFieldsEqual) {
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }
    }

    /**
     * closePartition on one key does not affect another key's closed state.
     */
    @Property(tries = 50)
    void closePartitionIsKeyScoped(
            @ForAll("queryIds") String queryA,
            @ForAll("queryIds") String queryB,
            @ForAll("stageIds") String stageId,
            @ForAll @IntRange(min = 0, max = 15) int partitionId
    ) {
        if (queryA.equals(queryB)) {
            return;
        }

        ShuffleBufferManager manager = new ShuffleBufferManager(100);
        try {
            ShuffleBufferKey keyA = new ShuffleBufferKey(queryA, stageId, partitionId);
            ShuffleBufferKey keyB = new ShuffleBufferKey(queryB, stageId, partitionId);

            manager.createPartition(keyA);
            manager.createPartition(keyB);

            manager.closePartition(keyA);

            assertThat(manager.isPartitionClosed(keyA)).isTrue();
            assertThat(manager.isPartitionClosed(keyB)).isFalse();
        } finally {
            manager.close();
        }
    }
}
