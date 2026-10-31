package org.pangrova.worker.shuffle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.pangrova.shuffle.api.ShuffleByteEgress;
import org.pangrova.shuffle.spool.SpoolStore;
import org.pangrova.shuffle.spool.SpoolWriter;
import org.pangrova.shuffle.spool.SpoolReader;
import org.pangrova.shuffle.api.ShuffleDurability;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DefaultShuffleByteEgress} (ADR-002 D2-egress).
 * Validates that bytes received from the native layer are correctly routed
 * to the SpoolStore with proper open/write/commit semantics.
 */
class DefaultShuffleByteEgressTest {

    @Test
    void routesBytesToSpoolStore() {
        var store = new RecordingSpoolStore();
        var egress = new DefaultShuffleByteEgress(store);

        byte[] payload = {0x01, 0x02, 0x03, 0x04, 0x05};
        ByteBuffer buf = ByteBuffer.wrap(payload);

        egress.onPartitionCommitted("q1", 2, 3, 4, 100L, buf);

        assertThat(store.segments).hasSize(1);
        var seg = store.segments.get(0);
        assertThat(seg.queryId).isEqualTo("q1");
        assertThat(seg.stageId).isEqualTo(2);
        assertThat(seg.partitionId).isEqualTo(3);
        assertThat(seg.mapId).isEqualTo(4);
        assertThat(seg.attemptId).isEqualTo(100L);
        assertThat(seg.data).isEqualTo(payload);
        assertThat(seg.committed).isTrue();
    }

    @Test
    void multipleCallsProduceMultipleSegments() {
        var store = new RecordingSpoolStore();
        var egress = new DefaultShuffleByteEgress(store);

        for (int i = 0; i < 5; i++) {
            byte[] payload = new byte[]{(byte) i};
            egress.onPartitionCommitted("q2", 1, i, 0, 0L, ByteBuffer.wrap(payload));
        }

        assertThat(store.segments).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(store.segments.get(i).partitionId).isEqualTo(i);
        }
    }

    @Test
    void wrapsWriteFailureInShuffleDrainException() {
        var store = new FailingSpoolStore();
        var egress = new DefaultShuffleByteEgress(store);

        assertThatThrownBy(() ->
                egress.onPartitionCommitted("q3", 0, 0, 0, 0L, ByteBuffer.allocate(10)))
                .isInstanceOf(ShuffleDrainException.class)
                .hasMessageContaining("Egress write failed")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    @Test
    void rejectsNullSpoolStore() {
        assertThatThrownBy(() -> new DefaultShuffleByteEgress(null))
                .isInstanceOf(NullPointerException.class);
    }

    // --- Test doubles ---

    /** Records all open/write/commit calls for verification. */
    private static class RecordingSpoolStore implements SpoolStore {
        final List<RecordedSegment> segments = new ArrayList<>();

        @Override
        public SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId) {
            var seg = new RecordedSegment(queryId, stageId, partitionId, mapId, attemptId);
            segments.add(seg);
            return new RecordingSpoolWriter(seg);
        }

        @Override public SpoolReader read(String queryId, int stageId, int partitionId, int startMapIndex, int endMapIndex) { return null; }
        @Override public void delete(String queryId) {}
        @Override public ShuffleDurability durability() { return ShuffleDurability.LOCAL_SPILL; }
        @Override public void close() {}
    }

    private static class RecordingSpoolWriter implements SpoolWriter {
        private final RecordedSegment seg;
        RecordingSpoolWriter(RecordedSegment seg) { this.seg = seg; }

        @Override
        public void write(ByteBuffer arrowIpc) {
            byte[] bytes = new byte[arrowIpc.remaining()];
            arrowIpc.get(bytes);
            seg.data = bytes;
        }

        @Override public void commit() { seg.committed = true; }
        @Override public void close() {}
    }

    private static class RecordedSegment {
        final String queryId;
        final int stageId;
        final int partitionId;
        final int mapId;
        final long attemptId;
        byte[] data;
        boolean committed;

        RecordedSegment(String queryId, int stageId, int partitionId, int mapId, long attemptId) {
            this.queryId = queryId;
            this.stageId = stageId;
            this.partitionId = partitionId;
            this.mapId = mapId;
            this.attemptId = attemptId;
        }
    }

    /** SpoolStore that always fails on write. */
    private static class FailingSpoolStore implements SpoolStore {
        @Override
        public SpoolWriter open(String queryId, int stageId, int partitionId, int mapId, long attemptId) {
            return new SpoolWriter() {
                @Override public void write(ByteBuffer arrowIpc) {
                    throw new RuntimeException("simulated disk failure");
                }
                @Override public void commit() {}
                @Override public void close() {}
            };
        }

        @Override public SpoolReader read(String queryId, int stageId, int partitionId, int startMapIndex, int endMapIndex) { return null; }
        @Override public void delete(String queryId) {}
        @Override public ShuffleDurability durability() { return ShuffleDurability.LOCAL_SPILL; }
        @Override public void close() {}
    }
}
