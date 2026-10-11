package org.pangrova.resource.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class MemoryArbitratorAllocationSnapshotTest {
    @Test
    void querySnapshotObservesActualReservationsAndRemainsImmutableAfterRelease() {
        var arbitrator = new MemoryArbitrator(4096, 0);
        var pool = new RootMemoryPool("actual-pool", 4096);
        assertThat(arbitrator.reserve(pool, 128)).isEqualTo(128);
        assertThat(arbitrator.requestMemory("buffer:actual-attempt", 512)).isEqualTo(512);
        var captured = arbitrator.queryAllocationsSnapshot();
        assertThat(captured)
                .containsExactlyEntriesOf(java.util.Map.of("buffer:actual-attempt", 512L));
        assertThat(arbitrator.snapshot())
                .containsExactlyEntriesOf(java.util.Map.of("actual-pool", 128L));
        assertThatThrownBy(() -> captured.put("changed", 1L))
                .isInstanceOf(UnsupportedOperationException.class);
        arbitrator.release("buffer:actual-attempt", 512);
        assertThat(arbitrator.queryAllocationsSnapshot()).isEmpty();
        assertThat(captured)
                .containsExactlyEntriesOf(java.util.Map.of("buffer:actual-attempt", 512L));
        assertThat(arbitrator.getUsedMemory()).isEqualTo(128);
        arbitrator.release(pool, 128);
        assertThat(arbitrator.getUsedMemory()).isZero();
    }
}
