package org.pangrova.worker.bootstrap.phase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.pangrova.worker.resource.memory.MemoryArbitrator;
import org.pangrova.worker.resource.memory.MemoryPool;
import org.pangrova.worker.resource.memory.NodeMemoryPool;
import org.pangrova.worker.resource.memory.RootMemoryPool;

class ShuffleServiceMemoryLeaseTest {

    @Test
    void reservesBothLedgersForProcessLifetimeThenReleasesOnce() {
        NodeMemoryPool node = new RootMemoryPool("root", 1024).createNodePool("node", 1024);
        MemoryArbitrator arbitrator = new MemoryArbitrator(1024, 0);
        ShuffleServiceMemoryLease lease = ShuffleServiceMemoryLease.reserve(
                "worker-a", 200, 300, node, arbitrator);
        assertThat(lease.bytes()).isEqualTo(500);
        assertThat(node.getUsedBytes()).isEqualTo(500);
        assertThat(arbitrator.getUsedMemory()).isEqualTo(500);
        assertThat(arbitrator.evictStaleAllocations()).isZero();
        lease.close();
        lease.close();
        assertThat(node.getUsedBytes()).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Test
    void partialArbitratorGrantRollsBackBothLedgers() {
        NodeMemoryPool node = new RootMemoryPool("root", 1024).createNodePool("node", 1024);
        MemoryArbitrator arbitrator = new MemoryArbitrator(1024, 0) {
            @Override
            public long reserve(MemoryPool pool, long bytes) {
                return super.reserve(pool, bytes / 2);
            }
        };
        assertThatThrownBy(() -> ShuffleServiceMemoryLease.reserve(
                "worker-a", 200, 300, node, arbitrator))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("arbitrator");
        assertThat(node.getUsedBytes()).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
    }

    @Test
    void partialNodeGrantNeverEntersArbitrator() {
        NodeMemoryPool node = new RootMemoryPool("root", 100).createNodePool("node", 100);
        MemoryArbitrator arbitrator = new MemoryArbitrator(1024, 0);
        assertThatThrownBy(() -> ShuffleServiceMemoryLease.reserve(
                "worker-a", 200, 300, node, arbitrator))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NodePool");
        assertThat(node.getUsedBytes()).isZero();
        assertThat(arbitrator.getUsedMemory()).isZero();
    }
}
