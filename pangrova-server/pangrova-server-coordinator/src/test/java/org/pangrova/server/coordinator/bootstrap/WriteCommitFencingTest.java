package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore;
import org.pangrova.coordinator.query.task.write.WriteCommitFencer;

import static org.assertj.core.api.Assertions.assertThat;

class WriteCommitFencingTest {

    @Test
    void eachCoordinatorOwnsItsProcessLocalWriteJournal() {
        assertThat(WriteCommitComponentFactory.createJournal(null)).isNotSameAs(WriteCommitComponentFactory.createJournal(null));
    }


    @Test
    void nonControlLeaderCanCommitWithItsOwnExecutionAnchor() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var anchors = new InMemoryExecutionAnchorStore();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        var leader = new HighAvailabilityCoordinationService(lease, config, anchors);
        var peer = new HighAvailabilityCoordinationService(lease, config, anchors);


        try {
            leader.start("coordinator-a");
            peer.start("coordinator-b");
            assertThat(peer.acquireQueryOwnership("query-1", "coordinator-b")).isTrue();

            WriteCommitFencer fencer = WriteCommitComponentFactory.createFencer(peer);
            var anchor = fencer.currentAnchor("query-1");

            assertThat(peer.isLeader("coordinator-b")).isFalse();
            assertThat(anchor.ownerId()).isEqualTo("coordinator-b");
            assertThat(fencer.isCurrent("query-1", anchor.fencingToken())).isTrue();
        } finally {
            leader.close();
            peer.close();
        }
    }


    @Test
    void takeoverInvalidatesThePreviousExecutionToken() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var anchors = new InMemoryExecutionAnchorStore();
        var config = new HighAvailabilityCoordinationService.CoordinationConfig(5000, 1000);
        var first = new HighAvailabilityCoordinationService(lease, config, anchors);
        var replacement = new HighAvailabilityCoordinationService(lease, config, anchors);


        try {
            first.start("coordinator-a");
            replacement.start("coordinator-b");
            assertThat(first.acquireQueryOwnership("query-1", "coordinator-a")).isTrue();
            WriteCommitFencer stale = WriteCommitComponentFactory.createFencer(first);
            var oldAnchor = stale.currentAnchor("query-1");

            assertThat(first.yieldExecution(oldAnchor)).isTrue();
            assertThat(replacement.acquireQueryOwnership("query-1", "coordinator-b")).isTrue();
            WriteCommitFencer current = WriteCommitComponentFactory.createFencer(replacement);
            var newAnchor = current.currentAnchor("query-1");

            assertThat(newAnchor.fencingToken()).isGreaterThan(oldAnchor.fencingToken());
            assertThat(stale.isCurrent("query-1", oldAnchor.fencingToken())).isFalse();
            assertThat(current.isCurrent("query-1", newAnchor.fencingToken())).isTrue();
        } finally {
            first.close();
            replacement.close();
        }
    }
}
