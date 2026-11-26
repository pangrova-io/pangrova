package org.pangrova.server.coordinator.bootstrap;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryDurableRecoveryTest {

    @Test
    void unclaimedExecutionDoesNotEnterWriteOrShuffleRecovery() {
        var config = CoordinatorBootstrapConfig.withDefaults("coordinator-1", 0);
        try (var wiring = new QueryDurableRecovery(config, null, null)) {
            assertThat(wiring.recover(List.of(), null)).isEmpty();
            assertThatThrownBy(() -> wiring.resolveShuffleOwner("query-1"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not initialized");
        }
    }

    @Test
    void resolvesTheExactCurrentProcessGrantWithoutAShuffleScan() {
        String coordinatorId = "coordinator-1";
        var config = CoordinatorBootstrapConfig.withDefaults(coordinatorId, 0);
        try (var service = new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(), new InMemoryExecutionAnchorStore());
                var wiring = new QueryDurableRecovery(config, null, service)) {
            assertThat(service.acquireQueryOwnership("query-1", coordinatorId)).isTrue();
            var grant = service.requireExecutionAnchor("query-1");

            assertThat(wiring.resolveShuffleOwner("query-1")).satisfies(owner -> {
                assertThat(owner.ownerId()).isEqualTo(grant.ownerId());
                assertThat(owner.ownerIncarnation()).isEqualTo(grant.ownerIncarnation());
                assertThat(owner.fencingToken()).isEqualTo(grant.fencingToken());
            });
        }
    }
}
