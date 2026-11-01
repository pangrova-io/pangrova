package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.memory.coordination.InMemoryDistributedLease;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that the HA consumer retains the token issued by its independent lease provider. */
class LeaseEpochRenewalTest {
    private static final String HOLDER_A = "coordinator-A";

    @Test
    void highAvailabilityServiceUsesIssuedEpoch() {
        InMemoryDistributedLease lease = new InMemoryDistributedLease();
        var service = new HighAvailabilityCoordinationService(
                lease,
                new HighAvailabilityCoordinationService.CoordinationConfig(
                        30_000, 10_000));
        try {
            assertThat(service.tryBecomeLeader(HOLDER_A)).isTrue();
            assertThat(service.getCurrentEpoch()).isEqualTo(1);
            assertThat(service.renewLease(HOLDER_A)).isTrue();
            assertThat(service.getCurrentEpoch()).isEqualTo(1);
        } finally {
            service.close();
        }
    }
}
