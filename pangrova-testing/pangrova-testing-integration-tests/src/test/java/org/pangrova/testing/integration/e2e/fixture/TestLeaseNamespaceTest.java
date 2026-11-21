package org.pangrova.testing.integration.e2e.fixture;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class TestLeaseNamespaceTest {

    @Test
    void isolatesIndependentTestsButRetainsCompetitionWithinOneTest() {
        var backing = new InMemoryDistributedLease();
        var firstNamespace = new TestLeaseNamespace();
        DistributedLease first = firstNamespace.wrap(backing);
        DistributedLease contender = firstNamespace.wrap(backing);
        DistributedLease independent = new TestLeaseNamespace().wrap(backing);

        var firstEntry = first.tryAcquire("coordinator_leader", "first",
                Duration.ofSeconds(10)).orElseThrow();
        assertThat(firstEntry.leaseKey()).isEqualTo("coordinator_leader");
        assertThat(contender.tryAcquire("coordinator_leader", "contender",
                Duration.ofSeconds(10))).isEmpty();
        assertThat(independent.tryAcquire("coordinator_leader", "independent",
                Duration.ofSeconds(10))).isPresent();

        assertThat(first.release("coordinator_leader", "first", firstEntry.epoch())).isTrue();
        var nextEntry = contender.tryAcquire("coordinator_leader", "contender",
                Duration.ofSeconds(10)).orElseThrow();
        assertThat(nextEntry.epoch()).isGreaterThan(firstEntry.epoch());
    }
}
