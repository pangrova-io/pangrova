package org.pangrova.worker.resource.memory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for Tasks 94/97: allocation behavior without spill manager.
 */
class SynchronousReclaimHandshakeTest {

    @Test
    void withoutSpillManagerAllocationBehavesAsBeforeFix() {
        // No spill manager wired: original behavior.
        MemoryArbitrator arbitrator = new MemoryArbitrator(1000, 0);

        arbitrator.requestMemory("q1", 900);

        // Only 100 available, request 200 -> partial allocation.
        long allocated = arbitrator.requestMemory("q2", 200);
        assertThat(allocated).isEqualTo(100);
    }
}
