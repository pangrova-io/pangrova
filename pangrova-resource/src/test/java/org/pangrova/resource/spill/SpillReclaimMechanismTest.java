package org.pangrova.resource.spill;

import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.spill.SpillManager.SpillCandidate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for Task 142 / F-RES-02: the Java-heap memory-pressure
 * mitigation path must reclaim memory correctly.
 */
class SpillReclaimMechanismTest {

    @Test
    void handleMemoryPressureReclaimsDeclaredAmount() {
        SpillManager spillManager = new SpillManager();
        spillManager.registerCandidate(new SpillCandidate("a", 1, 500));
        spillManager.registerCandidate(new SpillCandidate("b", 2, 300));

        long reclaimed = spillManager.handleMemoryPressure(MemoryPressure.HIGH);

        assertThat(reclaimed).isEqualTo(800L);
    }
}
