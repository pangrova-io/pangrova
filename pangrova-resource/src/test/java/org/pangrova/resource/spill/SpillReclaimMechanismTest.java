package org.pangrova.resource.spill;

import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.spill.DiskSpillableBuffer;
import org.pangrova.resource.spill.SpillManager.SpillCandidate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;


import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for Task 142 / F-RES-02: the Java-heap memory-pressure
 * mitigation path must reclaim memory correctly.
 */
class SpillReclaimMechanismTest {

    @TempDir
    Path spillDirectory;

    private final java.util.List<DiskSpillableBuffer> actualBuffers = new java.util.ArrayList<>();

    private SpillCandidate actualCandidate(String id, int priority, int bytes) {
        DiskSpillableBuffer buffer = new DiskSpillableBuffer(id, priority, spillDirectory);
        buffer.addBatch(new byte[bytes]);
        actualBuffers.add(buffer);
        return buffer.toSpillCandidate();
    }

    @AfterEach
    void closeActualBuffers() {
        for (DiskSpillableBuffer buffer : actualBuffers) {
            buffer.close();
        }
    }


    @Test
    void handleMemoryPressureReclaimsActualDiskBufferBytes() {
        SpillManager spillManager = new SpillManager();
        spillManager.registerCandidate(actualCandidate("a", 1, 500));
        spillManager.registerCandidate(actualCandidate("b", 2, 300));

        long reclaimed = spillManager.handleMemoryPressure(MemoryPressure.HIGH);

        assertThat(reclaimed).isEqualTo(800L);
    }
}
