package org.pangrova.resource.spill;

import org.pangrova.resource.MemoryPressure;
import org.pangrova.resource.spill.SpillManager.SpillCandidate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SpillManager}.
 *
 * <p><b>Validates: Requirements 14.7</b></p>
 */
class SpillManagerTest {

    private SpillManager spillManager;

    @BeforeEach
    void setUp() {
        spillManager = new SpillManager("/tmp/spill");
    }

    @Test
    void spillDirectoryIsRetained() {
        assertThat(spillManager.getSpillDirectory()).isEqualTo("/tmp/spill");
    }

    @Test
    void defaultConstructorHasNullDirectory() {
        var manager = new SpillManager();
        assertThat(manager.getSpillDirectory()).isNull();
    }

    @Test
    void selectCandidatesReturnsEmptyOnLowPressure() {
        spillManager.registerCandidate(new SpillCandidate("a", 1, 1000));
        List<SpillCandidate> result = spillManager.selectCandidates(500, MemoryPressure.LOW);
        assertThat(result).isEmpty();
    }

    @Test
    void selectCandidatesReturnsByPriorityOnMediumPressure() {
        spillManager.registerCandidate(new SpillCandidate("scan", 1, 500));
        spillManager.registerCandidate(new SpillCandidate("sort", 3, 800));
        List<SpillCandidate> result = spillManager.selectCandidates(1000, MemoryPressure.MEDIUM);
        // MEDIUM allows priority <= 1 only
        assertThat(result).hasSize(1);
        assertThat(result.get(0).id()).isEqualTo("scan");
    }

    @Test
    void selectCandidatesReturnsHighPriorityOnHighPressure() {
        spillManager.registerCandidate(new SpillCandidate("scan", 1, 500));
        spillManager.registerCandidate(new SpillCandidate("sort", 3, 800));
        spillManager.registerCandidate(new SpillCandidate("join", 5, 1200));
        List<SpillCandidate> result = spillManager.selectCandidates(2000, MemoryPressure.HIGH);
        // HIGH allows priority <= 3
        assertThat(result).hasSize(2);
    }

    @Test
    void selectCandidatesReturnsAllOnCriticalPressure() {
        spillManager.registerCandidate(new SpillCandidate("scan", 1, 500));
        spillManager.registerCandidate(new SpillCandidate("join", 5, 1200));
        List<SpillCandidate> result = spillManager.selectCandidates(2000, MemoryPressure.CRITICAL);
        assertThat(result).hasSize(2);
    }

    @Test
    void selectCandidatesStopsAtTargetBytes() {
        spillManager.registerCandidate(new SpillCandidate("a", 1, 600));
        spillManager.registerCandidate(new SpillCandidate("b", 1, 500));
        List<SpillCandidate> result = spillManager.selectCandidates(600, MemoryPressure.CRITICAL);
        assertThat(result).hasSize(1);
    }

    @Test
    void removeCandidateExcludesFromSelection() {
        spillManager.registerCandidate(new SpillCandidate("a", 1, 500));
        spillManager.removeCandidate("a");
        List<SpillCandidate> result = spillManager.selectCandidates(500, MemoryPressure.CRITICAL);
        assertThat(result).isEmpty();
    }

    @Test
    void handleMemoryPressureReclaimsAndRemovesCandidates() {
        spillManager.registerCandidate(new SpillCandidate("a", 1, 500));
        spillManager.registerCandidate(new SpillCandidate("b", 2, 300));
        long reclaimed = spillManager.handleMemoryPressure(MemoryPressure.HIGH);
        assertThat(reclaimed).isEqualTo(800);
        // Candidates should be removed after spill
        assertThat(spillManager.selectCandidates(1000, MemoryPressure.CRITICAL)).isEmpty();
    }

    @Test
    void handleMemoryPressureReturnsZeroOnLow() {
        spillManager.registerCandidate(new SpillCandidate("a", 1, 500));
        long reclaimed = spillManager.handleMemoryPressure(MemoryPressure.LOW);
        assertThat(reclaimed).isEqualTo(0);
    }
}
