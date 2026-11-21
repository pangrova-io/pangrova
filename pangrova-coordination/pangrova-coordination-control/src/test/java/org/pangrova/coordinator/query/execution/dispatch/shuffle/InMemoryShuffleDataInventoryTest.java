package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.api.DecommissionAssessment;
import org.pangrova.shuffle.api.ShuffleDataRef;
import org.pangrova.shuffle.api.enums.ShuffleRetentionPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link InMemoryShuffleDataInventory} (G-RES-08).
 */
class InMemoryShuffleDataInventoryTest {

    private InMemoryShuffleDataInventory inventory;

    @BeforeEach
    void setUp() {
        inventory = new InMemoryShuffleDataInventory();
    }

    @Test
    void emptyInventoryReturnsNoLiveData() {
        List<ShuffleDataRef> live = inventory.getLiveDataOnWorker("w1");
        assertThat(live).isEmpty();
    }

    @Test
    void registerAndRetrieveLiveData() {
        var ref = new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 1024, false);
        inventory.register(ref);

        List<ShuffleDataRef> live = inventory.getLiveDataOnWorker("w1");
        assertThat(live).hasSize(1);
        assertThat(live.get(0).queryId()).isEqualTo("q1");
    }

    @Test
    void consumedDataNotReturnedAsLive() {
        var ref = new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 1024, false);
        inventory.register(ref);
        inventory.markConsumed("q1", 1, "w1");

        List<ShuffleDataRef> live = inventory.getLiveDataOnWorker("w1");
        assertThat(live).isEmpty();
    }

    @Test
    void removeQueryCleansUpAllRefs() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 500, false));
        inventory.register(new ShuffleDataRef("q1", 2, "w1",
                ShuffleRetentionPolicy.DETACHED, 500, false));
        inventory.register(new ShuffleDataRef("q2", 1, "w1",
                ShuffleRetentionPolicy.PERSISTENT, 500, false));

        inventory.removeQuery("q1");

        assertThat(inventory.size()).isEqualTo(1);
        List<ShuffleDataRef> live = inventory.getLiveDataOnWorker("w1");
        assertThat(live).hasSize(1);
        assertThat(live.get(0).queryId()).isEqualTo("q2");
    }

    @Test
    void getLiveDataFiltersByWorker() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 500, false));
        inventory.register(new ShuffleDataRef("q1", 2, "w2",
                ShuffleRetentionPolicy.EPHEMERAL, 500, false));

        assertThat(inventory.getLiveDataOnWorker("w1")).hasSize(1);
        assertThat(inventory.getLiveDataOnWorker("w2")).hasSize(1);
        assertThat(inventory.getLiveDataOnWorker("w3")).isEmpty();
    }

    @Test
    void emptyUnassembledInventoryCannotAuthorizeRemoval() {
        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment).isEqualTo(DecommissionAssessment.UNKNOWN);
        assertThat(assessment.isSafe()).isFalse();
        assertThat(assessment.isSafeWithDrain()).isFalse();
        assertThat(assessment.requiresReExecution()).isFalse();
    }

    @Test
    void storageClassificationAndConsumedFlagsCannotSupplyMissingAuthority() {
        for (ShuffleRetentionPolicy retention : ShuffleRetentionPolicy.values()) {
            for (boolean consumed : List.of(false, true)) {
                inventory.register(new ShuffleDataRef("q", 0, "w", retention, 1, consumed));
                assertThat(inventory.assessDecommission("w")).isEqualTo(DecommissionAssessment.UNKNOWN);
                inventory.markConsumed("q", 0, "w");
                assertThat(inventory.assessDecommission("w")).isEqualTo(DecommissionAssessment.UNKNOWN);
                inventory.removeQuery("q");
                assertThat(inventory.assessDecommission("w")).isEqualTo(DecommissionAssessment.UNKNOWN);
            }
        }
    }
}
