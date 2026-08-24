package org.pangrova.coordinator.query.execution.dispatch.shuffle;

import org.pangrova.shuffle.DecommissionAssessment;
import org.pangrova.shuffle.ShuffleDataRef;
import org.pangrova.shuffle.ShuffleRetentionPolicy;
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

    // ── assessDecommission ──

    @Test
    void assessDecommissionEmptyInventoryIsSafe() {
        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.isSafe()).isTrue();
        assertThat(assessment).isEqualTo(DecommissionAssessment.SAFE);
    }

    @Test
    void assessDecommissionWithEphemeralDataRequiresReExecution() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 2048, false));

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.requiresReExecution()).isTrue();
        assertThat(assessment.ephemeralData()).hasSize(1);
        assertThat(assessment.ephemeralBytes()).isEqualTo(2048);
    }

    @Test
    void assessDecommissionWithOnlyPersistentDataIsSafe() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.PERSISTENT, 4096, false));

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.isSafe()).isTrue();
    }

    @Test
    void assessDecommissionWithOnlyDetachedDataIsSafeWithDrain() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.DETACHED, 4096, false));

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.isSafeWithDrain()).isTrue();
        assertThat(assessment.isSafe()).isFalse();
    }

    @Test
    void assessDecommissionMixedDetachedAndPersistentIsSafeWithDrain() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.DETACHED, 1024, false));
        inventory.register(new ShuffleDataRef("q1", 2, "w1",
                ShuffleRetentionPolicy.PERSISTENT, 2048, false));

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.readiness())
                .isEqualTo(DecommissionAssessment.Readiness.SAFE_WITH_DRAIN);
    }

    @Test
    void assessDecommissionMixedEphemeralAndPersistentRequiresReExecution() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 512, false));
        inventory.register(new ShuffleDataRef("q1", 2, "w1",
                ShuffleRetentionPolicy.PERSISTENT, 2048, false));

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.requiresReExecution()).isTrue();
        assertThat(assessment.ephemeralData()).hasSize(1);
        assertThat(assessment.ephemeralBytes()).isEqualTo(512);
    }

    @Test
    void assessDecommissionConsumedDataDoesNotAffectAssessment() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 2048, false));
        inventory.markConsumed("q1", 1, "w1");

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.isSafe()).isTrue();
    }

    @Test
    void multipleEphemeralRefsAggregateSizeCorrectly() {
        inventory.register(new ShuffleDataRef("q1", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 1000, false));
        inventory.register(new ShuffleDataRef("q2", 1, "w1",
                ShuffleRetentionPolicy.EPHEMERAL, 2000, false));

        var assessment = inventory.assessDecommission("w1");
        assertThat(assessment.requiresReExecution()).isTrue();
        assertThat(assessment.ephemeralData()).hasSize(2);
        assertThat(assessment.ephemeralBytes()).isEqualTo(3000);
    }
}
