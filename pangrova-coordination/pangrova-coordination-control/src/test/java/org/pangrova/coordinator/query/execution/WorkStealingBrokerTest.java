package org.pangrova.coordinator.query.execution;

import org.pangrova.coordinator.query.execution.split.WorkStealingBroker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link WorkStealingBroker}: work-stealing protocol for split assignment.
 */
class WorkStealingBrokerTest {

    @Test
    void noStealWhenNoBusyWorkers() {
        WorkStealingBroker broker = new WorkStealingBroker();
        broker.registerWorkerSplits("q1", 0, "w1", 1);

        int stolen = broker.requestSteal("q1", 0, "w2");
        assertThat(stolen).isZero();
    }

    @Test
    void stealFromBusiestWorker() {
        WorkStealingBroker broker = new WorkStealingBroker();
        broker.registerWorkerSplits("q1", 0, "w1", 10);
        broker.registerWorkerSplits("q1", 0, "w2", 4);

        int stolen = broker.requestSteal("q1", 0, "w3");
        assertThat(stolen).isEqualTo(5); // half of w1's 10
        assertThat(broker.totalSteals()).isEqualTo(5);
    }

    @Test
    void noStealBelowThreshold() {
        WorkStealingBroker broker = new WorkStealingBroker();
        broker.registerWorkerSplits("q1", 0, "w1", 1);

        int stolen = broker.requestSteal("q1", 0, "w2");
        assertThat(stolen).isZero();
    }

    @Test
    void completionDecrementsAndAffectsSteal() {
        WorkStealingBroker broker = new WorkStealingBroker();
        broker.registerWorkerSplits("q1", 0, "w1", 4);

        broker.onSplitCompleted("q1", 0, "w1");
        broker.onSplitCompleted("q1", 0, "w1");
        broker.onSplitCompleted("q1", 0, "w1");
        // now w1 has 1 pending - below threshold
        int stolen = broker.requestSteal("q1", 0, "w2");
        assertThat(stolen).isZero();
    }

    @Test
    void cleanupRemovesQueryEntries() {
        WorkStealingBroker broker = new WorkStealingBroker();
        broker.registerWorkerSplits("q1", 0, "w1", 10);
        broker.cleanup("q1");

        int stolen = broker.requestSteal("q1", 0, "w2");
        assertThat(stolen).isZero();
    }

    @Test
    void thiefCannotStealFromSelf() {
        WorkStealingBroker broker = new WorkStealingBroker();
        broker.registerWorkerSplits("q1", 0, "w1", 10);

        int stolen = broker.requestSteal("q1", 0, "w1");
        assertThat(stolen).isZero();
    }
}
