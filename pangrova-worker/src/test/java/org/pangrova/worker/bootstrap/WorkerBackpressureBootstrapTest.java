package org.pangrova.worker.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.resilience.backpressure.BackpressureController;
import org.pangrova.resilience.backpressure.BackpressureController.Action;
import org.pangrova.worker.buffer.ActiveOutputBuffer;
import org.pangrova.worker.buffer.OutputBufferBackpressure;
import org.pangrova.worker.buffer.OutputBufferManager;
import org.pangrova.worker.execution.SimulatedTaskExecutor;
import org.junit.jupiter.api.Test;

/**
 * Verifies BackpressureController wiring in bootstrap context:
 * ActiveOutputBuffer → BackpressureController → TaskExecutor signal chain.
 */
class WorkerBackpressureBootstrapTest {

    @Test
    void backpressureWiringProceedsWhenBufferEmpty() {
        OutputBufferManager buffer = new OutputBufferManager(10);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        BackpressureController controller = new BackpressureController(10, 8, 5);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(0);
        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, executor);

        var result = bp.evaluate(1L);

        assertThat(result.action()).isEqualTo(Action.PROCEED);
        assertThat(bp.hasCapacity()).isTrue();
    }

    @Test
    void backpressureRejectsWhenBufferFull() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(3);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        BackpressureController controller = new BackpressureController(3, 2, 1);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(0);

        buffer.addBatch(new byte[]{1});
        buffer.addBatch(new byte[]{2});
        buffer.addBatch(new byte[]{3});

        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, executor);
        var result = bp.evaluate(1L);

        assertThat(result.action()).isEqualTo(Action.REJECT);
    }
}
