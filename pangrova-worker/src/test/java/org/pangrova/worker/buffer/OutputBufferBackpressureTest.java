package org.pangrova.worker.buffer;

import org.pangrova.resilience.backpressure.BackpressureController;
import org.pangrova.resilience.backpressure.BackpressureController.Action;
import org.pangrova.resilience.backpressure.BackpressureController.BackpressureResult;
import org.pangrova.worker.execution.SimulatedTaskExecutor;
import org.pangrova.worker.CanonicalTaskRequestTestFixture;
import org.pangrova.protocol.v2.TaskKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests OutputBufferBackpressure wiring between ActiveOutputBuffer and BackpressureController.
 * Uses real OutputBufferManager, BackpressureController, and SimulatedTaskExecutor — no mocks.
 */
class OutputBufferBackpressureTest {

    @Test
    void emptyBuffer_proceedsNormally() {
        OutputBufferManager buffer = new OutputBufferManager(10);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        BackpressureController controller = new BackpressureController(10, 8, 5);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(0);
        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, executor);

        long handle = executor.createTask(canonicalTaskBytes());
        BackpressureResult result = bp.evaluate(handle);

        assertThat(result.action()).isEqualTo(Action.PROCEED);
        assertThat(bp.hasCapacity()).isTrue();
    }

    @Test
    void fullBuffer_rejectsAndSignalsPause() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(3);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        // maxPending=3, highWaterMark=2, lowWaterMark=1
        BackpressureController controller = new BackpressureController(3, 2, 1);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(0);
        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, executor);

        long handle = executor.createTask(canonicalTaskBytes());

        // Fill buffer to capacity
        buffer.addBatch(new byte[]{1});
        buffer.addBatch(new byte[]{2});
        buffer.addBatch(new byte[]{3});

        BackpressureResult result = bp.evaluate(handle);
        assertThat(result.action()).isEqualTo(Action.REJECT);
        assertThat(bp.hasCapacity()).isFalse();
    }

    @Test
    void highWaterMark_slowsDown() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(10);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        // maxPending=10, highWaterMark=5, lowWaterMark=3
        BackpressureController controller = new BackpressureController(10, 5, 3);
        SimulatedTaskExecutor executor = new SimulatedTaskExecutor(0);
        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, executor);

        long handle = executor.createTask(canonicalTaskBytes());

        // Add 6 batches — above highWaterMark(5)
        for (int i = 0; i < 6; i++) {
            buffer.addBatch(new byte[]{(byte) i});
        }

        BackpressureResult result = bp.evaluate(handle);
        assertThat(result.action()).isEqualTo(Action.SLOW_DOWN);
        assertThat(result.delay().toMillis()).isGreaterThan(0);
    }

    @Test
    void capacityMethods_reflectBufferState() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(5);
        assertThat(buffer.isFull()).isFalse();
        assertThat(buffer.getRemainingCapacity()).isEqualTo(5);
        assertThat(buffer.getMaxBatches()).isEqualTo(5);

        buffer.addBatch(new byte[]{1});
        assertThat(buffer.getBufferedCount()).isEqualTo(1);
        assertThat(buffer.getRemainingCapacity()).isEqualTo(4);

        // Fill to capacity
        for (int i = 0; i < 4; i++) {
            buffer.addBatch(new byte[]{(byte) i});
        }
        assertThat(buffer.isFull()).isTrue();
        assertThat(buffer.getRemainingCapacity()).isEqualTo(0);
    }

    private static byte[] canonicalTaskBytes() {
        return CanonicalTaskRequestTestFixture.builder(
                "backpressure-task", "backpressure-query", 0, 0,
                TaskKind.TASK_KIND_READ, new byte[]{1}).build().toByteArray();
    }
}
