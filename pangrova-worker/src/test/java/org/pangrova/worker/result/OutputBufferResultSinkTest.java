package org.pangrova.worker.result;

import org.pangrova.worker.exception.BatchRejectedException;

import org.pangrova.worker.buffer.BackpressureController;
import org.pangrova.worker.buffer.ActiveOutputBuffer;
import org.pangrova.worker.buffer.OutputBufferBackpressure;
import org.pangrova.worker.buffer.OutputBufferManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputBufferResultSinkTest {

    @Test
    void onBatchDelegatesToBuffer() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(10);
        OutputBufferResultSink sink = new OutputBufferResultSink(buffer);

        byte[] batchData = {0x01, 0x02, 0x03, 0x04};
        sink.onBatch(batchData);

        byte[] polled = buffer.pollBatch(1000);
        assertArrayEquals(batchData, polled, "buffer should contain the batch data");
    }

    @Test
    void completeDelegatesToBufferClose() {
        OutputBufferManager buffer = new OutputBufferManager(10);
        OutputBufferResultSink sink = new OutputBufferResultSink(buffer);

        sink.complete();

        assertTrue(buffer.isClosed(), "buffer should be closed after complete()");
    }

    @Test
    void constructorRejectsNullBufferManager() {
        assertThatThrownBy(() -> new OutputBufferResultSink(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void onBatchThrowsWhenBufferFull() {
        // Buffer capacity = 1, fill it first
        OutputBufferManager buffer = new OutputBufferManager(1, 0) {
            @Override
            public void addBatch(byte[] batch) throws InterruptedException {
                throw new IllegalStateException(
                        "Output buffer full after " + BACKPRESSURE_TIMEOUT_MS + "ms timeout");
            }
        };
        OutputBufferResultSink sink = new OutputBufferResultSink(buffer);

        assertThatThrownBy(() -> sink.onBatch(new byte[]{0x01}))
                .isInstanceOf(BatchRejectedException.class)
                .hasMessageContaining("Output buffer full");
    }

    @Test
    void onBatchThrowsOnInterrupt() {
        OutputBufferManager buffer = new OutputBufferManager(1, 0) {
            @Override
            public void addBatch(byte[] batch) throws InterruptedException {
                throw new InterruptedException("simulated interrupt");
            }
        };
        OutputBufferResultSink sink = new OutputBufferResultSink(buffer);

        assertThatThrownBy(() -> sink.onBatch(new byte[]{0x01}))
                .isInstanceOf(BatchRejectedException.class);

        // Thread interrupt flag should be preserved
        assertThat(Thread.currentThread().isInterrupted()).isTrue();

        // Clear interrupt flag for test cleanup
        Thread.interrupted();
    }

    @Test
    void batchRejectedExceptionCarriesBatchSize() {
        OutputBufferManager buffer = new OutputBufferManager(1, 0) {
            @Override
            public void addBatch(byte[] batch) throws InterruptedException {
                throw new IllegalStateException(
                        "Output buffer full after " + BACKPRESSURE_TIMEOUT_MS + "ms timeout");
            }
        };
        OutputBufferResultSink sink = new OutputBufferResultSink(buffer);

        byte[] batch = new byte[1024];
        try {
            sink.onBatch(batch);
        } catch (BatchRejectedException e) {
            assertThat(e.getBatchSizeBytes()).isEqualTo(1024);
            return;
        }
        throw new AssertionError("Expected BatchRejectedException");
    }

    // --- Backpressure wiring tests (Task 26) ---

    @Test
    void backpressureRejectThrowsImmediately() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(10);
        // Controller: maxPending=5, high=4, low=2 — fill buffer to trigger REJECT
        BackpressureController controller = new BackpressureController(5, 4, 2);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, null);

        // Fill buffer to maxPending (5) to trigger REJECT on next evaluate
        for (int i = 0; i < 5; i++) {
            buffer.addBatch(new byte[]{(byte) i});
        }

        OutputBufferResultSink sink = new OutputBufferResultSink(buffer, bp);
        assertThatThrownBy(() -> sink.onBatch(new byte[]{0x01}))
                .isInstanceOf(BatchRejectedException.class);
    }

    @Test
    void backpressureProceedAllowsBatch() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(10);
        // Controller thresholds high enough that 0 pending = PROCEED
        BackpressureController controller = new BackpressureController(100, 80, 50);
        ActiveOutputBuffer active = new ActiveOutputBuffer(buffer);
        OutputBufferBackpressure bp = new OutputBufferBackpressure(active, controller, null);

        OutputBufferResultSink sink = new OutputBufferResultSink(buffer, bp);
        byte[] batchData = {0x01, 0x02};
        sink.onBatch(batchData);

        byte[] polled = buffer.pollBatch(1000);
        assertArrayEquals(batchData, polled);
    }

    @Test
    void nullBackpressureSkipsEvaluation() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(10);
        OutputBufferResultSink sink = new OutputBufferResultSink(buffer, null);

        byte[] batchData = {0x05, 0x06};
        sink.onBatch(batchData);

        byte[] polled = buffer.pollBatch(1000);
        assertArrayEquals(batchData, polled);
    }
}
