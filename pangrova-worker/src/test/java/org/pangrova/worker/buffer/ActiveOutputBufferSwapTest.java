package org.pangrova.worker.buffer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies {@link ActiveOutputBuffer#swap} atomically replaces the active buffer,
 * closes the previous buffer, and resets the queryId binding.
 */
class ActiveOutputBufferSwapTest {

    @Test
    void swapClosesPreviousBufferAndReturnsIt() {
        OutputBufferManager first = new OutputBufferManager();
        ActiveOutputBuffer active = new ActiveOutputBuffer(first);

        OutputBufferManager second = new OutputBufferManager();
        OutputBufferManager returned = active.swap(second);

        assertThat(returned).isSameAs(first);
        assertThat(first.isClosed()).isTrue();
        assertThat(active.get()).isSameAs(second);
    }

    @Test
    void swapResetsQueryIdToNull() {
        OutputBufferManager first = new OutputBufferManager();
        ActiveOutputBuffer active = new ActiveOutputBuffer(first);

        OutputBufferManager second = new OutputBufferManager();
        active.swap(second);

        assertThat(active.queryId()).isNull();
    }

    @Test
    void swapProvidesFreshBufferWithZeroBatches() throws InterruptedException {
        OutputBufferManager first = new OutputBufferManager();
        first.addBatch(new byte[]{0x01});
        first.addBatch(new byte[]{0x02});
        ActiveOutputBuffer active = new ActiveOutputBuffer(first);

        OutputBufferManager second = new OutputBufferManager();
        active.swap(second);

        assertThat(active.get().getBufferedCount()).isZero();
        assertThat(active.get().isClosed()).isFalse();
    }
}
