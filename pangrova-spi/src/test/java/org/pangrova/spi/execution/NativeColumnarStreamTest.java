package org.pangrova.spi.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Unit tests for the {@link NativeColumnarStream} interface contract.
 * Uses a stub implementation to verify default methods and lifecycle behavior.
 */
class NativeColumnarStreamTest {

    static class StubNativeColumnarStream implements NativeColumnarStream {

        private static final long SCHEMA_PTR = 0xDEAD_BEEF_0000L;
        private final List<Long> batches;
        private final List<Long> releasedBatches = new ArrayList<>();
        private int index = 0;
        private boolean closed = false;

        StubNativeColumnarStream(List<Long> batchPointers) {
            this.batches = new ArrayList<>(batchPointers);
        }

        @Override
        public long getSchema() {
            if (closed) throw new IllegalStateException("Stream is closed");
            return SCHEMA_PTR;
        }

        @Override
        public boolean hasNext() {
            if (closed) throw new IllegalStateException("Stream is closed");
            return index < batches.size();
        }

        @Override
        public long nextBatch() {
            if (closed) throw new IllegalStateException("Stream is closed");
            if (index >= batches.size()) throw new NoSuchElementException("No more batches");
            return batches.get(index++);
        }

        @Override
        public void releaseBatch(long batchPtr) {
            releasedBatches.add(batchPtr);
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() { return closed; }
        List<Long> releasedBatches() { return releasedBatches; }
    }

    @Test
    void defaultInteropModeIsNativeCStream() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of());
        assertThat(stream.interopMode()).isEqualTo(InteropMode.NATIVE_C_STREAM);
    }

    @Test
    void getSchemaReturnsPointer() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of());
        assertThat(stream.getSchema()).isEqualTo(0xDEAD_BEEF_0000L);
    }

    @Test
    void emptyStreamHasNoBatches() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of());
        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    void singleBatchStream() {
        long batchPtr = 0x1234_0000L;
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of(batchPtr));

        assertThat(stream.hasNext()).isTrue();
        assertThat(stream.nextBatch()).isEqualTo(batchPtr);
        assertThat(stream.hasNext()).isFalse();
    }

    @Test
    void multipleBatchesStreamInOrder() {
        List<Long> ptrs = List.of(0x1000L, 0x2000L, 0x3000L);
        StubNativeColumnarStream stream = new StubNativeColumnarStream(ptrs);

        List<Long> received = new ArrayList<>();
        while (stream.hasNext()) {
            received.add(stream.nextBatch());
        }
        assertThat(received).containsExactlyElementsOf(ptrs);
    }

    @Test
    void nextBatchThrowsWhenExhausted() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of(0x1000L));
        stream.nextBatch();

        assertThatThrownBy(stream::nextBatch).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void releaseBatchTracksPointers() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of(0x1000L, 0x2000L));

        long b1 = stream.nextBatch();
        stream.releaseBatch(b1);
        long b2 = stream.nextBatch();
        stream.releaseBatch(b2);

        assertThat(stream.releasedBatches()).containsExactly(0x1000L, 0x2000L);
    }

    @Test
    void closeMarksStreamClosed() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of(0x1000L));
        stream.close();
        assertThat(stream.isClosed()).isTrue();
    }

    @Test
    void operationsAfterCloseThrow() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of(0x1000L));
        stream.close();

        assertThatThrownBy(stream::getSchema).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(stream::hasNext).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(stream::nextBatch).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeIsIdempotent() {
        StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of());
        stream.close();
        stream.close();
        assertThat(stream.isClosed()).isTrue();
    }

    @Test
    void nativeExecutionExceptionWithMessage() {
        NativeColumnarStream.NativeExecutionException ex =
                new NativeColumnarStream.NativeExecutionException("engine OOM");
        assertThat(ex.getMessage()).isEqualTo("engine OOM");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void nativeExecutionExceptionWithCause() {
        RuntimeException cause = new RuntimeException("underlying");
        NativeColumnarStream.NativeExecutionException ex =
                new NativeColumnarStream.NativeExecutionException("wrap", cause);
        assertThat(ex.getMessage()).isEqualTo("wrap");
        assertThat(ex.getCause()).isSameAs(cause);
    }

    @Test
    void streamIsAutoCloseable() {
        try (StubNativeColumnarStream stream = new StubNativeColumnarStream(List.of(0x1000L))) {
            assertThat(stream.hasNext()).isTrue();
            stream.nextBatch();
        }
    }
}
