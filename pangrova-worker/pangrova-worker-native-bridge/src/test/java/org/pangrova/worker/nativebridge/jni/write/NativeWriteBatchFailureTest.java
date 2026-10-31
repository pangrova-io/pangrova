package org.pangrova.worker.nativebridge.jni.write;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.worker.nativebridge.jni.NativeRuntimeException;
import org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.BatchSink;
import org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.RowSink;
import org.pangrova.worker.nativebridge.jni.write.WriteBatchFrames.InvalidFrame;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.append;
import static org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.descriptor;
import static org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.directRange;
import static org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.executor;
import static org.pangrova.worker.nativebridge.jni.write.WriteBatchFixtures.integers;

class NativeWriteBatchFailureTest {
    @ParameterizedTest(name = "{0}, direct={1}")
    @MethodSource("malformedFrames")
    void rejectsMalformedFramesBeforeCallingSink(InvalidFrame frame, boolean direct) {
        BatchSink sink = new BatchSink(batch -> { throw new AssertionError("Malformed stream reached sink"); });
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            Throwable failure = catchThrowable(() -> append(writer, handle, frame.bytes(), direct, 1, 1));
            assertDecodeFailure(failure);
            assertThat(sink.batchCalls).isZero();
            assertThat(sink.pageCalls).isZero();
            assertAborted(writer, handle, sink);
        }
    }

    static Stream<Arguments> malformedFrames() throws IOException {
        return WriteBatchFrames.malformed().flatMap(frame ->
                Stream.of(Arguments.of(frame, false), Arguments.of(frame, true)));
    }

    @ParameterizedTest(name = "rows={0}, sourceColumns={1}, direct={2}")
    @MethodSource("dimensions")
    void validatesNonnegativeAdvertisedSourceDimensionsBeforeProjection(int rows, int columns, boolean direct)
            throws Exception {
        BatchSink sink = new BatchSink(batch -> { throw new AssertionError("Invalid dimensions reached sink"); });
        byte[] data = integers(List.of("ignored", "id"), new Integer[] {8, 7});
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            Throwable failure = catchThrowable(() -> append(writer, handle, data, direct, rows, columns));
            assertDecodeFailure(failure);
            assertThat(sink.batchCalls).isZero();
            assertAborted(writer, handle, sink);
        }
    }

    static Stream<Arguments> dimensions() {
        return Stream.of(new int[] {-1, 2}, new int[] {Integer.MIN_VALUE, 2},
                        new int[] {1, -1}, new int[] {1, Integer.MIN_VALUE}, new int[] {0, 2}, new int[] {2, 2},
                        new int[] {Integer.MAX_VALUE, 2}, new int[] {1, 0}, new int[] {1, 1},
                        new int[] {1, 3}, new int[] {1, Integer.MAX_VALUE})
                .flatMap(size -> Stream.of(Arguments.of(size[0], size[1], false), Arguments.of(size[0], size[1], true)));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void rawEmptyOrNullPayloadIsNotAValidZeroRowBatch(boolean direct, boolean nullPayload) {
        BatchSink sink = new BatchSink(batch -> { throw new AssertionError("Missing batch reached sink"); });
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            byte[] data = nullPayload ? null : new byte[0];
            assertDecodeFailure(catchThrowable(() -> append(writer, handle, data, direct, 0, 1)));
            assertThat(sink.batchCalls).isZero();
            assertAborted(writer, handle, sink);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void decoderExceptionAbortsAndPreservesCleanupFailures(boolean direct) throws Exception {
        BatchSink sink = new BatchSink(batch -> { throw new AssertionError("Undecodable batch reached sink"); });
        sink.abortFailure = new IllegalStateException("abort failed");
        sink.closeFailure = new IllegalArgumentException("close failed");
        byte[] data = WriteBatchFrames.undecodable();
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            Throwable failure = catchThrowable(() -> append(writer, handle, data, direct, 1, 1));
            assertDecodeFailure(failure);
            assertThat(failure.getSuppressed()).containsExactly(sink.abortFailure, sink.closeFailure);
            assertThat(sink.batchCalls).isZero();
            assertCleanupRetainedUntilRetry(writer, handle, sink);
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false,false", "false,false,true", "false,true,false", "false,true,true",
            "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
    void sinkFailureOrErrorAbortsAndClosesWithoutReplacingTheOriginal(boolean direct, boolean columnar,
                                                                   boolean error) throws Exception {
        Throwable expected = error ? new AssertionError("write failed") : new IllegalStateException("write failed");
        RowSink sink = columnar ? new BatchSink(batch -> {
            assertThat(batch.value(0, 0)).isEqualTo(7);
            raise(expected);
        }) : new RowSink(page -> {
            assertThat(page.getPositionCount()).isEqualTo(1);
            raise(expected);
        });
        sink.abortFailure = new IllegalArgumentException("abort failed");
        sink.closeFailure = new IllegalStateException("close failed");
        byte[] data = integers(List.of("id"), new Integer[] {7}, new Integer[] {8});
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            Throwable actual = catchThrowable(() -> append(writer, handle, data, direct, 2, 1));
            assertThat(actual).isSameAs(expected);
            assertThat(actual.getSuppressed()).containsExactly(sink.abortFailure, sink.closeFailure);
            if (sink instanceof BatchSink batchSink) {
                assertThat(batchSink.batchCalls).isEqualTo(1);
                NativeWriteBatchTest.assertClosed(batchSink.borrowed);
            } else {
                assertThat(sink.pageCalls).isEqualTo(1);
            }
            assertCleanupRetainedUntilRetry(writer, handle, sink);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rowOnlyFailureStopsBeforeRemainingRowsAndDiscardsEarlierWrites(boolean direct) throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RowSink sink = new RowSink(page -> {
            assertThat(page.getPositionCount()).isEqualTo(1);
            int row = calls.incrementAndGet();
            assertThat(NativeWriteBatchTest.raw(page, 0, "id")).isEqualTo(row);
            if (row == 2) {
                throw new IllegalStateException("second row failed");
            }
        });
        byte[] data = integers(List.of("id"), new Integer[] {1}, new Integer[] {2}, new Integer[] {3});
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            assertThatThrownBy(() -> append(writer, handle, data, direct, 3, 1))
                    .isInstanceOf(IllegalStateException.class).hasMessage("second row failed");
            assertThat(sink.pageCalls).isEqualTo(2);
            assertAborted(writer, handle, sink);
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void cellConversionFailureAlsoInvalidatesBorrowedBatchAndSession(boolean direct, boolean columnar) throws Exception {
        RowSink sink = columnar ? new BatchSink(batch -> batch.value(0, 0))
                : new RowSink(page -> { throw new AssertionError("Invalid map reached row sink"); });
        byte[] data = WriteBatchFixtures.complex(true);
        try (NativeWriteExecutor writer = executor(sink, "attributes")) {
            long handle = writer.beginWrite(descriptor("attributes"));
            assertThatThrownBy(() -> append(writer, handle, data, direct, 2, 6))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Map keys");
            if (sink instanceof BatchSink batchSink) {
                NativeWriteBatchTest.assertClosed(batchSink.borrowed);
            } else {
                assertThat(sink.pageCalls).isZero();
            }
            assertAborted(writer, handle, sink);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"framing", "dimensions", "decode", "write"})
    void failureDoesNotChangeCallerBufferRangeOrMark(String phase) throws Exception {
        byte[] data = phase.equals("decode") ? WriteBatchFrames.undecodable()
                : integers(List.of("id"), new Integer[] {7});
        if (phase.equals("framing")) {
            data[0] = 0;
        }
        ByteBuffer input = directRange(data).asReadOnlyBuffer();
        input.mark();
        int position = input.position();
        int limit = input.limit();
        BatchSink sink = new BatchSink(batch -> { throw new IllegalStateException("write failed"); });
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            assertThatThrownBy(() -> writer.appendDirectBatch(handle, input, phase.equals("dimensions") ? 2 : 1, 1))
                    .isInstanceOf(RuntimeException.class);
            assertThat(input.position()).isEqualTo(position);
            assertThat(input.limit()).isEqualTo(limit);
            assertThat(input.reset().position()).isEqualTo(position);
            assertAborted(writer, handle, sink);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "dimensions", "decode", "write"})
    void readerAndPerBatchAllocatorAreReleasedEvenOnFailure(String phase) throws Exception {
        byte[] data = phase.equals("decode") ? WriteBatchFrames.undecodable()
                : integers(List.of("id"), new Integer[] {7});
        try (var parent = new RootAllocator(1024 * 1024)) {
            for (int attempt = 0; attempt < 4; attempt++) {
                try (BufferAllocator child = parent.newChildAllocator("write-batch", 0, 1024 * 1024)) {
                    Throwable failure = catchThrowable(() -> {
                        try (ColumnarBatch batch = ColumnarBatch.open(directRange(data), child,
                                phase.equals("dimensions") ? 2 : 1, 1)) {
                            assertThat(child.getAllocatedMemory()).isPositive();
                            assertThat(batch.value(0, 0)).isEqualTo(7);
                            if (phase.equals("write")) {
                                throw new IllegalStateException("write failed");
                            }
                        }
                    });
                    assertResourceOutcome(phase, failure);
                    assertThat(child.getAllocatedMemory()).isZero();
                }
                assertThat(parent.getAllocatedMemory()).isZero();
                assertThat(parent.getChildAllocators()).isEmpty();
            }
        }
    }

    private static void assertResourceOutcome(String phase, Throwable failure) {
        switch (phase) {
            case "success" -> assertThat(failure).isNull();
            case "write" -> assertThat(failure).isInstanceOf(IllegalStateException.class).hasMessage("write failed");
            default -> assertThat(failure).isInstanceOf(IOException.class);
        }
    }

    private static void assertDecodeFailure(Throwable failure) {
        assertThat(failure).isInstanceOf(NativeRuntimeException.class).hasCauseInstanceOf(IOException.class);
        assertThat(((NativeRuntimeException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.NATIVE_DATA_INTERFACE_ERROR);
    }

    private static void assertAborted(NativeWriteExecutor writer, long handle, RowSink sink) {
        assertThat(writer.activeSessionCount()).isZero();
        assertThat(sink.events).containsExactly("abort", "close");
        assertThatThrownBy(() -> writer.commitWrite(handle)).isInstanceOfSatisfying(NativeRuntimeException.class,
                failure -> assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.NATIVE_INVALID_HANDLE));
        assertThatThrownBy(() -> writer.appendDirectBatch(handle, directRange(new byte[0]), 0, 0))
                .isInstanceOfSatisfying(NativeRuntimeException.class,
                        failure -> assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.NATIVE_INVALID_HANDLE));
        writer.abortWrite(handle);
        writer.closeAllSessions();
        assertThat(sink.events).containsExactly("abort", "close");
        assertThat(writer.drainCommitMetadataForTask("write-task")).isEmpty();
    }

    private static void assertCleanupRetainedUntilRetry(NativeWriteExecutor writer, long handle, RowSink sink) {
        assertThat(writer.activeSessionCount()).isEqualTo(1);
        assertThat(sink.events).containsExactly("abort", "close");
        assertThat(writer.drainCommitMetadataForTask("write-task")).isEmpty();

        sink.abortFailure = null;
        sink.closeFailure = null;
        writer.abortWrite(handle);

        assertThat(writer.activeSessionCount()).isZero();
        assertThat(sink.events).containsExactly("abort", "close", "abort", "close");
        assertThatThrownBy(() -> writer.commitWrite(handle)).isInstanceOfSatisfying(NativeRuntimeException.class,
                failure -> assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.NATIVE_INVALID_HANDLE));
        assertThat(writer.drainCommitMetadataForTask("write-task")).isEmpty();
    }

    private static void raise(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        throw (RuntimeException) failure;
    }
}
