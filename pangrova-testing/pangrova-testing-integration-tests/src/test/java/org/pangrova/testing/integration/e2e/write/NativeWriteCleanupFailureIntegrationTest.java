package org.pangrova.testing.integration.e2e.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.testing.integration.e2e.write.NativeWriteBatchStreams.InvalidFrame;
import org.pangrova.worker.exception.NativeRuntimeException;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Failure contracts backed by authenticated deployment, actual SDK writers and owned filesystem faults. */
class NativeWriteCleanupFailureIntegrationTest extends NativeWriteLifecycleTestBase {
    @ParameterizedTest(name = "{0}, direct={1}")
    @MethodSource("malformedFrames")
    void rejectsMalformedFramesBeforeCallingSink(InvalidFrame frame, boolean direct) throws Exception {
        try (var fixture = lifecycle()) {
            var opened = fixture.open(newTable());
            Throwable failure = catchThrowable(() -> fixture.executor().appendDirectBatch(opened.handle(),
                    NativeWriteBatchStreams.range(frame.bytes(), direct), 1, 1));
            assertDecodeFailure(failure);
            assertAborted(fixture, opened);
        }
    }

    static Stream<Arguments> malformedFrames() throws IOException {
        return NativeWriteBatchStreams.malformed().flatMap(frame ->
                Stream.of(Arguments.of(frame, false), Arguments.of(frame, true)));
    }

    @ParameterizedTest(name = "rows={0}, columns={1}, direct={2}")
    @MethodSource("dimensions")
    void validatesAdvertisedDimensionsWithoutPublishingAnyCommit(int rows, int columns, boolean direct)
            throws Exception {
        byte[] bytes = NativeWriteBatchStreams.ignoredAndId(8, 7);
        try (var fixture = lifecycle()) {
            var opened = fixture.open(newTable());
            assertDecodeFailure(catchThrowable(() -> fixture.executor().appendDirectBatch(opened.handle(),
                    NativeWriteBatchStreams.range(bytes, direct), rows, columns)));
            assertAborted(fixture, opened);
        }
    }

    static Stream<Arguments> dimensions() {
        return Stream.of(new int[] {-1, 2}, new int[] {Integer.MIN_VALUE, 2},
                new int[] {1, -1}, new int[] {1, Integer.MIN_VALUE}, new int[] {0, 2}, new int[] {2, 2},
                new int[] {Integer.MAX_VALUE, 2}, new int[] {1, 0}, new int[] {1, 1},
                new int[] {1, 3}, new int[] {1, Integer.MAX_VALUE})
                .flatMap(size -> Stream.of(Arguments.of(size[0], size[1], false),
                        Arguments.of(size[0], size[1], true)));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void rawEmptyOrNullPayloadCannotFinishEvenForZeroRows(boolean direct, boolean nullPayload) throws Exception {
        try (var fixture = lifecycle()) {
            var opened = fixture.open(newTable());
            assertDecodeFailure(catchThrowable(() -> fixture.executor().appendDirectBatch(opened.handle(),
                    NativeWriteBatchStreams.range(nullPayload ? null : new byte[0], direct), 0, 1)));
            assertAborted(fixture, opened);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void decoderExceptionPreservesActualDiscardFailureAndClosedFileUntilRetry(boolean direct) throws Exception {
        try (var fixture = lifecycle()) {
            var opened = fixture.open(naturalFlushTable("BIGINT"));
            Path owned = fixture.appendAndFindOwnedFile(opened, 7);
            byte[] bytes = NativeWriteBatchStreams.undecodable();
            ByteBuffer input = NativeWriteBatchStreams.range(bytes, direct).asReadOnlyBuffer();
            input.mark();
            int position = input.position();
            int limit = input.limit();
            try (var barrier = new NativeWriteLifecycleFixture.DeletionBarrier(owned)) {
                Throwable original = catchThrowable(() -> fixture.executor().appendDirectBatch(
                        opened.handle(), input, 1, 1));
                assertDecodeFailure(original);
                assertThat(original.getSuppressed()).singleElement().isInstanceOf(RuntimeException.class);
                assertThat(input.position()).isEqualTo(position);
                assertThat(input.limit()).isEqualTo(limit);
                assertThat(input.reset().position()).isEqualTo(position);
                assertRetained(fixture, opened);
                assertThat(readOwnedFile(opened, barrier.retainedFile())).singleElement()
                        .satisfies(row -> assertThat(row.getField("id")).isEqualTo(7L));
                assertThatThrownBy(() -> fixture.executor().releaseTask(opened.taskId()))
                        .isInstanceOf(RuntimeException.class);
                assertRetained(fixture, opened);
            }
            fixture.executor().abortWrite(opened.handle());
            assertAborted(fixture, opened);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void realWriterFailureStopsBeforeRemainingRowsAndPreservesCleanupOwner(boolean direct) throws Exception {
        String table = naturalFlushTable("DECIMAL(6,0)");
        try (var fixture = lifecycle()) {
            var opened = fixture.open(table);
            fixture.executor().appendDirectBatch(opened.handle(), NativeWriteBatchStreams.range(
                    NativeWriteBatchStreams.decimals(new BigDecimal("7.00")), direct), 1, 1);
            Path owned = fixture.findOwnedFile(opened);
            try (var barrier = new NativeWriteLifecycleFixture.DeletionBarrier(owned)) {
                byte[] bytes = NativeWriteBatchStreams.decimals(new BigDecimal("1.00"),
                        new BigDecimal("7.01"), new BigDecimal("100000.00"));
                Throwable original = catchThrowable(() -> fixture.executor().appendDirectBatch(opened.handle(),
                        NativeWriteBatchStreams.range(bytes, direct), 3, 1));
                assertThat(original).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scale");
                assertThat(Stream.of(original.getStackTrace()).map(StackTraceElement::getClassName))
                        .anyMatch(name -> name.startsWith("org.apache.iceberg."));
                assertThat(original.getSuppressed()).singleElement().isInstanceOf(RuntimeException.class);
                assertRetained(fixture, opened);
                // A failed writer need not finalize a readable Parquet footer; its uncommitted file
                // must remain owned until abort and the third row must not replace the scale error.
                assertThat(Files.size(barrier.retainedFile())).isPositive();
            }
            fixture.executor().abortWrite(opened.handle());
            assertAborted(fixture, opened);
            assertThat(sqlRunner.execute("SELECT COUNT(*) AS remaining FROM " + qualified(table)))
                    .singleElement().satisfies(row -> assertThat(row.getLong("remaining")).isZero());
        }
    }

    @Test
    void failedGlobalClosePreservesActualCatalogLeaseAndCanCompleteAfterStorageRecovery() throws Exception {
        try (var fixture = lifecycle()) {
            var opened = fixture.open(naturalFlushTable("BIGINT"));
            Path owned = fixture.appendAndFindOwnedFile(opened, 7);
            try (var barrier = new NativeWriteLifecycleFixture.DeletionBarrier(owned)) {
                assertThatThrownBy(fixture.executor()::close).isInstanceOf(RuntimeException.class);
                assertRetained(fixture, opened);
                assertThatThrownBy(() -> fixture.executor().beginWrite(opened.descriptor()))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
                assertThat(readOwnedFile(opened, barrier.retainedFile())).hasSize(1);
            }
            fixture.executor().releaseTask(opened.taskId());
            assertThat(fixture.executor().activeSessionCount()).isZero();
            assertThat(owned).doesNotExist();
            fixture.executor().close();
            fixture.executor().close();
        }
    }

    private static void assertDecodeFailure(Throwable failure) {
        assertThat(failure).isInstanceOf(NativeRuntimeException.class).hasCauseInstanceOf(IOException.class);
        assertThat(((NativeRuntimeException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.NATIVE_DATA_INTERFACE_ERROR);
    }

    private String naturalFlushTable(String type) {
        String table = "cleanup_" + UUID.randomUUID().toString().replace("-", "");
        sqlRunner.executeDdl("CREATE TABLE " + qualified(table) + " (id " + type + ")"
                + NativeWriteLifecycleFixture.naturalFlushTableOptions());
        return table;
    }

    private static void assertRetained(NativeWriteLifecycleFixture fixture,
            NativeWriteLifecycleFixture.OpenSession opened) {
        assertThat(fixture.executor().activeSessionCount()).isEqualTo(1);
        assertThat(fixture.cachedCatalogRevisionCount()).isPositive();
        assertThatThrownBy(() -> fixture.executor().commitWrite(opened.handle()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Write session cannot finish after failure or abort");
        assertThatThrownBy(() -> fixture.append(opened.handle(), 9))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("stopped");
        assertThat(fixture.executor().drainCommitMetadataForTask(opened.taskId())).isEmpty();
    }

    private static void assertAborted(NativeWriteLifecycleFixture fixture,
            NativeWriteLifecycleFixture.OpenSession opened) throws IOException {
        assertThat(fixture.executor().activeSessionCount()).isZero();
        assertThat(NativeWriteLifecycleFixture.files(opened))
                .containsExactlyInAnyOrderElementsOf(opened.existingFiles());
        assertThat(fixture.executor().drainCommitMetadataForTask(opened.taskId())).isEmpty();
        assertThatThrownBy(() -> fixture.executor().commitWrite(opened.handle()))
                .isInstanceOf(NativeRuntimeException.class);
        assertThatThrownBy(() -> fixture.append(opened.handle(), 9)).isInstanceOf(NativeRuntimeException.class);
        fixture.executor().abortWrite(opened.handle());
        fixture.executor().closeAllSessions();
        assertThat(NativeWriteLifecycleFixture.files(opened))
                .containsExactlyInAnyOrderElementsOf(opened.existingFiles());
    }

    private static List<Record> readOwnedFile(NativeWriteLifecycleFixture.OpenSession opened, Path path)
            throws IOException {
        var schema = SchemaParser.fromJson(opened.outputHandle().schemaJson());
        var input = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), new Configuration());
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = Parquet.read(input).project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema)).build()) {
            records.forEach(row -> rows.add(row.copy()));
        }
        return rows;
    }
}
