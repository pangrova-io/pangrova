package org.pangrova.worker.jni.write;

import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.worker.jni.write.WriteBatchFixtures.BatchSink;
import org.pangrova.worker.jni.write.WriteBatchFixtures.RowSink;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.protocol.substrait.SubstraitRelationLayout.physicalAlias;
import static org.pangrova.worker.jni.write.WriteBatchFixtures.append;
import static org.pangrova.worker.jni.write.WriteBatchFixtures.descriptor;
import static org.pangrova.worker.jni.write.WriteBatchFixtures.executor;
import static org.pangrova.worker.jni.write.WriteBatchFixtures.integers;

class WritePageProjectorTest {
    @ParameterizedTest(name = "{0}, direct={1}, columnar={2}")
    @MethodSource("projections")
    void projectsBeforeSynchronousSinkDispatch(Projection projection, boolean direct, boolean columnar) throws Exception {
        List<List<Object>> received = new ArrayList<>();
        RowSink sink = columnar ? new BatchSink(batch -> {
            assertThat(batch.columns()).containsExactlyElementsOf(projection.expectedColumns());
            for (int row = 0; row < batch.rowCount(); row++) {
                List<Object> values = new ArrayList<>();
                for (int col = 0; col < batch.columns().size(); col++) {
                    values.add(batch.value(row, col));
                }
                received.add(values);
            }
        }) : new RowSink(page -> {
            assertThat(page.columns()).containsExactlyElementsOf(projection.expectedColumns());
            assertThat(page.getPositionCount()).isEqualTo(1);
            received.add(page.columns().stream()
                    .map(name -> ColumnValue.toRaw(page.rows().get(0).get(name))).toList());
        });
        List<String> tableColumns = projection.expectedColumns().isEmpty()
                ? projection.source() : projection.expectedColumns();
        try (NativeWriteExecutor writer = executor(sink, tableColumns.toArray(String[]::new))) {
            long handle = writer.beginWrite(descriptor(projection.target().toArray(String[]::new)));
            byte[] data = integers(projection.source(), projection.rows());
            append(writer, handle, data, direct, projection.rows().length, projection.source().size());
            assertThat(received).isEqualTo(Arrays.stream(projection.expected()).map(Arrays::asList).toList());
            if (sink instanceof BatchSink batchSink) {
                assertThat(batchSink.batchCalls).isEqualTo(1);
                assertThat(batchSink.pageCalls).isZero();
                NativeWriteBatchTest.assertClosed(batchSink.borrowed);
            } else {
                assertThat(sink.pageCalls).isEqualTo(projection.rows().length);
            }
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(projection.rows().length);
        }
    }

    static Stream<Arguments> projections() {
        return Stream.of(
                new Projection("reordered", List.of("id", "value"), List.of("value", "id"),
                        new Integer[][] {{1, 2}}, List.of("value", "id"), new Integer[][] {{2, 1}}),
                new Projection("subset", List.of("ignored", "id", "extra"), List.of("id"),
                        new Integer[][] {{10, 20, 30}}, List.of("id"), new Integer[][] {{20}}),
                new Projection("verified logical right suffix", List.of("amount_right"), List.of("amount_right"),
                        new Integer[][] {{7}}, List.of("amount_right"), new Integer[][] {{7}}),
                new Projection("missing becomes null", List.of("id"), List.of("missing", "id"),
                        new Integer[][] {{1}}, List.of("missing", "id"), new Integer[][] {{null, 1}}),
                new Projection("canonical positional", List.of(physicalAlias(0), physicalAlias(1)), List.of("id", "value"),
                        new Integer[][] {{7, null}}, List.of("id", "value"), new Integer[][] {{7, null}}),
                new Projection("named subset width differs", List.of("ignored", "id"), List.of("id"),
                        new Integer[][] {{7, 8}}, List.of("id"), new Integer[][] {{8}}),
                new Projection("last duplicate including null", List.of("id", "id", "value"), List.of("id", "value"),
                        new Integer[][] {{1, 2, 3}, {4, null, 5}}, List.of("id", "value"),
                        new Integer[][] {{2, 3}, {null, 5}}),
                new Projection("descriptor deduplication", List.of("id", "value"), List.of("id", "id", "value"),
                        new Integer[][] {{1, 2}}, List.of("id", "value"), new Integer[][] {{1, 2}}),
                new Projection("right coalesce", List.of("id", "value", "id_right", "value_right"), List.of("id", "value"),
                        new Integer[][] {{1, null, 9, 8}, {null, 2, 7, 6}, {null, null, null, null}},
                        List.of("id", "value"), new Integer[][] {{1, 8}, {7, 2}, {null, null}}),
                new Projection("last right duplicate", List.of("id", "id_right", "id_right"), List.of("id"),
                        new Integer[][] {{null, 7, 8}, {null, 9, null}}, List.of("id"),
                        new Integer[][] {{8}, {null}}),
                new Projection("missing primary uses right", List.of("id_right"), List.of("id"),
                        new Integer[][] {{9}, {null}}, List.of("id"), new Integer[][] {{9}, {null}}),
                new Projection("right descriptors set target order", List.of("id", "value", "id_right", "value_right"),
                        List.of("value", "id_right", "value_right", "id"), new Integer[][] {{null, 5, 6, 7}},
                        List.of("id", "value"), new Integer[][] {{6, 5}}),
                new Projection("zero source columns", List.of(), List.of("id"),
                        new Integer[][] {{}}, List.of("id"), new Integer[][] {{null}}),
                new Projection("zero projected columns", List.of("id"), List.of(),
                        new Integer[][] {{1}}, List.of(), new Integer[][] {{}}))
                .flatMap(projection -> Stream.of(Arguments.of(projection, false, false),
                    Arguments.of(projection, false, true), Arguments.of(projection, true, false),
                    Arguments.of(projection, true, true)));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void canonicalDescriptorResolvesVerifiedOutputColumnsBeforeProjection(boolean direct, boolean columnar)
            throws Exception {
        List<List<Object>> received = new ArrayList<>();
        RowSink sink = columnar ? new BatchSink(batch -> {
            assertThat(batch.columns()).containsExactly("id", "amount_right");
            received.add(Arrays.asList(batch.value(0, 0), batch.value(0, 1)));
        }) : new RowSink(page -> {
            assertThat(page.columns()).containsExactly("id", "amount_right");
            received.add(page.columns().stream()
                    .map(name -> ColumnValue.toRaw(page.rows().get(0).get(name))).toList());
        });
        try (NativeWriteExecutor writer = executor(sink, "id", "amount_right")) {
            long handle = writer.beginWrite(descriptor(physicalAlias(0), physicalAlias(1)));
            append(writer, handle, integers(List.of(physicalAlias(0), physicalAlias(1)),
                    new Integer[] {1, 2}), direct, 1, 2);
            assertThat(received).containsExactly(List.of(1, 2));
            if (sink instanceof BatchSink batchSink) {
                assertThat(batchSink.batchCalls).isEqualTo(1);
                NativeWriteBatchTest.assertClosed(batchSink.borrowed);
            }
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(1);
        }
    }

    @ParameterizedTest(name = "source={0}, direct={1}, columnar={2}")
    @MethodSource("invalidCanonicalBatches")
    void canonicalSessionRejectsEveryChangedPhysicalLayoutBeforeSinkDispatch(
            List<String> source, boolean direct, boolean columnar) throws Exception {
        RowSink sink = columnar ? new BatchSink(batch -> { }) : new RowSink(page -> { });
        Integer[] values = new Integer[source.size()];
        Arrays.fill(values, 7);
        byte[] bytes = integers(source, values);
        try (NativeWriteExecutor writer = executor(sink, "id", "amount")) {
            long handle = writer.beginWrite(descriptor(physicalAlias(0), physicalAlias(1)));
            assertThatThrownBy(() -> append(writer, handle, bytes, direct, 1, source.size()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(writer.activeSessionCount()).isZero();
            assertThat(sink.pageCalls).isZero();
            if (sink instanceof BatchSink batchSink) assertThat(batchSink.batchCalls).isZero();
            assertThat(sink.events).containsExactly("abort", "close");
        }
    }

    static Stream<Arguments> invalidCanonicalBatches() {
        return Stream.of(List.of(physicalAlias(0)), List.of(physicalAlias(0), physicalAlias(1), physicalAlias(2)),
                List.of(physicalAlias(1), physicalAlias(0)), List.of(physicalAlias(0), physicalAlias(2)),
                List.of(physicalAlias(0), "amount"), List.of("id", "amount"), List.of("invalid_0", "invalid_1"),
                List.of(malformedAlias("01"), physicalAlias(1)),
                List.of(malformedAlias("-1"), physicalAlias(1)),
                List.of(malformedAlias("2147483648"), physicalAlias(1)),
                List.of(malformedAlias("999999999999999999999999"), physicalAlias(1)),
                List.of(malformedAlias("bad"), physicalAlias(1)))
                .flatMap(source -> Stream.of(Arguments.of(source, false, false), Arguments.of(source, false, true),
                        Arguments.of(source, true, false), Arguments.of(source, true, true)));
    }

    @ParameterizedTest
    @MethodSource("invalidCanonicalDescriptors")
    void canonicalDescriptorRejectsNoncanonicalCoordinatesBeforeSinkAllocation(List<String> columns) {
        RowSink sink = new RowSink(page -> { });
        try (NativeWriteExecutor writer = executor(sink, "id", "amount")) {
            assertThatThrownBy(() -> writer.beginWrite(descriptor(columns.toArray(String[]::new))))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(writer.activeSessionCount()).isZero();
            assertThat(sink.events).isEmpty();
        }
    }

    static Stream<List<String>> invalidCanonicalDescriptors() {
        return Stream.of(List.of(physicalAlias(1), physicalAlias(0)), List.of(physicalAlias(0), physicalAlias(2)),
                List.of(physicalAlias(0), "amount"), List.of(physicalAlias(0), physicalAlias(0)),
                List.of(physicalAlias(7), physicalAlias(8)), List.of("invalid_0", "invalid_1"),
                List.of(malformedAlias("01"), malformedAlias("02")),
                List.of(malformedAlias("-1"), malformedAlias("-2")),
                List.of(malformedAlias("2147483648"), malformedAlias("2147483649")),
                List.of(malformedAlias("999999999999999999999999"), malformedAlias("bad")));
    }

    private static String malformedAlias(String ordinal) {
        String actual = physicalAlias(0);
        return actual.substring(0, actual.lastIndexOf('_') + 1) + ordinal;
    }

    @Test
    void canonicalDescriptorWidthMismatchFailsClosed() {
        RowSink sink = new RowSink(page -> { });
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            assertThatThrownBy(() -> writer.beginWrite(descriptor(physicalAlias(0), physicalAlias(1))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("projection width");
            assertThat(writer.activeSessionCount()).isZero();
            assertThat(sink.pageCalls).isZero();
        }
    }

    @Test
    void canonicalDescriptorMetadataFailureIsNotConvertedToPositionalWrite() {
        try (NativeWriteExecutor writer = new NativeWriteExecutor(
                new TestCatalogResourceResolver((taskId) -> {
            throw new IllegalStateException("catalog metadata unavailable");
        }))) {
            assertThatThrownBy(() -> writer.beginWrite(descriptor(physicalAlias(0))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("catalog metadata unavailable");
            assertThat(writer.activeSessionCount()).isZero();
        }
    }

    @Test
    void closingProjectionDoesNotCloseOwnerButClosingOwnerInvalidatesAllViews() throws Exception {
        byte[] bytes = integers(List.of("id", "id"), new Integer[] {1, 2});
        try (var allocator = new RootAllocator(1024 * 1024)) {
            try (ColumnarBatch owner = ColumnarBatch.open(ByteBuffer.wrap(bytes), allocator, 1, 2)) {
                assertThat(owner.columns()).containsExactly("id", "id");
                try (ColumnarBatch view = WritePageProjector.projectToWriteSchema(owner, List.of("id"), false)) {
                    assertThat(view.value(0, 0)).isEqualTo(2);
                }
                assertThat(owner.value(0, 0)).isEqualTo(1);
                try (ColumnarBatch remaining = WritePageProjector.projectToWriteSchema(owner, List.of("id"), false)) {
                    owner.close();
                    assertThatThrownBy(remaining::columns).isInstanceOf(IllegalStateException.class);
                    assertThatThrownBy(() -> remaining.value(0, 0)).isInstanceOf(IllegalStateException.class);
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private record Projection(String name, List<String> source, List<String> target, Integer[][] rows,
                              List<String> expectedColumns, Integer[][] expected) {
        @Override
        public String toString() {
            return name;
        }
    }
}
