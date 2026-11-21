package org.pangrova.worker.write;

import org.pangrova.batch.api.reader.ReadableBatch;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.batch.api.page.Page;
import org.pangrova.worker.write.WriteBatchFixtures.BatchSink;
import org.pangrova.worker.write.WriteBatchFixtures.RowSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.worker.write.WriteBatchFixtures.append;
import static org.pangrova.worker.write.WriteBatchFixtures.descriptor;
import static org.pangrova.worker.write.WriteBatchFixtures.directRange;
import static org.pangrova.worker.write.WriteBatchFixtures.executor;
import static org.pangrova.worker.write.WriteBatchFixtures.integers;

class NativeWriteBatchTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void columnarCapabilityReceivesScalarsAndNullsSynchronously(boolean direct) throws Exception {
        List<Page> snapshots = new ArrayList<>();
        BatchSink sink = new BatchSink(batch -> snapshots.add(WriteBatchFixtures.snapshot(batch)));
        byte[] bytes = integers(List.of("id", "value"),
                new Integer[] {Integer.MIN_VALUE, Integer.MAX_VALUE},
                new Integer[] {null, -1}, new Integer[] {0, null});
        try (NativeWriteExecutor writer = executor(sink, "id", "value")) {
            long handle = writer.beginWrite(descriptor("id", "value"));
            append(writer, handle, bytes, direct, 3, 2);

            assertClosed(sink.borrowed);
            assertThat(sink.batchCalls).isEqualTo(1);
            assertThat(sink.pageCalls).isZero();
            Page page = snapshots.get(0);
            assertThat(page.columns()).containsExactly("id", "value");
            assertThat(page.getPositionCount()).isEqualTo(3);
            assertThat(raw(page, 0, "id")).isEqualTo(Integer.MIN_VALUE);
            assertThat(raw(page, 0, "value")).isEqualTo(Integer.MAX_VALUE);
            assertThat(raw(page, 1, "id")).isNull();
            assertThat(raw(page, 1, "value")).isEqualTo(-1);
            assertThat(raw(page, 2, "value")).isNull();
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(3);
            assertThat(writer.drainCommitMetadataForTask("another-task")).isEmpty();
            assertThat(writer.drainCommitMetadataForTask("write-task")).containsExactly(1, 2, 3);
            assertThat(sink.events).containsExactly("finish", "close");
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void nestedValuesAreDetachedForBothSinkCapabilities(boolean direct, boolean columnar) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        ConnectorPageSink sink = columnar
            ? new BatchSink(batch -> snapshotCells(batch, rows))
            : new RowSink(page -> page.rows().forEach(row -> rows.add(page.columns().stream()
                .map(name -> ColumnValue.toRaw(row.get(name))).toList())));
        byte[] bytes = WriteBatchFixtures.complex(false);
        try (NativeWriteExecutor writer = executor(sink, "id", "name", "items", "attributes", "payload", "amount")) {
            long handle = writer.beginWrite(descriptor("id", "name", "items", "attributes", "payload", "amount"));
            append(writer, handle, bytes, direct, 2, 6);
            Arrays.fill(bytes, (byte) 0);
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(2);
        }
        assertThat(rows).hasSize(2);
        List<Object> first = rows.get(0);
        List<Object> second = rows.get(1);
        assertThat(first.get(0)).isEqualTo(Long.MIN_VALUE);
        assertThat(second.get(0)).isEqualTo(Long.MAX_VALUE);
        assertThat(first.get(1)).isEqualTo("café");
        assertThat(second.get(1)).isNull();
        List<?> items = (List<?>) first.get(2);
        assertThat(items).hasSize(2);
        assertThat(items.get(1)).isNull();
        assertThat(((Map<?, ?>) items.get(0)).get("label")).isEqualTo("nested");
        assertThat(((Map<?, ?>) items.get(0)).get("quantity")).isNull();
        assertThat((List<?>) second.get(2)).isEmpty();
        Map<?, ?> attributes = (Map<?, ?>) first.get(3);
        assertThat(attributes).hasSize(2);
        assertThat(attributes.get(-7)).isEqualTo("typed-key");
        assertThat(attributes.containsKey(9)).isTrue();
        assertThat(attributes.get(9)).isNull();
        assertThat(second.get(3)).isNull();
        assertThat((byte[]) first.get(4)).containsExactly(0, 1, -1);
        assertThat((byte[]) second.get(4)).isEmpty();
        assertThat(first.get(5)).isEqualTo(new BigDecimal("-1234.50"));
        assertThat(second.get(5)).isNull();
    }

    @ParameterizedTest
    @CsvSource({"false,items", "false,attributes", "true,items", "true,attributes"})
    void rowOnlyProjectionPreservesNestedNullsInEachCollection(boolean direct, String column) throws Exception {
        List<Page> pages = new ArrayList<>();
        RowSink sink = new RowSink(pages::add);
        try (NativeWriteExecutor writer = executor(sink, column)) {
            long handle = writer.beginWrite(descriptor(column));
            append(writer, handle, WriteBatchFixtures.complex(false), direct, 2, 6);
            assertThat(pages).hasSize(2);
            assertThat(pages).allSatisfy(page -> assertThat(page.getPositionCount()).isEqualTo(1));
            Object value = raw(pages.get(0), 0, column);
            if (column.equals("items")) {
                List<?> items = (List<?>) value;
                assertThat(items).hasSize(2);
                assertThat(items.get(1)).isNull();
                assertThat((List<?>) raw(pages.get(1), 0, column)).isEmpty();
            } else {
                Map<?, ?> entries = (Map<?, ?>) value;
                assertThat(entries.containsKey(9)).isTrue();
                assertThat(entries.get(9)).isNull();
                assertThat(raw(pages.get(1), 0, column)).isNull();
            }
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void zeroRowsStillDeliverTheProjectedSchema(boolean direct, boolean columnar) throws Exception {
        List<Page> pages = new ArrayList<>();
        ConnectorPageSink sink = columnar
                ? new BatchSink(batch -> pages.add(WriteBatchFixtures.snapshot(batch))) : new RowSink(pages::add);
        try (NativeWriteExecutor writer = executor(sink, "value", "id")) {
            long handle = writer.beginWrite(descriptor("value", "id"));
            append(writer, handle, integers(List.of("id", "value")), direct, 0, 2);
            assertThat(writer.commitWrite(handle).rowsWritten()).isZero();
        }
        assertThat(pages).hasSize(1);
        assertThat(pages.get(0).columns()).containsExactly("value", "id");
        assertThat(pages.get(0).rows()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rowOnlySinkReceivesOneRowAtATimeInOrder(boolean direct) throws Exception {
        Integer[][] rows = new Integer[4096][1];
        for (int i = 0; i < rows.length; i++) {
            rows[i][0] = i;
        }
        AtomicInteger next = new AtomicInteger();
        RowSink sink = new RowSink(page -> {
            assertThat(page.columns()).containsExactly("id");
            assertThat(page.getPositionCount()).isEqualTo(1);
            assertThat(raw(page, 0, "id")).isEqualTo(next.getAndIncrement());
        });
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            append(writer, handle, integers(List.of("id"), rows), direct, rows.length, 1);
            assertThat(sink.pageCalls).isEqualTo(rows.length);
            assertThat(next.get()).isEqualTo(rows.length);
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(rows.length);
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void bufferRangeAndMarkAreUnchangedAndBorrowedViewsExpire(boolean direct, boolean readOnly) throws Exception {
        byte[] bytes = integers(List.of("id"), new Integer[] {7});
        ByteBuffer storage = direct ? directRange(bytes) : heapRange(bytes);
        ByteBuffer input = readOnly ? storage.asReadOnlyBuffer() : storage;
        input.mark();
        int position = input.position();
        int limit = input.limit();
        List<Page> snapshots = new ArrayList<>();
        BatchSink sink = new BatchSink(batch -> snapshots.add(WriteBatchFixtures.snapshot(batch)));
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            writer.appendDirectBatch(handle, input, 1, 1);
            ReadableBatch first = sink.borrowed;
            assertThat(input.position()).isEqualTo(position);
            assertThat(input.limit()).isEqualTo(limit);
            assertThat(input.reset().position()).isEqualTo(position);
            while (storage.hasRemaining()) {
                storage.put((byte) 0);
            }
            assertClosed(first);
            writer.appendDirectBatch(handle,
                    directRange(integers(List.of("id"), new Integer[] {9})), 1, 1);
            assertClosed(first);
            assertClosed(sink.borrowed);
            assertThat(raw(snapshots.get(0), 0, "id")).isEqualTo(7);
            assertThat(raw(snapshots.get(1), 0, "id")).isEqualTo(9);
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(2);
        }
    }

    @Test
    void byteArrayMayBeReusedImmediatelyAfterAppend() throws Exception {
        List<Page> pages = new ArrayList<>();
        BatchSink sink = new BatchSink(batch -> pages.add(WriteBatchFixtures.snapshot(batch)));
        try (NativeWriteExecutor writer = executor(sink, "id")) {
            long handle = writer.beginWrite(descriptor("id"));
            byte[] data = integers(List.of("id"), new Integer[] {42});
            writer.appendDirectBatch(handle, directRange(data), 1, 1);
            Arrays.fill(data, (byte) 0);
            assertClosed(sink.borrowed);
            assertThat(raw(pages.get(0), 0, "id")).isEqualTo(42);
            assertThat(writer.commitWrite(handle).rowsWritten()).isEqualTo(1);
        }
    }

    private static ByteBuffer heapRange(byte[] bytes) {
        return ByteBuffer.allocate(bytes.length + 16).position(8).put(bytes).limit(bytes.length + 8).position(8);
    }

    private static void snapshotCells(ReadableBatch batch, List<List<Object>> rows) {
        for (int row = 0; row < batch.rowCount(); row++) {
            List<Object> values = new ArrayList<>();
            for (int column = 0; column < batch.columns().size(); column++) {
                values.add(batch.value(row, column));
            }
            rows.add(values);
        }
    }

    static Object raw(Page page, int row, String column) {
        return ColumnValue.toRaw(page.rows().get(row).get(column));
    }

    static void assertClosed(ReadableBatch batch) {
        assertThatThrownBy(batch::columns).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(batch::rowCount).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> batch.value(0, 0)).isInstanceOf(IllegalStateException.class);
    }
}
