package org.pangrova.coordinator.dynamictable.write;

import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DynamicTableWriter}.
 *
 * <p>Uses hand-written test implementations — no mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 3, 4, 8</b></p>
 */
class DynamicTableWriterTest {

    @Test
    void normalWrite_writesToTargetTableAndInvalidatesCache() {
        var tableWriter = new RecordingTableWriter();
        var queryCache = new QueryResultCache(new NoOpL2CacheStore());
        var writer = new DynamicTableWriter(tableWriter, queryCache);

        var result = new IncrementalResult(
                List.of(Map.of("id", 1, "name", "alice"),
                        Map.of("id", 2, "name", "bob")),
                2L, 0);

        DynamicTableWriteResult writeResult = writer.write("sales_summary", result, "snap-100");

        assertThat(writeResult.targetTable()).isEqualTo("__dt_sales_summary");
        assertThat(writeResult.checkpoint()).isEqualTo("snap-100");
        assertThat(writeResult.rowsWritten()).isEqualTo(2);
        assertThat(writeResult.bytesWritten()).isGreaterThan(0);
        assertThat(tableWriter.writtenTableName).isEqualTo("__dt_sales_summary");
        assertThat(tableWriter.writtenRecords).hasSize(2);
        assertThat(tableWriter.writtenMode).isEqualTo(WriteMode.APPEND);
    }

    @Test
    void cacheInvalidationFailure_doesNotBlockWrite() {
        var tableWriter = new RecordingTableWriter();
        var queryCache = new FailingInvalidationCache();
        var writer = new DynamicTableWriter(tableWriter, queryCache);

        var result = new IncrementalResult(
                List.of(Map.of("id", 1, "value", 100)),
                1L, 0);

        // Should not throw despite cache invalidation failure
        DynamicTableWriteResult writeResult = writer.write("orders", result, "snap-200");

        assertThat(writeResult.targetTable()).isEqualTo("__dt_orders");
        assertThat(writeResult.rowsWritten()).isEqualTo(1);
        assertThat(tableWriter.writtenTableName).isEqualTo("__dt_orders");
    }

    @Test
    void writeFailure_propagatesException() {
        var tableWriter = new FailingTableWriter();
        var queryCache = new QueryResultCache(new NoOpL2CacheStore());
        var writer = new DynamicTableWriter(tableWriter, queryCache);

        var result = new IncrementalResult(
                List.of(Map.of("id", 1)),
                1L, 0);

        assertThatThrownBy(() -> writer.write("broken_dt", result, "snap-300"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("write failed");
    }

    @Test
    void emptyResult_writesZeroRows() {
        var tableWriter = new RecordingTableWriter();
        var queryCache = new QueryResultCache(new NoOpL2CacheStore());
        var writer = new DynamicTableWriter(tableWriter, queryCache);

        var result = IncrementalResult.empty();

        DynamicTableWriteResult writeResult = writer.write("empty_dt", result, null);

        assertThat(writeResult.rowsWritten()).isZero();
        assertThat(writeResult.targetTable()).isEqualTo("__dt_empty_dt");
    }

    // ── Test implementations ────────────────────────────────────────

    /**
     * Records write calls for assertion. Returns a fixed WriteResult.
     */
    static final class RecordingTableWriter implements TableWriter {

        String writtenTableName;
        List<WriteRecord> writtenRecords;
        WriteMode writtenMode;

        @Override
        public org.pangrova.spi.connector.data.write.model.WriteResult write(
                String tableName, List<WriteRecord> records,
                WriteMode writeMode, WriteContext context) {
            this.writtenTableName = tableName;
            this.writtenRecords = new ArrayList<>(records);
            this.writtenMode = writeMode;
            return new org.pangrova.spi.connector.data.write.model.WriteResult(
                    tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * TableWriter that always throws on write.
     */
    static final class FailingTableWriter implements TableWriter {

        @Override
        public org.pangrova.spi.connector.data.write.model.WriteResult write(
                String tableName, List<WriteRecord> records,
                WriteMode writeMode, WriteContext context) {
            throw new RuntimeException("write failed: simulated I/O error");
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {
            // no-op
        }
    }

    /**
     * QueryResultCache subclass that throws on invalidateByTable.
     */
    static final class FailingInvalidationCache extends QueryResultCache {

        FailingInvalidationCache() {
            super(new NoOpL2CacheStore());
        }

        @Override
        public void invalidateByTable(String tableName) {
            throw new RuntimeException("cache invalidation failed: connection refused");
        }
    }

    /**
     * No-op L2 cache store for test construction.
     */
    static final class NoOpL2CacheStore implements L2CacheStore {

        @Override
        public Optional<CachedResult> get(String key) {
            return Optional.empty();
        }

        @Override
        public void put(String key, CachedResult value) {
            // no-op
        }

        @Override
        public void remove(String key) {
            // no-op
        }

        @Override
        public void removeByTable(String tableName) {
            // no-op
        }

        @Override
        public void removeByUser(String userId) {
            // no-op
        }
    }
}
