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
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DynamicTableWriter write operations and cache invalidation.
 *
 * <p>Validates that:
 * <ul>
 *   <li>The write method writes records to the target table with correct prefix</li>
 *   <li>The source checkpoint is returned in the DynamicTableWriteResult</li>
 *   <li>Cache invalidation is fault-tolerant</li>
 * </ul>
 */
class AtomicCheckpointWriteTest {

    @Test
    void write_returnsResultWithCheckpoint() {
        var tableWriter = new RecordingTableWriter();
        var writer = new DynamicTableWriter(tableWriter, noOpCache());

        var result = new IncrementalResult(
                List.of(Map.of("id", 1, "value", 100)),
                1L, 0);

        DynamicTableWriteResult writeResult = writer.write("my_dt", result, "snap-42");

        assertThat(writeResult.checkpoint()).isEqualTo("snap-42");
        assertThat(writeResult.targetTable()).isEqualTo("__dt_my_dt");
        assertThat(writeResult.rowsWritten()).isEqualTo(1);
    }

    @Test
    void write_delegatesToTableWriter() {
        var tableWriter = new RecordingTableWriter();
        var writer = new DynamicTableWriter(tableWriter, noOpCache());

        var result = new IncrementalResult(
                List.of(Map.of("id", 1, "value", 200)),
                1L, 0);

        writer.write("my_dt", result, "snap-1");

        assertThat(tableWriter.writeCount).isEqualTo(1);
        assertThat(tableWriter.lastTableName).isEqualTo("__dt_my_dt");
    }

    @Test
    void write_nullCheckpoint_returnsNullInResult() {
        var tableWriter = new RecordingTableWriter();
        var writer = new DynamicTableWriter(tableWriter, noOpCache());

        var result = new IncrementalResult(
                List.of(Map.of("id", 1)),
                1L, 0);

        DynamicTableWriteResult writeResult = writer.write("my_dt", result, null);

        assertThat(writeResult.checkpoint()).isNull();
    }

    @Test
    void write_multipleRecords() {
        var tableWriter = new RecordingTableWriter();
        var writer = new DynamicTableWriter(tableWriter, noOpCache());

        var result = new IncrementalResult(
                List.of(
                        Map.of("id", 1, "name", "a"),
                        Map.of("id", 2, "name", "b"),
                        Map.of("id", 3, "name", "c")),
                3L, 0);

        DynamicTableWriteResult writeResult = writer.write("sales_dt", result, "snap-5");

        assertThat(writeResult.rowsWritten()).isEqualTo(3);
        assertThat(writeResult.targetTable()).isEqualTo("__dt_sales_dt");
    }

    @Test
    void write_cacheInvalidationFailure_doesNotBlockWrite() {
        var tableWriter = new RecordingTableWriter();
        // Use a cache that throws on invalidation
        var failingCache = new FailingInvalidationCache();
        var writer = new DynamicTableWriter(tableWriter, failingCache);

        var result = new IncrementalResult(
                List.of(Map.of("id", 1)),
                1L, 0);

        // Should not throw despite cache invalidation failure
        DynamicTableWriteResult writeResult = writer.write("my_dt", result, "snap-10");

        assertThat(writeResult).isNotNull();
        assertThat(tableWriter.writeCount).isEqualTo(1);
    }

    // -- Test implementations --

    static final class RecordingTableWriter implements TableWriter {
        int writeCount = 0;
        String lastTableName = null;

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            writeCount++;
            lastTableName = tableName;
            return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
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
        public void close() {}
    }

    private static QueryResultCache noOpCache() {
        return new QueryResultCache(new NoOpL2CacheStore());
    }

    /**
     * A QueryResultCache that throws on invalidation, testing fault tolerance.
     */
    private static QueryResultCache failingInvalidationCache() {
        return new QueryResultCache(new NoOpL2CacheStore()) {
            @Override
            public void invalidateByTable(String tableName) {
                throw new RuntimeException("cache invalidation failed");
            }
        };
    }

    static final class FailingInvalidationCache extends QueryResultCache {
        FailingInvalidationCache() {
            super(new NoOpL2CacheStore());
        }

        @Override
        public void invalidateByTable(String tableName) {
            throw new RuntimeException("simulated cache invalidation failure");
        }
    }

    static final class NoOpL2CacheStore implements L2CacheStore {
        @Override
        public Optional<CachedResult> get(String key) {
            return Optional.empty();
        }

        @Override
        public void put(String key, CachedResult value) {}

        @Override
        public void remove(String key) {}

        @Override
        public void removeByTable(String tableName) {}

        @Override
        public void removeByUser(String userId) {}

    }
}
