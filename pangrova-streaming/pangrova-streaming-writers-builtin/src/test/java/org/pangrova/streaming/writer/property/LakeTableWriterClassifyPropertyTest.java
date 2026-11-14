package org.pangrova.streaming.writer.property;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.types.value.TypedRow;
import org.pangrova.connector.api.write.DataFileSink;
import org.pangrova.connector.api.enums.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.streaming.writer.lake.LakeTableWriter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link LakeTableWriter#classifyDeletes} correctness.
 */
class LakeTableWriterClassifyPropertyTest {

    @Property(tries = 10)
    void allRecordsAreClassified(
            @ForAll @IntRange(min = 1, max = 50) int recordCount) {
        LakeTableWriter writer = createWriter();
        List<WriteRecord> records = generateRecords(recordCount);

        LakeTableWriter.DeleteClassification result = writer.classifyDeletes(records);

        int total = result.inserts().size()
                + result.positionDeletes().size()
                + result.equalityDeletes().size();
        assertThat(total).isEqualTo(recordCount);
    }

    @Property(tries = 10)
    void duplicateKeysProducePositionDeletes(
            @ForAll @IntRange(min = 2, max = 10) int duplicateCount) {
        LakeTableWriter writer = createWriter();
        List<WriteRecord> records = new ArrayList<>();
        for (int i = 0; i < duplicateCount; i++) {
            records.add(insertRecord(1, "name_" + i));
        }

        LakeTableWriter.DeleteClassification result = writer.classifyDeletes(records);

        // Last record becomes insert, rest become position deletes
        assertThat(result.inserts()).hasSize(1);
        assertThat(result.positionDeletes()).hasSize(duplicateCount - 1);
        assertThat(result.equalityDeletes()).isEmpty();
    }

    @Property(tries = 10)
    void uniqueInsertsProduceNoDeletes(
            @ForAll @IntRange(min = 1, max = 20) int count) {
        LakeTableWriter writer = createWriter();
        List<WriteRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(insertRecord(i, "name_" + i));
        }

        LakeTableWriter.DeleteClassification result = writer.classifyDeletes(records);

        assertThat(result.inserts()).hasSize(count);
        assertThat(result.positionDeletes()).isEmpty();
        assertThat(result.equalityDeletes()).isEmpty();
    }

    @Property(tries = 10)
    void singleDeleteProducesEqualityDelete(
            @ForAll @IntRange(min = 1, max = 20) int id) {
        LakeTableWriter writer = createWriter();
        List<WriteRecord> records = List.of(deleteRecord(id));

        LakeTableWriter.DeleteClassification result = writer.classifyDeletes(records);

        assertThat(result.inserts()).isEmpty();
        assertThat(result.positionDeletes()).isEmpty();
        assertThat(result.equalityDeletes()).hasSize(1);
    }

    private static LakeTableWriter createWriter() {
        return new LakeTableWriter(new NoOpDataFileSink(), "id", r -> {});
    }

    private static List<WriteRecord> generateRecords(int count) {
        List<WriteRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(insertRecord(i, "name_" + i));
        }
        return records;
    }

    private static WriteRecord insertRecord(int id, String name) {
        TypedRow row = TypedRow.fromRawMap(Map.of("id", id, "name", name));
        return new WriteRecord(row, WriteRecord.WriteOperation.INSERT, Instant.now());
    }

    private static WriteRecord deleteRecord(int id) {
        TypedRow row = TypedRow.fromRawMap(Map.of("id", id));
        return new WriteRecord(row, WriteRecord.WriteOperation.DELETE, Instant.now());
    }

    static final class NoOpDataFileSink implements DataFileSink {
        @Override
        public int writeRecords(String tableName, List<WriteRecord> records, WriteMode writeMode) {
            return records.isEmpty() ? 0 : 1;
        }

        @Override
        public void close() {}
    }
}
