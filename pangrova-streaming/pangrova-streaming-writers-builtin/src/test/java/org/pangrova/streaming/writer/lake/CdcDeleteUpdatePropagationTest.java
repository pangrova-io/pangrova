package org.pangrova.streaming.writer.lake;

import org.pangrova.connector.api.enums.WriteOperation;
import org.pangrova.connector.api.write.DataFileSink;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.enums.WriteMode;
import org.pangrova.connector.api.write.WriteRecord;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.cdc.WriteRecordConverter;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for Task 168 / F-CHAIN-STREAM-02.
 *
 * <p>Feeds a CDC change stream containing DELETE and UPDATE records through the
 * parse&rarr;write seam ({@link WriteRecordConverter} &rarr; {@link LakeTableWriter})
 * and asserts the {@link DataFileSink} actually receives them as
 * DELETE/UPDATE operations instead of having them silently dropped (the
 * pre-fix behaviour materialised only the after-image of INSERTs).</p>
 */
class CdcDeleteUpdatePropagationTest {

    @Test
    void deleteAndUpdateCdcRecordsReachSink() {
        AccumulatingDataFileSink sink = new AccumulatingDataFileSink();
        try (LakeTableWriter writer = new LakeTableWriter(sink, "id", null)) {
            List<WriteRecord> records = List.of(
                    WriteRecordConverter.fromCdcEnvelope(insertEnvelope(1, "alice")),
                    WriteRecordConverter.fromCdcEnvelope(updateEnvelope(2, "bob", "bobby")),
                    WriteRecordConverter.fromCdcEnvelope(deleteEnvelope(3, "carol")));

            writer.write("db.target", records, WriteMode.UPSERT, WriteContext.selfManaged());
        }

        List<WriteRecord> sent = sink.allSentRecords();

        // The DELETE of key 3 must reach the sink as a DELETE operation.
        assertThat(sent)
                .as("DELETE record must be materialised through the sink")
                .anySatisfy(r -> {
                    assertThat(r.operation()).isEqualTo(WriteOperation.DELETE);
                    assertThat(r.data().getInt("id")).isEqualTo(3);
                });

        // The UPDATE of key 2 must produce a delete of the old row AND an insert
        // of the after-image (delete+insert semantics), so the after-image
        // ("bobby") must be written, not dropped.
        assertThat(sent)
                .as("UPDATE after-image must be materialised through the sink")
                .anySatisfy(r -> {
                    assertThat(r.operation()).isEqualTo(WriteOperation.UPDATE);
                    assertThat(r.data().getInt("id")).isEqualTo(2);
                    assertThat(r.data().getString("name")).isEqualTo("bobby");
                });

        // Deletes/updates must be routed with delete-capable write mode, not plain APPEND.
        assertThat(sink.modesUsed())
                .as("delete materialisation must not use APPEND-only mode")
                .contains(WriteMode.UPSERT);
    }

    // --- CDC envelope builders ---

    private static CdcEnvelope insertEnvelope(int id, String name) {
        return new CdcEnvelope(CdcOperation.INSERT, "db.target",
                null, row(id, name), Instant.now(), 0, Map.of());
    }

    private static CdcEnvelope updateEnvelope(int id, String before, String after) {
        return new CdcEnvelope(CdcOperation.UPDATE, "db.target",
                row(id, before), row(id, after), Instant.now(), 0, Map.of());
    }

    private static CdcEnvelope deleteEnvelope(int id, String name) {
        return new CdcEnvelope(CdcOperation.DELETE, "db.target",
                row(id, name), null, Instant.now(), 0, Map.of());
    }

    private static CdcRow row(int id, String name) {
        return CdcRow.of(Map.of("id", id, "name", name));
    }

    /** DataFileSink that accumulates every record and write mode across all calls. */
    static final class AccumulatingDataFileSink implements DataFileSink {
        private final List<WriteRecord> sent = new ArrayList<>();
        private final List<WriteMode> modes = new ArrayList<>();

        @Override
        public int writeRecords(String tableName, List<WriteRecord> records, WriteMode writeMode) {
            sent.addAll(records);
            modes.add(writeMode);
            return records.size();
        }

        List<WriteRecord> allSentRecords() {
            return sent;
        }

        List<WriteMode> modesUsed() {
            return modes;
        }

        @Override
        public void close() {}
    }
}
