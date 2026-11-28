package org.pangrova.connector.paimon.write;

import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import java.util.Map;

/**
 * Writes pages of data to a Paimon table via BatchWriteBuilder (append semantics).
 * Supports both primary key tables (LSM merge-on-read) and append-only tables.
 */
public class PaimonPageSink extends AbstractPaimonPageSink {

    PaimonPageSink(FileStoreTable table, PaimonOutputTableHandle output, String tempDir) {
        super(new PaimonPageSinkContext(table, output, resolveWriteBuilder(table), tempDir,
                isDynamicBucket(table) || isPostponeBucket(table)));
    }

    private static BatchWriteBuilder resolveWriteBuilder(FileStoreTable table) {
        if (isPostponeBucket(table)) {
            // PostponeBucketFileStoreWrite forces AVRO format internally.
            // The native reader (Velox) cannot read AVRO. Override to
            // fixed bucket so writes use the table's configured format.
            return table.copy(Map.of("bucket", "1")).newBatchWriteBuilder();
        }
        return table.newBatchWriteBuilder();
    }

    private static boolean isDynamicBucket(FileStoreTable table) {
        return "-1".equals(table.options().get("bucket"));
    }

    private static boolean isPostponeBucket(FileStoreTable table) {
        return "-2".equals(table.options().get("bucket"));
    }
}
