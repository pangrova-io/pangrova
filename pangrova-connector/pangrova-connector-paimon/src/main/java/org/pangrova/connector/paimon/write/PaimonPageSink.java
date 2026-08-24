package org.pangrova.connector.paimon.write;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import java.util.Map;

/**
 * Writes pages of data to a Paimon table via BatchWriteBuilder (append semantics).
 * Supports both primary key tables (LSM merge-on-read) and append-only tables.
 */
public class PaimonPageSink extends AbstractPaimonPageSink {

    PaimonPageSink(Table table, String schemaName, String tableName, String tempDir) {
        super(new PaimonPageSinkContext(resolveWriteBuilder(table),
                new PaimonRowConverter(table.rowType()),
                schemaName, tableName, tempDir,
                isDynamicBucket(table) || isPostponeBucket(table),
                false));
    }

    private static BatchWriteBuilder resolveWriteBuilder(Table table) {
        if (isPostponeBucket(table) && table instanceof FileStoreTable fst) {
            // PostponeBucketFileStoreWrite forces AVRO format internally.
            // The native reader (Velox) cannot read AVRO. Override to
            // fixed bucket so writes use the table's configured format.
            return fst.copy(Map.of("bucket", "1")).newBatchWriteBuilder();
        }
        return table.newBatchWriteBuilder();
    }

    private static boolean isDynamicBucket(Table table) {
        return "-1".equals(table.options().get("bucket"));
    }

    private static boolean isPostponeBucket(Table table) {
        return "-2".equals(table.options().get("bucket"));
    }
}
