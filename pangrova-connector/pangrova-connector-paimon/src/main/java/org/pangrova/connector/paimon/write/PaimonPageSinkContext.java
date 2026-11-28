package org.pangrova.connector.paimon.write;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;

/** Parameters for one SDK writer and its durable output binding. */
record PaimonPageSinkContext(FileStoreTable table, PaimonOutputTableHandle output, BatchWriteBuilder writeBuilder,
                            String tempDir, boolean dynamicBucket) {

    PaimonPageSinkContext(FileStoreTable table, PaimonOutputTableHandle output, BatchWriteBuilder writeBuilder,
                          String tempDir) {
        this(table, output, writeBuilder, tempDir, false);
    }
}
