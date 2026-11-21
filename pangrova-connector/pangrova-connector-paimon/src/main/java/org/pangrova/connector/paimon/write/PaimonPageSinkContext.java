package org.pangrova.connector.paimon.write;

import org.apache.paimon.table.sink.BatchWriteBuilder;

/**
 * Bundles the parameters needed to construct an {@link AbstractPaimonPageSink}.
 */
record PaimonPageSinkContext(BatchWriteBuilder writeBuilder, PaimonRowConverter rowConverter,
                              String schemaName, String tableName, String tempDir,
                              boolean dynamicBucket) {

    PaimonPageSinkContext(BatchWriteBuilder writeBuilder, PaimonRowConverter rowConverter,
                          String schemaName, String tableName, String tempDir) {
        this(writeBuilder, rowConverter, schemaName, tableName, tempDir, false);
    }
}
