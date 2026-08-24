package org.pangrova.connector.paimon.write;

import org.pangrova.spi.connector.index.CommitEventType;
import org.apache.paimon.table.Table;

/**
 * Overwrites data in a Paimon table atomically.
 * Uses BatchWriteBuilder.withOverwrite() with the partition map from the output handle.
 * Empty map = dynamic partition overwrite; non-empty = static partition overwrite.
 */
public final class PaimonOverwritePageSink extends AbstractPaimonPageSink {

    PaimonOverwritePageSink(Table table, String schemaName, String tableName,
                            String tempDir, java.util.Map<String, String> overwritePartition) {
        super(new PaimonPageSinkContext(
                table.newBatchWriteBuilder().withOverwrite(overwritePartition),
                new PaimonRowConverter(table.rowType()),
                schemaName, tableName, tempDir));
    }

    @Override
    protected CommitEventType commitEventType() {
        return CommitEventType.OVERWRITE;
    }
}
