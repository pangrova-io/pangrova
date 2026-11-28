package org.pangrova.connector.paimon.write;

import org.pangrova.connector.api.enums.CommitEventType;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;

/**
 * Overwrites data in a Paimon table atomically.
 * Uses BatchWriteBuilder.withOverwrite() with the partition map from the output handle.
 * Empty map = dynamic partition overwrite; non-empty = static partition overwrite.
 */
public final class PaimonOverwritePageSink extends AbstractPaimonPageSink {

    PaimonOverwritePageSink(FileStoreTable table, PaimonOutputTableHandle output, String tempDir) {
        super(new PaimonPageSinkContext(table, output,
                table.newBatchWriteBuilder().withOverwrite(output.overwritePartition()), tempDir));
    }

    @Override
    protected CommitEventType commitEventType() {
        return CommitEventType.OVERWRITE;
    }
}
