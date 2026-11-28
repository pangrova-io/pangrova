package org.pangrova.connector.paimon.write;

import org.pangrova.connector.api.enums.CommitEventType;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.apache.paimon.types.RowKind;

/**
 * Writes delete markers to a Paimon primary key table.
 *
 * <p>Each incoming row is written with {@link RowKind#DELETE}, causing the
 * Paimon LSM engine to mark matching primary key rows as deleted during
 * compaction. Requires the target table to have primary keys defined.
 */
public final class PaimonDeletePageSink extends AbstractPaimonPageSink {

    PaimonDeletePageSink(FileStoreTable table, PaimonOutputTableHandle output, String tempDir) {
        super(new PaimonPageSinkContext(table, output, table.newBatchWriteBuilder(), tempDir));
    }

    @Override
    protected RowKind writeRowKind() {
        return RowKind.DELETE;
    }

    @Override
    protected CommitEventType commitEventType() {
        return CommitEventType.DELETE;
    }
}
