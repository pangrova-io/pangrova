package org.pangrova.connector.paimon.write;

import org.pangrova.spi.connector.index.CommitEventType;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowKind;

/**
 * Writes delete markers to a Paimon primary key table.
 *
 * <p>Each incoming row is written with {@link RowKind#DELETE}, causing the
 * Paimon LSM engine to mark matching primary key rows as deleted during
 * compaction. Requires the target table to have primary keys defined.
 */
public final class PaimonDeletePageSink extends AbstractPaimonPageSink {

    PaimonDeletePageSink(Table table, String schemaName, String tableName, String tempDir) {
        super(new PaimonPageSinkContext(table.newBatchWriteBuilder(),
                new PaimonRowConverter(table.rowType()),
                schemaName, tableName, tempDir));
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
