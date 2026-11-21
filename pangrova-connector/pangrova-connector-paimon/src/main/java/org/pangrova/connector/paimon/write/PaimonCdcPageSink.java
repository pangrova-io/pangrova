package org.pangrova.connector.paimon.write;

import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.connector.api.data.ConnectorCdcPageSink;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.types.value.TypedRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.RowKind;

import java.util.Map;

/** Paimon CDC sink preserving source row kinds in one MERGE commit fragment. */
public class PaimonCdcPageSink extends AbstractPaimonPageSink implements ConnectorCdcPageSink {

    PaimonCdcPageSink(Table table, String schemaName, String tableName, String tempDir) {
        super(new PaimonPageSinkContext(resolveWriteBuilder(table),
                new PaimonRowConverter(table.rowType()), schemaName, tableName, tempDir,
                isDynamicBucket(table) || isPostponeBucket(table)));
    }

    @Override
    public void appendRow(TypedRow row, CdcRowKind kind) {
        appendRowKind(row, switch (kind) {
            case INSERT -> RowKind.INSERT;
            case UPDATE_BEFORE -> RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER -> RowKind.UPDATE_AFTER;
            case DELETE -> RowKind.DELETE;
        });
    }

    @Override
    protected CommitEventType commitEventType() {
        return CommitEventType.MERGE;
    }

    private static BatchWriteBuilder resolveWriteBuilder(Table table) {
        if (isPostponeBucket(table) && table instanceof FileStoreTable fst) {
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
