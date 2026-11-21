package org.pangrova.connector.paimon;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.connector.api.data.ConnectorCdcPageSink;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.write.PaimonPageSinkProvider;
import org.pangrova.testfixtures.tableformat.PaimonCommitTestSupport;
import org.pangrova.types.value.TypedRow;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves Paimon preserves CDC row kinds in one merge commit. */
class PaimonCdcMergeCommitTest extends PaimonDeleteTestSupport {

    @Test
    void updateBeforeAndAfterCommitAsOneMergeSnapshot() throws Exception {
        createTable(false, false);
        StoredRow original = new StoredRow(1, "east", "one", "old");
        StoredRow survivor = new StoredRow(2, "west", "two", "keep");
        StoredRow updated = new StoredRow(1, "east", "one", "new");
        writeRows(original, survivor);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new PaimonSplitRegistry());
        var handle = metadata.getTableHandle(SESSION,
                new SchemaTableName(DATABASE, TABLE_NAME));
        PaimonOutputTableHandle output = (PaimonOutputTableHandle) metadata.beginMerge(SESSION, handle);
        assertThat(output.tableIdentity()).hasValueSatisfying(identity -> {
            assertThat(identity.format()).isEqualTo("PAIMON");
            assertThat(identity.formatTableUuid()).isEqualTo(output.tableUuid());
            assertThat(identity.location()).isEqualTo(output.tableLocation());
            assertThat(identity.schemaFingerprint()).isEqualTo(output.schemaFingerprint());
        });
        var provider = new PaimonPageSinkProvider(catalog, warehouse.resolve("spill").toString());
        ConnectorCdcPageSink sink = provider.createCdcPageSink(
                ConnectorTransactionHandle.empty(), SESSION, output);
        byte[] fragment;
        try {
            sink.appendRow(row(original), CdcRowKind.UPDATE_BEFORE);
            sink.appendRow(row(updated), CdcRowKind.UPDATE_AFTER);
            fragment = sink.finishWithMetadata();
        } finally {
            sink.close();
        }

        assertThat(readRows()).containsExactlyInAnyOrder(original, survivor);
        PaimonCommitTestSupport.finish(metadata, SESSION, output, List.of(fragment));

        assertThat(readRows()).containsExactlyInAnyOrder(updated, survivor);
    }

    private static TypedRow row(StoredRow row) {
        return TypedRow.builder()
                .putLong("id", row.id()).putString("region", row.region())
                .putString("day", row.day()).putString("payload", row.payload()).build();
    }
}
