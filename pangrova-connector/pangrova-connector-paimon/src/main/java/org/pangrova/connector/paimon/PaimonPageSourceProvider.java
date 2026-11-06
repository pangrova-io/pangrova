package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.api.error.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.read.PaimonPageSource;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.ConnectorPageSource;
import org.pangrova.connector.spi.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/** Creates readers from the same SDK table and split selected during planning. */
public class PaimonPageSourceProvider implements ConnectorPageSourceProvider {
    private final PaimonSplitRegistry splitRegistry;

    /** Uses the connector's query-scoped split owner. */
    public PaimonPageSourceProvider(PaimonSplitRegistry splitRegistry) {
        this.splitRegistry = Objects.requireNonNull(splitRegistry, "splitRegistry");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split,
            ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
        PaimonSplit paimonSplit = (PaimonSplit) split;
        PaimonTableHandle handle = (PaimonTableHandle) table;
        List<PaimonColumnHandle> projected = columns.stream().map(PaimonColumnHandle.class::cast).toList();
        var lease = splitRegistry.claim(PaimonSplitRegistry.authority(session), paimonSplit.splitRegistryId());
        try {
            requireSelectedTable(lease, handle);
            var reader = lease.openReader();
            return new PaimonPageSource(reader, projected, lease.table().rowType(),
                    PaimonPageSource.DEFAULT_BATCH_SIZE, lease);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                lease.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (failure instanceof IOException ioFailure) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_READ_ERROR,
                        "Failed to create Paimon reader: " + paimonSplit.tableName(), ioFailure);
            }
            if (failure instanceof Error error) throw error;
            throw (RuntimeException) failure;
        }
    }

    private static void requireSelectedTable(PaimonSplitRegistry.ReadLease lease, PaimonTableHandle handle) {
        if (!(lease.table() instanceof FileStoreTable fileStore)
                || !fileStore.location().toString().equals(handle.tableLocation())
                || !JsonSerdeUtil.toFlatJson(fileStore.rowType()).equals(handle.rowTypeJson())) {
            throw new IllegalStateException("Paimon reader table does not match the selected handle");
        }
        PaimonReadBindings.validateSelected(fileStore, handle);
    }
}
