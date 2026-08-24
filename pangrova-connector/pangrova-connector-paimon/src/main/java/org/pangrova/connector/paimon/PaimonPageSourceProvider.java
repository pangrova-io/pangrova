package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.connector.paimon.handle.PaimonColumnHandle;
import org.pangrova.connector.paimon.handle.PaimonSplit;
import org.pangrova.connector.paimon.handle.PaimonTableHandle;
import org.pangrova.connector.paimon.read.PaimonPageSource;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSource;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorSplit;
import org.pangrova.spi.connector.metadata.DynamicFilter;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.data.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Creates page sources for reading Paimon data files via RecordReader.
 * Always loads a fresh Table from catalog to see the latest snapshot,
 * ensuring merge-on-read semantics (e.g. partial-update) work correctly.
 */
public class PaimonPageSourceProvider implements ConnectorPageSourceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPageSourceProvider.class);

    private final Catalog catalog;
    private final PaimonSplitRegistry splitRegistry;

    public PaimonPageSourceProvider(Catalog catalog, PaimonSplitRegistry splitRegistry) {
        this.catalog = catalog;
        this.splitRegistry = splitRegistry;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter) {
        PaimonSplit paimonSplit = (PaimonSplit) split;
        PaimonTableHandle paimonTable = (PaimonTableHandle) table;
        List<PaimonColumnHandle> projected = toColumnHandles(columns);

        LOG.debug("Opening page source for {} with {} columns",
                paimonSplit.tableName(), projected.size());

        // BUG-24 fix: load table once and reuse for both reader and rowType
        Table freshTable = loadFreshTable(paimonTable);
        RecordReader<InternalRow> reader = createReader(freshTable, paimonSplit);
        return new PaimonPageSource(reader, projected, freshTable.rowType(),
                PaimonPageSource.DEFAULT_BATCH_SIZE);
    }

    private RecordReader<InternalRow> createReader(Table freshTable, PaimonSplit split) {
        // getAndMarkConsumed returns null for already-consumed splits (dedup for expanded append-only splits)
        org.apache.paimon.table.source.Split sdkSplit = splitRegistry.getAndMarkConsumed(split.splitRegistryId());
        if (sdkSplit == null) {
            // Empty reader — this expanded split was already consumed by a sibling
            return new RecordReader<InternalRow>() {
                @Override public RecordReader.RecordIterator<InternalRow> readBatch() { return null; }
                @Override public void close() {}
            };
        }
        try {
            ReadBuilder readBuilder = freshTable.newReadBuilder();
            return readBuilder.newRead().createReader(sdkSplit);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create reader for: " + split.tableName(), e);
        }
    }

    private Table loadFreshTable(PaimonTableHandle handle) {
        Identifier id = Identifier.create(handle.schemaName(), handle.tableName());
        try {
            return catalog.getTable(id);
        } catch (Catalog.TableNotExistException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND,
                    "Table not found: " + id, e);
        }
    }

    private List<PaimonColumnHandle> toColumnHandles(List<ColumnHandle> columns) {
        return columns.stream()
                .map(PaimonColumnHandle.class::cast)
                .toList();
    }
}
