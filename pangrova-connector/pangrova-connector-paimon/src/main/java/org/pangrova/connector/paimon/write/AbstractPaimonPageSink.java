package org.pangrova.connector.paimon.write;

import org.pangrova.common.concurrency.NotThreadSafe;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.batch.api.reader.ReadableBatch;
import org.pangrova.connector.api.data.ColumnarPageSink;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.types.value.TypedRow;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.connector.runtime.codec.ConnectorBinaryPageCodec;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.enums.CommitEventType;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Base class for Paimon page sinks that handles common write, commit, and
 * resource cleanup logic.
 *
 * <p>Subclasses construct a {@link PaimonPageSinkContext} that determines
 * the write semantics (append vs overwrite).
 *
 * <p>Not thread-safe: instances are confined to a single writer thread.
 */
@NotThreadSafe
abstract class AbstractPaimonPageSink implements ColumnarPageSink {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPaimonPageSink.class);

    private final IOManager ioManager;
    private final BatchTableWrite tableWrite;
    private final BatchTableCommit tableCommit;
    private final PaimonRowConverter rowConverter;
    private final FileStoreTable table;
    private final PaimonOutputTableHandle output;
    private final boolean dynamicBucket;
    private boolean finished;
    private boolean aborted;
    private boolean stopped;
    private boolean writeClosed;
    private boolean commitClosed;
    private boolean ioClosed;
    private byte[] commitMetadata;

    protected AbstractPaimonPageSink(PaimonPageSinkContext context) {
        this.ioManager = new IOManagerImpl(context.tempDir());
        this.tableWrite = context.writeBuilder().newWrite();
        this.tableWrite.withIOManager(ioManager);
        this.tableCommit = context.writeBuilder().newCommit();
        this.table = context.table();
        this.output = context.output();
        this.rowConverter = new PaimonRowConverter(table.rowType());
        this.dynamicBucket = context.dynamicBucket();
    }

    @Override
    public void appendPage(Page page) {
        for (TypedRow row : page.rows()) {
            writeRow(row);
        }
    }

    /**
     * Converts and writes each row synchronously without retaining the caller's batch.
     *
     * @param batch caller-owned batch, with missing target columns written as null
     * @throws ConnectorException if value conversion or writing fails
     */
    @Override
    public void appendColumnarBatch(ReadableBatch batch) {
        requireWriting();
        int[] ordinals = rowConverter.columnOrdinals(batch.columns());
        int rowCount = batch.rowCount();
        try {
            for (int row = 0; row < rowCount; row++) {
                writeConvertedRow(rowConverter.convertColumnarRow(batch, row, ordinals, writeRowKind()));
            }
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to write columnar batch to Paimon table: " + e.getMessage(), e);
        }
    }

    @Override
    public void appendNativePage(NativePage page) {
        appendPage(ConnectorBinaryPageCodec.toPage(page));
    }

    @Override
    public byte[] finishWithMetadata() {
        if (aborted) {
            return new byte[0];
        }
        if (finished) return commitMetadata.clone();
        requireWriting();
        stopped = true;
        commitMetadata = collectCommitMetadata();
        finished = true;
        return commitMetadata.clone();
    }

    @Override
    public void abort() {
        if (finished || aborted) {
            return;
        }
        stopped = true;
        closeWrite();
        aborted = true;
    }

    @Override
    public void close() {
        stopped = true;
        // The writer may still be joining compaction/IO. Its owner and spill
        // manager must survive a failed close; never free them first.
        closeWrite();
        if (!commitClosed) {
            closeResource(tableCommit, "commit");
            commitClosed = true;
        }
        if (!ioClosed) {
            closeResource(ioManager, "spill manager");
            ioClosed = true;
        }
    }

    private void writeRow(TypedRow row) {
        requireWriting();
        try {
            writeConvertedRow(rowConverter.convert(row, writeRowKind()));
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to write row to Paimon table: " + e.getMessage(), e);
        }
    }

    private void writeConvertedRow(GenericRow row) throws Exception {
        if (dynamicBucket) {
            tableWrite.write(row, 0);
        } else {
            // The SDK extractor assigns fixed or postponed buckets from table options.
            tableWrite.write(row);
        }
    }

    private void requireWriting() {
        if (stopped) throw new IllegalStateException("Paimon write sink has stopped accepting rows");
    }

    private void closeWrite() {
        if (!writeClosed) {
            closeResource(tableWrite, "writer");
            writeClosed = true;
        }
    }

    private void closeResource(AutoCloseable resource, String name) {
        try {
            resource.close();
        } catch (Exception failure) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to close Paimon " + name + " for " + output.schemaName() + "." + output.tableName(),
                    failure);
        }
    }

    /**
     * Returns the RowKind applied to every row written by this sink.
     * Default is INSERT; subclasses override for DELETE semantics.
     */
    protected RowKind writeRowKind() {
        return RowKind.INSERT;
    }

    /** Appends one row with an explicit CDC row kind. */
    protected void appendRowKind(TypedRow row, RowKind kind) {
        requireWriting();
        try {
            writeConvertedRow(rowConverter.convert(row, kind));
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to write CDC row to Paimon table: " + e.getMessage(), e);
        }
    }

    /**
     * Prepares commit metadata without performing the catalog commit.
     * Uses Paimon's native two-phase support: prepareCommit() produces
     * CommitMessage list, which is serialized and returned to the coordinator.
     * The coordinator performs the actual commit via
     * {@code PaimonMetadata.finishDistributedWrite()}.
     */
    private byte[] collectCommitMetadata() {
        try {
            List<CommitMessage> messages = tableWrite.prepareCommit();
            LOG.debug("event=paimon_write_prepared messageCount={} sinkType={}",
                    messages.size(), getClass().getSimpleName());
            return PaimonSdkWriteFragment.encode(table, output, commitEventType(), messages);
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to prepare Paimon commit metadata", e);
        }
    }

    /**
     * Returns the commit event type for this sink. Subclasses override
     * to indicate OVERWRITE semantics.
     */
    protected CommitEventType commitEventType() {
        return CommitEventType.APPEND;
    }

}
