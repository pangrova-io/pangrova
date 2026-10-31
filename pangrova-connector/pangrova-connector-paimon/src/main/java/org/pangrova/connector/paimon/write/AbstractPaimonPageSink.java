package org.pangrova.connector.paimon.write;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.protocol.batch.ColumnarBatch;
import org.pangrova.protocol.batch.ColumnarPageSink;
import org.pangrova.connector.spi.base.ConnectorException;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.NativePage;
import org.pangrova.connector.spi.data.page.NativePageConverter;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.index.CommitEventType;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    private final String schemaName;
    private final String tableName;
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
        this.rowConverter = context.rowConverter();
        this.schemaName = context.schemaName();
        this.tableName = context.tableName();
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
    public void appendColumnarBatch(ColumnarBatch batch) {
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
        appendPage(NativePageConverter.toPage(page));
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
                    "Failed to close Paimon " + name + " for " + schemaName + "." + tableName, failure);
        }
    }

    /**
     * Returns the RowKind applied to every row written by this sink.
     * Default is INSERT; subclasses override for DELETE semantics.
     */
    protected RowKind writeRowKind() {
        return RowKind.INSERT;
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
            return serializeCommitMessages(messages);
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to prepare Paimon commit metadata", e);
        }
    }

    /**
     * Serializes Paimon CommitMessage objects using Paimon's native
     * {@link CommitMessageSerializer}. The binary format is:
     * {@code [version:int][operation:utf][schema:utf][table:utf][count:int][msg_len:int][msg_bytes]...}
     */
    private byte[] serializeCommitMessages(List<CommitMessage> messages) {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(serializer.getVersion());
            out.writeUTF(commitEventType().name());
            out.writeUTF(schemaName);
            out.writeUTF(tableName);
            out.writeInt(messages.size());
            for (CommitMessage msg : messages) {
                byte[] msgBytes = serializer.serialize(msg);
                out.writeInt(msgBytes.length);
                out.write(msgBytes);
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to serialize Paimon commit messages", e);
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
