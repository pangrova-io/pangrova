package org.pangrova.connector.paimon.write;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.page.NativePage;
import org.pangrova.spi.connector.data.page.NativePageConverter;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.index.CommitEventType;
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
abstract class AbstractPaimonPageSink implements ConnectorPageSink {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPaimonPageSink.class);

    private final IOManager ioManager;
    private final BatchTableWrite tableWrite;
    private final BatchTableCommit tableCommit;
    private final PaimonRowConverter rowConverter;
    private final String schemaName;
    private final String tableName;
    private final boolean dynamicBucket;
    private final boolean postponeBucket;
    private boolean finished;
    private boolean aborted;

    protected AbstractPaimonPageSink(PaimonPageSinkContext context) {
        this.ioManager = new IOManagerImpl(context.tempDir());
        this.tableWrite = context.writeBuilder().newWrite();
        this.tableWrite.withIOManager(ioManager);
        this.tableCommit = context.writeBuilder().newCommit();
        this.rowConverter = context.rowConverter();
        this.schemaName = context.schemaName();
        this.tableName = context.tableName();
        this.dynamicBucket = context.dynamicBucket();
        this.postponeBucket = context.postponeBucket();
    }

    @Override
    public void appendPage(Page page) {
        for (TypedRow row : page.rows()) {
            writeRow(row);
        }
    }

    @Override
    public void appendNativePage(NativePage page) {
        appendPage(NativePageConverter.toPage(page));
    }

    @Override
    public byte[] finishWithMetadata() {
        if (finished || aborted) {
            return new byte[0];
        }
        finished = true;
        return collectCommitMetadata();
    }

    @Override
    public void abort() {
        if (finished || aborted) {
            return;
        }
        aborted = true;
        ResourceCloser.closeQuietly(tableWrite);
    }

    @Override
    public void close() {
        ResourceCloser.closeQuietly(tableWrite);
        ResourceCloser.closeQuietly(tableCommit);
        ResourceCloser.closeQuietly(ioManager);
    }

    private void writeRow(TypedRow row) {
        try {
            if (dynamicBucket) {
                tableWrite.write(rowConverter.convert(row, writeRowKind()), 0);
            } else if (postponeBucket) {
                // Postpone bucket (bucket=-2): let Paimon's internal
                // PostponeBucketRowKeyExtractor assign bucket=-2
                tableWrite.write(rowConverter.convert(row, writeRowKind()));
            } else {
                tableWrite.write(rowConverter.convert(row, writeRowKind()));
            }
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to write row to Paimon table: " + e.getMessage(), e);
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
