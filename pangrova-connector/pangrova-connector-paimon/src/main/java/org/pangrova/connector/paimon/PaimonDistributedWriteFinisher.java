package org.pangrova.connector.paimon;

import org.pangrova.common.concurrent.RetryingCommitWrapper;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.index.CommitEventType;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Finishes a distributed Paimon write: deserializes commit-message fragments
 * produced by workers, commits them (with retries), and notifies the commit
 * listener about added and removed data files.
 */
final class PaimonDistributedWriteFinisher {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDistributedWriteFinisher.class);

    private static final RetryingCommitWrapper COMMIT_RETRY = RetryingCommitWrapper.withDefaults(
            PaimonDistributedWriteFinisher::isRetryableCommitException);

    private PaimonDistributedWriteFinisher() {}

    /**
     * Determines if an exception from Paimon commit is retryable based on SDK
     * exception types rather than fragile string matching.
     *
     * <p>Retryable: RuntimeException from commit path (OCC conflict, snapshot
     * version mismatch). Non-retryable: catalog-level permanent failures like
     * TableNotExistException, DatabaseNotExistException, or security exceptions.
     */
    private static boolean isRetryableCommitException(Exception e) {
        if (e instanceof Catalog.TableNotExistException
                || e instanceof Catalog.DatabaseNotExistException
                || e instanceof Catalog.TableNoPermissionException
                || e instanceof Catalog.DatabaseNoPermissionException) {
            return false;
        }
        if (e instanceof IllegalArgumentException || e instanceof IllegalStateException) {
            return false;
        }
        // Paimon commit conflicts surface as RuntimeException from
        // FileStoreCommitImpl with conflict-related messages. Any RuntimeException
        // from the commit path that is not a permanent failure is retryable.
        return e instanceof RuntimeException;
    }

    /** Callback used to notify commit listeners after a successful commit. */
    @FunctionalInterface
    interface CommitNotifier {
        void notify(String schemaName, String tableName, List<String> addedFiles,
                    List<String> removedFiles, String snapshotId, CommitEventType eventType);
    }

    /** Deserialized commit fragments with the commit event type they declare. */
    record DeserializedFragments(List<CommitMessage> messages, CommitEventType eventType) {}

    /** Finishes the distributed write for the given output handle. */
    static void finish(Table freshTable, PaimonOutputTableHandle handle,
                       List<byte[]> fragments, CommitNotifier notifier) {
        DeserializedFragments parsed = deserialize(fragments);
        List<CommitMessage> allMessages = parsed.messages();
        CommitEventType eventType = parsed.eventType();
        if (allMessages.isEmpty() && eventType != CommitEventType.OVERWRITE) {
            LOG.debug("event=paimon_distributed_write_empty schema={} table={}",
                    handle.schemaName(), handle.tableName());
            return;
        }

        commitMessages(freshTable, handle, allMessages, eventType);
        LOG.info("event=paimon_distributed_write_committed schema={} table={} messageCount={}",
                handle.schemaName(), handle.tableName(), allMessages.size());

        notifyAfterCommit(freshTable, handle, allMessages, eventType, notifier);
    }

    private static DeserializedFragments deserialize(List<byte[]> fragments) {
        List<CommitMessage> allMessages = new ArrayList<>();
        CommitEventType eventType = CommitEventType.APPEND;
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        for (byte[] fragment : fragments) {
            if (fragment == null || fragment.length == 0) {
                continue;
            }
            eventType = readFragment(fragment, serializer, allMessages);
        }
        return new DeserializedFragments(allMessages, eventType);
    }

    private static CommitEventType readFragment(byte[] fragment, CommitMessageSerializer serializer,
                                                List<CommitMessage> allMessages) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(fragment))) {
            int version = in.readInt();
            String operation = in.readUTF();
            in.readUTF(); // schema name (carried for wire-format compatibility)
            in.readUTF(); // table name (carried for wire-format compatibility)
            int messageCount = in.readInt();
            for (int i = 0; i < messageCount; i++) {
                int msgLen = in.readInt();
                byte[] msgBytes = new byte[msgLen];
                in.readFully(msgBytes);
                allMessages.add(serializer.deserialize(version, msgBytes));
            }
            return CommitEventType.valueOf(operation);
        } catch (IOException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to deserialize Paimon commit messages", e);
        }
    }

    private static void commitMessages(Table freshTable, PaimonOutputTableHandle handle,
                                       List<CommitMessage> allMessages, CommitEventType eventType) {
        BatchWriteBuilder writeBuilder = freshTable.newBatchWriteBuilder();
        if (eventType == CommitEventType.OVERWRITE) {
            writeBuilder = writeBuilder.withOverwrite(handle.overwritePartition());
        }
        final BatchWriteBuilder effectiveWriteBuilder = writeBuilder;
        try {
            COMMIT_RETRY.execute(() -> {
                try (BatchTableCommit tableCommit = effectiveWriteBuilder.newCommit()) {
                    tableCommit.commit(allMessages);
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (org.pangrova.common.exception.PangrovaException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to commit Paimon distributed write after retries", e);
        } catch (RuntimeException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to commit Paimon distributed write", e);
        }
    }

    private static void notifyAfterCommit(Table freshTable, PaimonOutputTableHandle handle,
                                          List<CommitMessage> allMessages,
                                          CommitEventType eventType, CommitNotifier notifier) {
        // Extract file names from commit messages for commit listener notification
        List<String> addedFiles = new ArrayList<>();
        List<String> removedFiles = new ArrayList<>();
        for (CommitMessage msg : allMessages) {
            if (msg instanceof CommitMessageImpl impl) {
                DataIncrement increment = impl.newFilesIncrement();
                for (DataFileMeta fileMeta : increment.newFiles()) {
                    addedFiles.add(fileMeta.fileName());
                }
                for (DataFileMeta fileMeta : increment.deletedFiles()) {
                    removedFiles.add(fileMeta.fileName());
                }
            }
        }

        Snapshot snapshot = null;
        if (freshTable instanceof FileStoreTable fst) {
            snapshot = fst.snapshotManager().latestSnapshot();
        }
        String snapshotId = snapshot != null ? String.valueOf(snapshot.id()) : "";
        notifier.notify(handle.schemaName(), handle.tableName(),
                addedFiles, removedFiles, snapshotId, eventType);
    }
}
