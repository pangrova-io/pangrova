package org.pangrova.connector.paimon;

import org.pangrova.common.concurrent.RetryingCommitWrapper;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
                    List<String> removedFiles, String snapshotId, CommitEventType eventType,
                    String commitFingerprint);
    }

    /** Deserialized commit fragments with the commit event type they declare. */
    record DeserializedFragments(List<CommitMessage> messages, CommitEventType eventType) {}

    /** Finishes the distributed write for the given output handle. */
    static void finish(Table freshTable, PaimonOutputTableHandle handle,
                       List<byte[]> fragments, DistributedWriteCommitContext context,
                       CommitNotifier notifier) {
        DeserializedFragments parsed = deserialize(fragments, handle);
        List<CommitMessage> allMessages = parsed.messages();
        CommitEventType eventType = parsed.eventType();
        if (allMessages.isEmpty() && eventType != CommitEventType.OVERWRITE) {
            LOG.debug("event=paimon_distributed_write_empty schema={} table={}",
                    handle.schemaName(), handle.tableName());
            return;
        }

        if (!commitMessages(freshTable, handle, allMessages, eventType, context)) {
            LOG.info("event=paimon_distributed_write_already_committed schema={} table={} fingerprint={}",
                    handle.schemaName(), handle.tableName(), context.commitFingerprint());
        } else {
            LOG.info("event=paimon_distributed_write_committed schema={} table={} messageCount={}",
                    handle.schemaName(), handle.tableName(), allMessages.size());
        }
        notifyAfterCommit(freshTable, handle, allMessages, eventType,
                context.commitFingerprint(), notifier);
    }

    private static DeserializedFragments deserialize(
            List<byte[]> fragments, PaimonOutputTableHandle handle) {
        List<CommitMessage> allMessages = new ArrayList<>();
        CommitEventType eventType = null;
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        for (byte[] fragment : fragments) {
            if (fragment == null || fragment.length == 0) {
                continue;
            }
            CommitEventType fragmentEvent = readFragment(
                    fragment, serializer, allMessages, handle);
            if (eventType != null && eventType != fragmentEvent) {
                throw invalidFragment("Mixed Paimon write operations in one commit");
            }
            eventType = fragmentEvent;
        }
        if (eventType == null) {
            throw invalidFragment("Paimon distributed commit has no typed fragments");
        }
        if (eventType != expectedEventType(handle.mode())) {
            throw invalidFragment("Paimon fragment operation differs from the bound write mode");
        }
        return new DeserializedFragments(allMessages, eventType);
    }

    private static CommitEventType expectedEventType(String mode) {
        return switch (mode) {
            case "INSERT" -> CommitEventType.APPEND;
            case "OVERWRITE" -> CommitEventType.OVERWRITE;
            case "DELETE" -> CommitEventType.DELETE;
            default -> throw invalidFragment("Unsupported Paimon write mode");
        };
    }

    private static CommitEventType readFragment(byte[] fragment, CommitMessageSerializer serializer,
                                                List<CommitMessage> allMessages,
                                                PaimonOutputTableHandle handle) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(fragment))) {
            int version = in.readInt();
            String operation = in.readUTF();
            String schema = in.readUTF();
            String table = in.readUTF();
            if (!handle.schemaName().equals(schema) || !handle.tableName().equals(table)) {
                throw invalidFragment("Paimon fragment target differs from the bound table");
            }
            readMessages(in, serializer, version, allMessages);
            if (in.available() != 0) {
                throw invalidFragment("Trailing bytes in Paimon commit fragment");
            }
            return CommitEventType.valueOf(operation);
        } catch (IOException | IllegalArgumentException e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to deserialize Paimon commit messages", e);
        }
    }

    private static void readMessages(DataInputStream in, CommitMessageSerializer serializer, int version,
            List<CommitMessage> messages) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available() / Integer.BYTES) {
            throw invalidFragment("Invalid Paimon commit message count");
        }
        for (int i = 0; i < count; i++) {
            int length = in.readInt();
            if (length < 0 || length > in.available()) {
                throw invalidFragment("Invalid Paimon commit message length");
            }
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            messages.add(serializer.deserialize(version, bytes));
        }
    }

    private static ConnectorException invalidFragment(String detail) {
        return new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR, detail);
    }

    private static boolean commitMessages(Table freshTable, PaimonOutputTableHandle handle,
                                          List<CommitMessage> allMessages, CommitEventType eventType,
                                          DistributedWriteCommitContext context) {
        if (!(freshTable instanceof FileStoreTable fileStore)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon distributed recovery requires a FileStoreTable");
        }
        var snapshots = fileStore.snapshotManager();
        String commitUser = context.commitFingerprint();
        if (snapshots.latestSnapshotOfUser(commitUser).isPresent()) {
            return false;
        }
        org.pangrova.connector.paimon.handle.PaimonTableHandleFactory.requireWriteDeclarationSupported(
                freshTable.options());
        validateReplayWindow(context, snapshots);
        InnerTable innerTable = requireCommitTable(freshTable, handle, eventType);
        commitPrepared(innerTable, handle, allMessages, eventType, commitUser);
        return true;
    }

    private static void validateReplayWindow(
            DistributedWriteCommitContext context,
            SnapshotManager snapshots) {
        Long earliest = snapshots.earliestSnapshotId();
        if (context.baseSnapshotId() >= 0
                && !snapshots.snapshotExists(context.baseSnapshotId())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon base snapshot expired before commit replay; outcome is ambiguous");
        }
        if (context.baseSnapshotId() < 0 && earliest != null && earliest > 1) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon initial snapshot history expired before commit replay");
        }
    }

    private static InnerTable requireCommitTable(
            Table freshTable, PaimonOutputTableHandle handle, CommitEventType eventType) {
        Table commitTable = freshTable;
        if (eventType == CommitEventType.OVERWRITE) {
            // The SPI overwrite replaces all data selected by the handle, including
            // partitions with no surviving rows. SDK dynamic overwrite only replaces
            // partitions present in the new files and would leave deleted rows behind.
            commitTable = freshTable.copy(Map.of(
                    CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false"));
        }
        if (!(commitTable instanceof InnerTable innerTable)) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon table does not expose a replay-safe commit implementation");
        }
        return innerTable;
    }

    private static void commitPrepared(
            InnerTable innerTable, PaimonOutputTableHandle handle,
            List<CommitMessage> allMessages, CommitEventType eventType, String commitUser) {
        try {
            COMMIT_RETRY.execute(() -> {
                try (InnerTableCommit tableCommit = innerTable.newCommit(commitUser)) {
                    if (eventType == CommitEventType.OVERWRITE) {
                        tableCommit.withOverwrite(handle.overwritePartition());
                    }
                    tableCommit.filterAndCommit(Map.of(0L, allMessages));
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
                                          CommitEventType eventType, String commitFingerprint,
                                          CommitNotifier notifier) {
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

        FileStoreTable fileStore = (FileStoreTable) freshTable;
        Snapshot snapshot = fileStore.snapshotManager().latestSnapshotOfUser(commitFingerprint)
                .orElseThrow(() -> new IllegalStateException(
                        "Paimon committed snapshot marker is absent after write completion"));
        String snapshotId = String.valueOf(snapshot.id());
        notifier.notify(handle.schemaName(), handle.tableName(),
                addedFiles, removedFiles, snapshotId, eventType, commitFingerprint);
    }
}
