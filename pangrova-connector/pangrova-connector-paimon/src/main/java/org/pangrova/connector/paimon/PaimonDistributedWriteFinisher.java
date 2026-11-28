package org.pangrova.connector.paimon;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
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
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Finishes a distributed Paimon write: deserializes commit-message fragments
 * produced by workers, commits them through the SDK, and notifies the commit
 * listener about added and removed data files.
 */
final class PaimonDistributedWriteFinisher {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDistributedWriteFinisher.class);

    private PaimonDistributedWriteFinisher() {}

    /** Callback used to notify commit listeners after a successful commit. */
    @FunctionalInterface
    interface CommitNotifier {
        void notify(String schemaName, String tableName, List<String> addedFiles,
                    List<String> removedFiles, String snapshotId, CommitEventType eventType,
                    String commitFingerprint);
    }

    /** Deserialized commit fragments with the commit event type they declare. */
    record DeserializedFragments(List<CommitMessage> messages, CommitEventType eventType) {}

    /** Publishes validated SDK messages and reuses the exact snapshot observed during preparation. */
    static void finish(FileStoreTable table, PaimonOutputTableHandle handle,
            DeserializedFragments parsed, DistributedWriteCommitContext context,
            Supplier<Optional<Snapshot>> existingPublication, CommitNotifier notifier) {
        List<CommitMessage> allMessages = parsed.messages();
        CommitEventType eventType = parsed.eventType();
        if (allMessages.isEmpty() && eventType != CommitEventType.OVERWRITE) {
            LOG.debug("event=paimon_distributed_write_empty schema={} table={}",
                    handle.schemaName(), handle.tableName());
            return;
        }

        var existing = existingPublication.get();
        Snapshot publication = existing.isPresent() ? existing.orElseThrow()
                : commitMessages(table, handle, allMessages, eventType, context);
        notifyAfterCommit(publication, handle, allMessages, eventType, context.commitFingerprint(), notifier);
    }

    static DeserializedFragments deserialize(
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
        if ("MERGE".equals(handle.mode())) {
            if (eventType != CommitEventType.MERGE) {
                throw invalidFragment("Paimon CDC merge requires a MERGE fragment");
            }
        } else if (eventType != expectedEventType(handle.mode())) {
            throw invalidFragment("Paimon fragment operation differs from the bound write mode");
        }
        return new DeserializedFragments(allMessages, eventType);
    }

    private static CommitEventType expectedEventType(String mode) {
        return switch (mode) {
            case "INSERT" -> CommitEventType.APPEND;
            case "OVERWRITE" -> CommitEventType.OVERWRITE;
            case "DELETE" -> CommitEventType.DELETE;
            case "MERGE" -> CommitEventType.MERGE;
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

    private static Snapshot commitMessages(FileStoreTable freshTable, PaimonOutputTableHandle handle,
            List<CommitMessage> allMessages, CommitEventType eventType, DistributedWriteCommitContext context) {
        var snapshots = freshTable.snapshotManager();
        String commitUser = context.commitFingerprint();
        org.pangrova.connector.paimon.handle.PaimonTableHandleFactory.requireWriteDeclarationSupported(
                freshTable.options());
        validateReplayWindow(context, snapshots);
        InnerTable innerTable = requireCommitTable(freshTable, handle, eventType);
        if (!context.tryBeginPublication()) {
            return snapshots.latestSnapshotOfUser(commitUser).orElseThrow(() ->
                    new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                            "Paimon publication previously started without a confirmed snapshot"));
        }
        commitPrepared(innerTable, handle, allMessages, eventType, commitUser);
        var publication = snapshots.latestSnapshotOfUser(commitUser).orElseThrow(() ->
                new IllegalStateException("Paimon committed snapshot marker is absent after write completion"));
        LOG.info("event=paimon_distributed_write_committed schema={} table={} messageCount={}",
                handle.schemaName(), handle.tableName(), allMessages.size());
        return publication;
    }

    private static void validateReplayWindow(
            DistributedWriteCommitContext context,
            SnapshotManager snapshots) {
        if (context.baseSnapshotId() >= 0
                && !snapshots.snapshotExists(context.baseSnapshotId())) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Paimon base snapshot expired before commit replay; outcome is ambiguous");
        }
        if (context.baseSnapshotId() < 0) {
            Long earliest = snapshots.earliestSnapshotId();
            if (earliest != null && earliest > 1) {
                throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                        "Paimon initial snapshot history expired before commit replay");
            }
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
        // The SDK owns conflict retry and snapshot idempotency state. Exceptions
        // escaping it can mean an atomic rename is still in flight; message
        // matching cannot prove failure or justify restarting the SDK commit.
        try (InnerTableCommit tableCommit = innerTable.newCommit(commitUser)) {
            if (eventType == CommitEventType.OVERWRITE) {
                tableCommit.withOverwrite(handle.overwritePartition());
            }
            tableCommit.filterAndCommit(Map.of(0L, allMessages));
        } catch (Exception e) {
            throw new ConnectorException(PangrovaErrorCode.CONNECTOR_WRITE_ERROR,
                    "Failed to commit Paimon distributed write; reconcile its durable fingerprint before replay", e);
        }
    }

    private static void notifyAfterCommit(Snapshot publication, PaimonOutputTableHandle handle,
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

        String snapshotId = String.valueOf(publication.id());
        notifier.notify(handle.schemaName(), handle.tableName(),
                addedFiles, removedFiles, snapshotId, eventType, commitFingerprint);
    }
}
