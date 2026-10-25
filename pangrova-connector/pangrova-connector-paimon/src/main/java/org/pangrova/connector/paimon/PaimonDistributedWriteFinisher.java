package org.pangrova.connector.paimon;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
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

    /**
     * Deletes data files staged by a distributed write that was never published.
     *
     * <p>The files referenced by the fragments are not yet part of any snapshot. If the table
     * advanced past the write's base snapshot, another writer may have committed concurrently, so
     * deletion is skipped and the files are left for orphan maintenance instead of risking a
     * committed file.
     */
    static void abort(Table freshTable, PaimonOutputTableHandle handle, List<byte[]> fragments) {
        FileStoreTable fileStore = org.pangrova.connector.paimon.handle.PaimonTableIdentity.requireSame(
                freshTable, handle.tableUuid(), handle.tableLocation(), handle.schemaFingerprint(),
                "Paimon distributed write abort");
        if (fragments == null || fragments.isEmpty()
                || fragments.stream().allMatch(fragment -> fragment == null || fragment.length == 0)) {
            return;
        }
        DeserializedFragments parsed = deserialize(fragments, handle);
        Long latest = fileStore.snapshotManager().latestSnapshotId();
        if (latest != null && (handle.baseSnapshotId() < 0 || latest > handle.baseSnapshotId())) {
            LOG.warn("event=paimon_distributed_write_abort_skipped schema={} table={} "
                            + "reason=snapshot_advanced baseSnapshot={} latestSnapshot={}",
                    handle.schemaName(), handle.tableName(), handle.baseSnapshotId(), latest);
            return;
        }
        int deleted = 0;
        String tablePrefix = fileStore.location().toString();
        for (CommitMessage message : parsed.messages()) {
            if (!(message instanceof CommitMessageImpl impl)) {
                continue;
            }
            var increment = impl.newFilesIncrement();
            var pathFactory = fileStore.store().pathFactory()
                    .createDataFilePathFactory(impl.partition(), impl.bucket());
            for (DataFileMeta file : concat(increment.newFiles(), increment.changelogFiles())) {
                var path = pathFactory.toPath(file);
                if (!path.toString().startsWith(tablePrefix + "/")) {
                    throw invalidFragment("Paimon staged file is outside the target table location");
                }
                fileStore.fileIO().deleteQuietly(path);
                deleted++;
            }
        }
        LOG.info("event=paimon_distributed_write_aborted schema={} table={} deletedFiles={}",
                handle.schemaName(), handle.tableName(), deleted);
    }

    private static List<DataFileMeta> concat(List<DataFileMeta> first, List<DataFileMeta> second) {
        if (second.isEmpty()) {
            return first;
        }
        List<DataFileMeta> merged = new ArrayList<>(first.size() + second.size());
        merged.addAll(first);
        merged.addAll(second);
        return merged;
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
        FileStoreTable fileStore = org.pangrova.connector.paimon.handle.PaimonTableIdentity.requireSame(
                freshTable, handle.tableUuid(), handle.tableLocation(), handle.schemaFingerprint(),
                "Paimon distributed write");
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
