package org.pangrova.persistence.query.api.commit;

import org.pangrova.connector.api.enums.NativeWriteCommitOutcome;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.commit.NativeWriteFinalization;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;

import org.pangrova.common.api.annotation.Stable;

import java.util.List;
import java.util.Objects;
import org.pangrova.common.file.CopyFileCommitBinding;

import static org.pangrova.common.util.Preconditions.requireText;

/**
 * Represents a write commit intent recorded in the {@link WriteCommitJournal}.
 *
 * <p>Each entry tracks a single distributed write operation from intent through
 * completion. The lifecycle is:
 * <ol>
 *   <li>{@code PENDING} — recorded before {@code finishDistributedWrite()} is called</li>
 *   <li>{@code COMMITTED} — updated after the commit succeeds</li>
 *   <li>{@code ABORTED} — confirmed unpublished; retained as a retry barrier</li>
 * </ol>
 *
 * <p>On coordinator crash recovery, entries in {@code COMMITTED} state indicate
 * writes that already succeeded and must not be retried, providing idempotency.</p>
 *
 * @param queryId        the query identifier (unique per write operation)
 * @param catalogName    the target catalog name
 * @param tableName      the fully qualified target table name
 * @param state          the pending or terminal commit state
 * @param timestampMs    epoch milliseconds when the entry was created or updated
 * @param fragments      serialized worker commit fragments needed to replay the commit
 * @param writeMode      the distributed write mode (e.g. INSERT/OVERWRITE/DELETE)
 * @param idempotencyKey stable key that dedups replayed commits (the commit fingerprint)
 * @param tenantId       authenticated tenant that owns the catalog write
 * @param principal      authenticated writer, retained without credentials or transient identity groups
 * @param authorizationReceipt exact admitted write authorization and definition receipt
 * @param baseSnapshotId snapshot captured when the write began (-1 for no snapshot)
 * @param writeOutputBinding exact canonical v2 binding dispatched to the Worker
 * @param commitFingerprint canonical SHA-256 identity of the immutable commit payload
 * @param ownerId        coordinator node that recorded the intent (nullable)
 * @param ownerIncarnation exact process incarnation that recorded it (nullable)
 * @param executionFencingToken backend-issued execution token (0 without HA)
 * @param publication confirmed external publication, persisted before catalog visibility (nullable)
 * @param copyFile exact COPY claim generation, absent for ordinary distributed writes
 * @param cdcSource exact CDC stream ownership and offset interval, absent for non-CDC writes
 */
@Stable(since = "1.0")
public record WriteCommitEntry(
        String queryId,
        String catalogName,
        String tableName,
        WriteCommitState state,
        long timestampMs,
        List<byte[]> fragments,
        String writeMode,
        String idempotencyKey,
        String tenantId,
        String principal,
        WriteAuthorizationReceipt authorizationReceipt,
        long baseSnapshotId,
        byte[] writeOutputBinding,
        String commitFingerprint,
        String ownerId,
        String ownerIncarnation,
        long executionFencingToken,
        NativeWriteFinalization publication,
        CopyFileCommitBinding copyFile,
        CdcSourceIdentity cdcSource) {

    public WriteCommitEntry {
        requireText(queryId, "queryId");
        requireText(catalogName, "catalogName");
        requireText(tableName, "tableName");
        Objects.requireNonNull(state, "state must not be null");
        if (timestampMs <= 0) {
            throw new IllegalArgumentException("timestampMs must be positive");
        }
        // A-COMMIT-01: the entry is a REPLAYABLE redo record, not just a state marker.
        // Normalize the replay payload so a recovering coordinator can reconstruct the commit.
        fragments = fragments == null
                ? List.of()
                : fragments.stream()
                        .map(fragment -> Objects.requireNonNull(
                                fragment, "commit fragment must not be null").clone())
                        .toList();
        requireText(writeMode, "writeMode");
        requireText(tenantId, "tenantId");
        requireText(principal, "principal");
        Objects.requireNonNull(authorizationReceipt, "authorizationReceipt");
        if (baseSnapshotId < -1) {
            throw new IllegalArgumentException("baseSnapshotId must be -1 or non-negative");
        }
        writeOutputBinding = Objects.requireNonNull(
                writeOutputBinding, "writeOutputBinding must not be null").clone();
        if (writeOutputBinding.length == 0) {
            throw new IllegalArgumentException("writeOutputBinding must not be empty");
        }
        String expectedFingerprint = WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent(queryId, tenantId, catalogName, tableName, writeMode,
                        baseSnapshotId, writeOutputBinding, principal, authorizationReceipt, copyFile,
                        cdcSource), fragments);
        if (!expectedFingerprint.equals(commitFingerprint)) {
            throw new IllegalArgumentException(
                    "commitFingerprint does not match the immutable write intent");
        }
        if (!commitFingerprint.equals(idempotencyKey)) {
            throw new IllegalArgumentException("idempotencyKey must equal commitFingerprint");
        }
        validateOwner(ownerId, ownerIncarnation, executionFencingToken);
        validatePublication(publication);
        if (state == WriteCommitState.ABORTED && publication != null) {
            throw new IllegalArgumentException("A published write cannot be marked aborted");
        }
    }

    private static void validatePublication(NativeWriteFinalization publication) {
        if (publication != null && publication.outcome() != NativeWriteCommitOutcome.COMMITTED) {
            throw new IllegalArgumentException("Only confirmed publication evidence may be persisted");
        }
    }

    private static void validateOwner(
            String ownerId, String ownerIncarnation, long executionFencingToken) {
        if ((ownerId == null) != (ownerIncarnation == null)) {
            throw new IllegalArgumentException(
                    "ownerId and ownerIncarnation must both be present or absent");
        }
        if (ownerId == null && executionFencingToken != 0) {
            throw new IllegalArgumentException(
                    "unowned intent cannot carry an execution fencing token");
        }
        if (ownerId != null && executionFencingToken <= 0) {
            throw new IllegalArgumentException(
                    "owned intent requires a positive execution fencing token");
        }
    }

    /** Returns this intent with the exact external publication; a conflicting replay is rejected. */
    public WriteCommitEntry withPublication(NativeWriteFinalization confirmed) {
        Objects.requireNonNull(confirmed, "confirmed must not be null");
        if (publication != null && !publication.equals(confirmed)) {
            throw new IllegalArgumentException("External publication identity differs from durable evidence");
        }
        return new WriteCommitEntry(queryId, catalogName, tableName, state, timestampMs, fragments,
                writeMode, idempotencyKey, tenantId, principal, authorizationReceipt,
                baseSnapshotId, writeOutputBinding,
                commitFingerprint, ownerId, ownerIncarnation, executionFencingToken, confirmed, copyFile,
                cdcSource);
    }

    /** Applies a monotonic lifecycle transition without dropping the immutable replay identity. */
    public WriteCommitEntry withState(WriteCommitState next, long timestamp) {
        Objects.requireNonNull(next, "next must not be null");
        if (state != WriteCommitState.PENDING && state != next) {
            throw new IllegalStateException("A terminal write intent cannot change state");
        }
        return new WriteCommitEntry(queryId, catalogName, tableName, next, timestamp, fragments,
                writeMode, idempotencyKey, tenantId, principal, authorizationReceipt,
                baseSnapshotId, writeOutputBinding, commitFingerprint, ownerId,
                ownerIncarnation, executionFencingToken, publication, copyFile, cdcSource);
    }

    /** Returns the exact connector commit context persisted with this intent. */
    public DistributedWriteCommitContext commitContext() {
        org.pangrova.connector.api.commit.CdcSourceCommitIdentity connectorCdc =
                cdcSource == null ? null
                        : new org.pangrova.connector.api.commit.CdcSourceCommitIdentity(
                                cdcSource.streamId(), cdcSource.sourceEpoch(),
                                cdcSource.ranges().stream()
                                        .map(range ->
                                                new org.pangrova.connector.api.commit
                                                        .CdcSourceCommitIdentity.OffsetRange(
                                                        range.topic(), range.partition(),
                                                        range.startOffset(), range.endOffset()))
                                        .toList());
        return new DistributedWriteCommitContext(
                queryId, tenantId, baseSnapshotId, writeOutputBinding, commitFingerprint,
                principal, authorizationReceipt, copyFile, connectorCdc);
    }

    /**
     * Returns defensive copies so callers cannot mutate the redo payload after it has been
     * validated or while another process is persisting/replaying it.
     */
    @Override
    public List<byte[]> fragments() {
        return fragments.stream().map(byte[]::clone).toList();
    }

    @Override
    public byte[] writeOutputBinding() {
        return writeOutputBinding.clone();
    }
}
