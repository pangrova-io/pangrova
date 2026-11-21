package org.pangrova.coordinator.streaming;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.persistence.query.api.commit.WriteCommitEntry;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;
import org.pangrova.persistence.query.api.enums.WriteCommitState;
import org.pangrova.streaming.api.checkpoint.StreamState;

import java.util.List;
import java.util.Objects;

/** Builds the durable journal intent for one completed set of CDC Worker fragments. */
public final class CdcWriteIntentFactory {

    private CdcWriteIntentFactory() {}

    /** Returns a PENDING intent whose fingerprint binds the exact fragments and CDC source. */
    public static WriteCommitEntry create(
            String queryId,
            StreamState stream,
            ConnectorSession session,
            CdcWriteAuthority.CdcWritePlan writePlan,
            CdcSourceIdentity source,
            List<byte[]> fragments,
            long baseSnapshotId,
            long timestampMs,
            String ownerId,
            String ownerIncarnation,
            long executionFencingToken) {
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(writePlan, "writePlan");
        Objects.requireNonNull(source, "source");
        QualifiedTableName target = QualifiedTableName.resolve(
                List.of(stream.targetTable().split("\\.")));
        String tableName = target.schema() + "." + target.table();
        String writeMode = writePlan.allocationIntent().sqlWriteMode();
        byte[] binding = writePlan.writeBinding().toByteArray();
        List<byte[]> ownedFragments = fragments == null ? List.of() : fragments;
        String fingerprint = WriteCommitFingerprint.compute(
                new WriteCommitFingerprint.Intent(queryId, stream.tenantId(), target.catalog(),
                        tableName, writeMode, baseSnapshotId, binding,
                        session.getUser(), writePlan.authorizationReceipt(), null, source),
                ownedFragments);
        return new WriteCommitEntry(queryId, target.catalog(), tableName,
                WriteCommitState.PENDING, timestampMs, ownedFragments, writeMode,
                fingerprint, stream.tenantId(), session.getUser(),
                writePlan.authorizationReceipt(), baseSnapshotId, binding,
                fingerprint, ownerId, ownerIncarnation, executionFencingToken,
                null, null, source);
    }
}
