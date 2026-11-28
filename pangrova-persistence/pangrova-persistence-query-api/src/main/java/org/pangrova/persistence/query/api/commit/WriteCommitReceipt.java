package org.pangrova.persistence.query.api.commit;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.api.commit.NativeWriteFinalization;
import org.pangrova.persistence.query.api.enums.WriteCommitState;

import static org.pangrova.common.validation.Preconditions.requireText;

/**
 * Compact terminal evidence of one exact write. The fingerprint binds the original authorization,
 * connector output allocation and fragments, but the receipt is not a replayable redo record.
 * A COPY or CDC consumer must still consult its own source-specific publication authority.
 */
@Stable(since = "1.0")
public record WriteCommitReceipt(String queryId, String tenantId, String catalogName, String tableName,
        String writeMode, long baseSnapshotId, String commitFingerprint, WriteCommitState state,
        long finalizedAtMs, boolean copyWrite, boolean cdcWrite, NativeWriteFinalization publication) {

    public WriteCommitReceipt {
        requireText(queryId, "queryId");
        requireText(tenantId, "tenantId");
        requireText(catalogName, "catalogName");
        requireText(tableName, "tableName");
        requireText(writeMode, "writeMode");
        if (baseSnapshotId < -1 || commitFingerprint == null || !commitFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Invalid immutable write receipt identity");
        }
        if (state != WriteCommitState.COMMITTED && state != WriteCommitState.ABORTED) {
            throw new IllegalArgumentException("Write receipt must describe a terminal state");
        }
        if (finalizedAtMs <= 0 || (state == WriteCommitState.ABORTED && publication != null)) {
            throw new IllegalArgumentException("Invalid terminal write evidence");
        }
    }
}
