package org.pangrova.persistence.query.api.commit;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.persistence.query.api.enums.WriteCommitState;

import java.util.Objects;

import static org.pangrova.common.util.Preconditions.requireText;

/** Exact journal identity and source ranges needed to decide whether CDC can admit another batch. */
@Stable(since = "1.0")
public record CdcWriteCheckpoint(
        String queryId, String commitFingerprint, WriteCommitState state, CdcSourceIdentity cdcSource) {

    public CdcWriteCheckpoint {
        requireText(queryId, "queryId");
        requireText(commitFingerprint, "commitFingerprint");
        if (state != WriteCommitState.PENDING && state != WriteCommitState.COMMITTED) {
            throw new IllegalArgumentException("CDC checkpoint must be pending or committed");
        }
        Objects.requireNonNull(cdcSource, "cdcSource");
    }
}
