package org.pangrova.persistence.query.api.enums;

import org.pangrova.common.api.annotation.Stable;

/** Durable outcome of a journaled write intent. */
@Stable(since = "1.0")
public enum WriteCommitState {
    /** Intent recorded; commit not yet executed. */
    PENDING,
    /** Commit succeeded; safe to skip on retry. */
    COMMITTED,
    /** Confirmed abort; the immutable intent cannot be resubmitted. */
    ABORTED
}
