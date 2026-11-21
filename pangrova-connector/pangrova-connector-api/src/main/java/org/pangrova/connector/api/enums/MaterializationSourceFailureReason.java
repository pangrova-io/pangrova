package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Stable;

/** Stable failure classification understood by the kernel. */
@Stable(since = "1.0")
public enum MaterializationSourceFailureReason {

    /** The source table does not exist or is not readable by this provider. */
    SOURCE_UNAVAILABLE,

    /** The connector cannot retain the observed snapshot. */
    PIN_UNSUPPORTED,

    /** The pinned snapshot has been expired or removed. */
    SNAPSHOT_UNAVAILABLE,

    /** The two snapshots were not observed against the same schema and policy shape. */
    SHAPE_MISMATCH,

    /** The connector cannot serve the requested change-set mode. */
    CHANGESET_UNSUPPORTED,

    /** The connector cannot prove that the change set is fully evaluated. */
    PROOF_INCOMPLETE
}
