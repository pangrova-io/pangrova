package org.pangrova.connector.iceberg.enums;

import org.pangrova.common.api.annotation.Internal;

/** Result of checking the committed snapshot ancestry for the original write fingerprint. */
@Internal
public enum IcebergCommitOutcome {
    /** Commit succeeded — the snapshot carries our durable fingerprint marker. */
    SUCCEEDED,
    /** Cannot determine outcome — requires reconciliation, never an automatic retry. */
    UNKNOWN
}
