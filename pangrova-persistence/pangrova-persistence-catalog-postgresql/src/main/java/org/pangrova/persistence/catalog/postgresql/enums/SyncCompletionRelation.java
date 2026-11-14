package org.pangrova.persistence.catalog.postgresql.enums;

import org.pangrova.common.api.annotation.Internal;

/** Whether the operation consults the durable sync intent or completion receipt. */
@Internal
public enum SyncCompletionRelation {
    INTENT,
    RECEIPT
}
