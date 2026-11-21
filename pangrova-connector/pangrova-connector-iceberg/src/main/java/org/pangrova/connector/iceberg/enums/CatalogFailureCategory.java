package org.pangrova.connector.iceberg.enums;

import org.pangrova.common.api.annotation.Internal;

/** Classifies catalog failures for retry and access-denial accounting. */
@Internal
public enum CatalogFailureCategory {
    /** Transient I/O failure — caller should retry with backoff. */
    IO_TRANSIENT,
    /** Access denied — caller must rethrow immediately, no retry. */
    ACCESS_DENIED,
    /** Other unclassified failure. */
    OTHER
}
