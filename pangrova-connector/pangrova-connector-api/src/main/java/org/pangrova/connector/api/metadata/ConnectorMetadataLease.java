package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Stable;

/** Holds the connector resources needed by one begun metadata instance. */
@Stable(since = "1.0")
public interface ConnectorMetadataLease {
    ConnectorMetadata metadata();

    /** Releases the resources after successful metadata cleanup. A failure remains retryable. */
    void release();
}
