package org.pangrova.connector.api.metadata;

/** Holds the connector resources needed by one begun metadata instance. */
public interface ConnectorMetadataLease {
    ConnectorMetadata metadata();

    /** Releases the resources after successful metadata cleanup. A failure remains retryable. */
    void release();
}
