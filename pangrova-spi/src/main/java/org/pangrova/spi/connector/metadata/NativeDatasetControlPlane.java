package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;

import java.util.List;
import java.util.OptionalLong;

/**
 * Metadata-only control of a Native dataset through authenticated Worker execution.
 * Implementations must not load JNI in the coordinator or transfer data rows.
 */
@Experimental
public interface NativeDatasetControlPlane {
    /**
     * Returns the Native storage schema's SHA-256 for these exact ordered columns.
     * A logical/JSON schema hash is not a substitute for the physical storage schema.
     */
    byte[] schemaFingerprint(ConnectorSession session, List<ConnectorTableMetadata.Column> columns);

    /** Enumerates bounded physical partitions from one immutable dataset manifest, without rows. */
    NativeDatasetScanPage scanPartitions(ConnectorSession session, String datasetUri, long version,
            byte[] schemaFingerprint, OptionalLong afterPartitionId, int pageSize);

    /**
     * Finalizes the immutable winners in an already durable write intent. A COMMITTED
     * result must have its publication identity durably journaled before returning;
     * the connector then performs the catalog visibility CAS. An uncertain RPC
     * outcome must be UNKNOWN, never NOT_COMMITTED.
     */
    NativeWriteFinalization finalizeWrite(ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments, DistributedWriteCommitContext context);
}
