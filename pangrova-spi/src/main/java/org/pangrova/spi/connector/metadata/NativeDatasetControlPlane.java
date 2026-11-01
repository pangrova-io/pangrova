package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.connector.coordination.DistributedWriteCommitContext;

import org.pangrova.common.api.annotation.Experimental;
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
    NativeDatasetScanPage scanPartitions(ConnectorSession session, AcceptedSourceSnapshot snapshot,
            OptionalLong afterPartitionId, int pageSize);

    /** Executes only an exact authoritative deletion batch and returns after actual SDK deletion joins. */
    void cleanupSourceObjects(ConnectorSession session, SourcePublicationReservation reservation);

    /** Prepares one immutable SDK manifest at the real PG reservation; UNKNOWN retains it for exact replay. */
    NativeWritePreparation prepareWrite(ConnectorSession session, ConnectorOutputTableHandle output,
            List<byte[]> fragments, DistributedWriteCommitContext context, SourcePublicationReservation reservation);
}
