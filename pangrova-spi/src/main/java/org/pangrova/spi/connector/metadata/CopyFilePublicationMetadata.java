package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.coordination.CopyFilePublication;

/** Connector capability that proves an exact COPY commit in the current table history. */
@Experimental
public interface CopyFilePublicationMetadata {

    CopyFilePublication confirmCopyFilePublication(
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            DistributedWriteCommitContext context);
}
