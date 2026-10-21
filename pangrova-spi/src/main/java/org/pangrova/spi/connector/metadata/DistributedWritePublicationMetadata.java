package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.ConnectorSession;

import java.util.List;

/** Catalog capability for a journal consumer that requires an exact committed Native write publication. */
@Experimental
public interface DistributedWritePublicationMetadata {
    /** Commits or reconciles the exact intent, including empty writes, and confirms its actual manifest. */
    NativeWriteFinalization finishDistributedWriteWithPublication(
            ConnectorSession session, ConnectorOutputTableHandle outputHandle,
            List<byte[]> fragments, DistributedWriteCommitContext context);
}
