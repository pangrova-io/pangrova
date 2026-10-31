package org.pangrova.connector.spi.metadata;

import org.pangrova.connector.spi.coordination.DistributedWriteCommitContext;
import org.pangrova.connector.spi.coordination.NativeWriteFinalization;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.ConnectorSession;

import java.util.List;

/** Catalog capability for a journal consumer that requires an exact committed Native write publication. */
@Experimental
public interface DistributedWritePublicationMetadata {
    /** Commits or reconciles the exact intent, including empty writes, and confirms its actual manifest. */
    NativeWriteFinalization finishDistributedWriteWithPublication(
            ConnectorSession session, ConnectorOutputTableHandle outputHandle,
            List<byte[]> fragments, DistributedWriteCommitContext context);
}
