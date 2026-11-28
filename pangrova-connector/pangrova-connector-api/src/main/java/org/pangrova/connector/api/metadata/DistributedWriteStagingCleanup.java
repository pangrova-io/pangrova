package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.connector.api.session.ConnectorSession;

import java.util.List;

/** Removes exact closed-object evidence after the Coordinator has fenced publication and drained the writers. */
@Experimental
public interface DistributedWriteStagingCleanup {
    /**
     * Reclaims the observed objects. Returns false if this deployment has no supported storage authority.
     * An acknowledgement covers only the objects named in the supplied fragments.
     */
    boolean reclaimObservedStaging(ConnectorSession session, ConnectorOutputTableHandle verifiedOutput,
            List<byte[]> fragments,
            DistributedWriteCommitContext context);
}
