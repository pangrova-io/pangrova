package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;

import org.pangrova.connector.api.commit.DistributedWriteCommitContext;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.common.file.CopyFilePublication;

/** Connector capability that proves an exact COPY commit in the current table history. */
@Experimental
public interface CopyFilePublicationMetadata {

    CopyFilePublication confirmCopyFilePublication(
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle,
            DistributedWriteCommitContext context);
}
