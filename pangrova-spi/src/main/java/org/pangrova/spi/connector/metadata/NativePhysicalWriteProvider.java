package org.pangrova.spi.connector.metadata;

import org.pangrova.spi.connector.coordination.DistributedWriteCommitContext;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.spi.connector.ConnectorSession;

import java.util.List;

/** Connector-owned metadata adapter; implementations must not execute rows or load coordinator JNI. */
@Experimental
public interface NativePhysicalWriteProvider {
    /** Validates exact decoded output allocation before a Worker creates any objects. */
    void validateAllocation(ConnectorSession session, ConnectorOutputTableHandle output,
                            NativePhysicalWriteAllocation allocation);

    /** Produces SDK commit messages only after every winning object and receipt has been verified. */
    List<byte[]> prepareCommitMetadata(ConnectorSession session, ConnectorOutputTableHandle output,
                                       List<NativePhysicalFileReceipt> receipts,
                                       DistributedWriteCommitContext context);

    /** Deletes only exact uncommitted allocations whose publication is known not to have occurred. */
    void abortUncommittedFiles(ConnectorSession session, ConnectorOutputTableHandle output,
                               List<NativePhysicalFileAllocation> allocations,
                               DistributedWriteCommitContext context);
}
