package org.pangrova.testfixtures.tableformat;

import org.pangrova.connector.paimon.PaimonMetadata;

import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.coordination.WriteCommitFingerprint;

import java.util.List;
import java.util.UUID;

/** Builds the same immutable commit identity used by the Coordinator. */
public final class PaimonCommitTestSupport {

    private static final byte[] WRITE_BINDING = {42};

    private PaimonCommitTestSupport() {}

    public static void finish(PaimonMetadata metadata, ConnectorSession session,
                              ConnectorOutputTableHandle handle, List<byte[]> fragments) {
        metadata.finishDistributedWrite(session, handle, fragments,
                context(metadata, session, handle, fragments));
    }

    public static DistributedWriteCommitContext context(
            PaimonMetadata metadata, ConnectorSession session,
            ConnectorOutputTableHandle handle, List<byte[]> fragments) {
        String executionId = UUID.randomUUID().toString();
        String tenantId = session.getProperties().get("tenant_id");
        long baseSnapshotId = handle.writeBaseSnapshotId().orElse(-1L);
        String table = handle.targetTable().orElseThrow().toString();
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        executionId,
                        tenantId,
                        metadata.getCatalogName(),
                        table,
                        handle.writeMode(),
                        baseSnapshotId,
                        WRITE_BINDING,
                        session.getUser(),
                        PaimonWriteReceiptFixtures.receipt(), null),
                fragments);
        return new DistributedWriteCommitContext(
                executionId,
                tenantId,
                baseSnapshotId,
                WRITE_BINDING,
                fingerprint,
                session.getUser(),
                PaimonWriteReceiptFixtures.receipt(), null);
    }
}
