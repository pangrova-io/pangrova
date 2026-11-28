package org.pangrova.testfixtures.tableformat;

import com.google.protobuf.ByteString;
import org.pangrova.connector.paimon.PaimonMetadata;
import org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;
import org.pangrova.persistence.query.api.commit.WriteCommitFingerprint;

import java.util.List;
import java.util.UUID;

/** Builds the same immutable commit identity used by the Coordinator. */
public final class PaimonCommitTestSupport {

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
        byte[] writeBinding = writeBinding(handle);
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.persistence.query.api.commit.WriteCommitFingerprint.Intent(
                        executionId,
                        tenantId,
                        metadata.getCatalogName(),
                        table,
                        handle.writeMode(),
                        baseSnapshotId,
                        writeBinding,
                        session.getUser(),
                        PaimonWriteReceiptFixtures.receipt(), null),
                fragments);
        var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
        return new DistributedWriteCommitContext(
                executionId,
                tenantId,
                baseSnapshotId,
                writeBinding,
                fingerprint,
                session.getUser(),
                PaimonWriteReceiptFixtures.receipt(), null, null,
                () -> allowed.compareAndSet(true, false));
    }

    /** Mirrors the coordinator's durable connector output binding for a Paimon handle. */
    private static byte[] writeBinding(ConnectorOutputTableHandle handle) {
        var codec = new PaimonOutputHandleCodec();
        byte[] payload = codec.serialize(handle);
        var serialized = SerializedConnectorOutputHandle.newBuilder()
                .setTypeUrl(codec.typeUrl())
                .setCodecVersion(codec.codecVersion())
                .setPayload(ByteString.copyFrom(payload))
                .addAllWriteColumnNames(codec.writeColumnNames(handle))
                .build();
        return WriteOutputBinding.newBuilder()
                .setConnector(ConnectorWriteBinding.newBuilder().setOutputHandle(serialized))
                .build().toByteArray();
    }
}
