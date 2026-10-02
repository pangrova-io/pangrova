package org.pangrova.connector.iceberg;

import com.google.protobuf.ByteString;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;

/** Exact connector handle bytes used by local durable-commit boundary tests. */
final class IcebergWriteBindingFixtures {
    private IcebergWriteBindingFixtures() {}

    static byte[] binding(IcebergOutputTableHandle handle) {
        var codec = new IcebergOutputHandleCodec();
        byte[] payload = codec.serialize(handle);
        return WriteOutputBinding.newBuilder().setConnector(ConnectorWriteBinding.newBuilder()
                .setOutputHandle(SerializedConnectorOutputHandle.newBuilder()
                        .setTypeUrl(codec.typeUrl()).setCodecVersion(codec.codecVersion())
                        .setPayload(ByteString.copyFrom(payload))
                        .setPayloadChecksum(CanonicalTaskRequests.sha256(payload))
                        .addAllWriteColumnNames(codec.writeColumnNames(handle)))).build().toByteArray();
    }
}
