package org.pangrova.connector.iceberg;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.connector.api.commit.DistributedWriteCommitContext;

import java.util.Arrays;

/** Requires the SDK commit handle to be the exact handle covered by the durable fingerprint. */
final class IcebergWriteBinding {
    private IcebergWriteBinding() {}

    static void requireMatches(IcebergOutputTableHandle handle, DistributedWriteCommitContext context) {
        try {
            var binding = WriteOutputBinding.parseFrom(context.writeOutputBinding());
            var serialized = binding.getConnector().getOutputHandle();
            var codec = new IcebergOutputHandleCodec();
            byte[] payload = codec.serialize(handle);
            if (!binding.hasConnector() || !serialized.getTypeUrl().equals(codec.typeUrl())
                    || serialized.getCodecVersion() != codec.codecVersion()
                    || !Arrays.equals(serialized.getPayload().toByteArray(), payload)
                    || !serialized.getPayloadChecksum().equals(ProtocolChecksums.sha256(payload))
                    || !serialized.getWriteColumnNamesList().equals(codec.writeColumnNames(handle))) {
                throw new IllegalArgumentException("Iceberg commit handle differs from durable write binding");
            }
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Malformed Iceberg durable write binding", failure);
        }
    }
}
