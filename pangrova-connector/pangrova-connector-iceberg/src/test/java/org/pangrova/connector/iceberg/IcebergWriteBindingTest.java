package org.pangrova.connector.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.coordination.WriteCommitFingerprint;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Durable binding identity, independent of live authorization and SDK publication. */
class IcebergWriteBindingTest {
    @Test
    void exactBoundHandleIsRequiredIncludingGeneratedAndVectorDeclarations() {
        var allocated = handle(3);
        var context = context(allocated, IcebergWriteBindingFixtures.binding(allocated));
        IcebergWriteBinding.requireMatches(allocated, context);
        assertThatThrownBy(() -> IcebergWriteBinding.requireMatches(handle(4), context))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("differs from durable write binding");
    }

    @Test
    void checksumColumnsAndSoleCodecVersionCannotBeAltered() throws Exception {
        var allocated = handle(3);
        var binding = WriteOutputBinding.parseFrom(IcebergWriteBindingFixtures.binding(allocated));
        var original = binding.getConnector().getOutputHandle();
        var invalidHandles = List.of(original.toBuilder().clearPayloadChecksum().build(),
                original.toBuilder().setCodecVersion(3).build(),
                original.toBuilder().clearWriteColumnNames().build(),
                original.toBuilder().setTypeUrl("another-connector").build());
        for (var invalid : invalidHandles) {
            byte[] bytes = binding.toBuilder().setConnector(binding.getConnector().toBuilder()
                    .setOutputHandle(invalid)).build().toByteArray();
            assertThatThrownBy(() -> IcebergWriteBinding.requireMatches(allocated, context(allocated, bytes)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void malformedBindingIsNotTreatedAsAnUnjournaledWrite() {
        var allocated = handle(3);
        assertThatThrownBy(() -> IcebergWriteBinding.requireMatches(allocated, context(allocated, new byte[]{42})))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Malformed Iceberg durable");
    }

    private IcebergOutputTableHandle handle(int dimension) {
        var schema = new Schema(Types.NestedField.optional(1, "embedding",
                Types.ListType.ofRequired(2, Types.FloatType.get())));
        return new IcebergOutputTableHandle("db", "events", "00000000-0000-0000-0000-000000000001",
                "file:///warehouse/events", SchemaParser.toJson(schema),
                PartitionSpecParser.toJson(PartitionSpec.unpartitioned()), 2, -1, "INSERT", Map.of(),
                IcebergDeclarationProperties.capture(Map.of("pangrova.vector.dimension.embedding", "" + dimension)));
    }

    private DistributedWriteCommitContext context(IcebergOutputTableHandle handle, byte[] bytes) {
        var receipt = IcebergWriteReceiptFixtures.receipt();
        String fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(
                "write", "tenant", "catalog", "db.events", "INSERT", -1, bytes, "writer", receipt), List.of());
        return new DistributedWriteCommitContext("write", "tenant", handle.baseSnapshotId(),
                bytes, fingerprint, "writer", receipt);
    }
}
