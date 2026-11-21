package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.persistence.query.api.commit.CdcSourceIdentity;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.enums.StreamLifecycleState;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class CdcWriteIntentFactoryTest {

    @Test
    void bindsFragmentsAndSourceIntoTheDurableIntent() {
        var source = new CdcSourceIdentity("stream", "epoch-2", List.of(
                new CdcSourceIdentity.OffsetRange("orders", 0, 10, 20)));
        var intent = CdcWriteIntentFactory.create(
                "q-cdc", stream(), new Session(), plan(), source,
                List.of(new byte[]{3}, new byte[]{4}), 42L, 1_000L,
                "coordinator", "process", 7L);

        assertThat(intent.catalogName()).isEqualTo("catalog");
        assertThat(intent.tableName()).isEqualTo("schema.target");
        assertThat(intent.writeMode()).isEqualTo("MERGE");
        assertThat(intent.fragments()).containsExactly(new byte[]{3}, new byte[]{4});
        assertThat(intent.cdcSource()).isEqualTo(source);
        assertThat(intent.commitFingerprint()).hasSize(64);
    }

    private static StreamState stream() {
        return new StreamState("stream", "tenant-a", "checkpoint-1",
                StreamLifecycleState.RUNNING, "catalog.schema.target", "kafka",
                Map.of("topic", "orders"), "debezium-json",
                Map.of(CdcWriteAuthority.PRIVILEGES_PROPERTY, "INSERT,UPDATE,DELETE"),
                "creator", "coordinator-a", 2L, 1, null,
                0L, 0L, 0L, null, Instant.EPOCH, Instant.EPOCH);
    }

    private static CdcWriteAuthority.CdcWritePlan plan() {
        var receipt = new WriteAuthorizationReceipt(new byte[]{1},
                new WriteDeclarationSnapshot(new QualifiedTableName("catalog", "schema", "target"),
                        "0".repeat(64), "0".repeat(64), false));
        return new CdcWriteAuthority.CdcWritePlan(new Handle(),
                WriteOutputBinding.newBuilder()
                        .setConnector(ConnectorWriteBinding.newBuilder()
                                .setOutputHandle(org.pangrova.protocol.v2.SerializedConnectorOutputHandle
                                        .newBuilder().setTypeUrl("fixture").setCodecVersion(1)
                                        .setPayload(com.google.protobuf.ByteString.copyFrom(new byte[]{2}))
                                        .setPayloadChecksum(org.pangrova.protocol.checksum.ProtocolChecksums
                                                .sha256(new byte[]{2}))
                                        .addWriteColumnNames("id")))
                        .build(),
                receipt, new org.pangrova.coordinator.query.task.write.WriteAllocationIntent(
                        "MERGE", Map.of()));
    }

    private static final class Handle implements ConnectorOutputTableHandle { }

    private static final class Session implements ConnectorSession {
        @Override public String getUser() { return "creator"; }
        @Override public Optional<String> getCatalog() { return Optional.of("catalog"); }
        @Override public Optional<String> getSchema() { return Optional.of("schema"); }
        @Override public Map<String, String> getProperties() {
            return Map.of("tenant_id", "tenant-a");
        }
        @Override public Optional<ConnectorQueryAuthority> getQueryAuthority() {
            return Optional.empty();
        }
    }
}
