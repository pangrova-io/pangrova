package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.enums.StreamLifecycleState;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcWriteAuthorityTest {

    @Test
    void parsesTheExplicitCdcWriteActions() {
        assertThat(CdcWriteAuthority.privileges(state("insert, update")))
                .containsExactlyInAnyOrder(Privilege.INSERT, Privilege.UPDATE);
        assertThat(CdcWriteAuthority.privileges(state("DELETE")))
                .containsExactly(Privilege.DELETE);
    }

    @Test
    void missingOrNonWritePrivilegesFailClosed() {
        assertThatThrownBy(() -> CdcWriteAuthority.privileges(state(null)))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("cdc.write.privileges");
        assertThatThrownBy(() -> CdcWriteAuthority.privileges(state("SELECT")))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Invalid CDC write action");
        assertThatThrownBy(() -> CdcWriteAuthority.privileges(state(" , ")))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("at least one write action");
    }

    @Test
    void registeredPlanCarriesTheExactCdcSourceIdentity() {
        var tracker = new org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker();
        var source = new org.pangrova.persistence.query.api.commit.CdcSourceIdentity(
                "stream", "epoch", java.util.List.of(
                        new org.pangrova.persistence.query.api.commit.CdcSourceIdentity.OffsetRange(
                                "orders", 0, 10, 20)));
        var receipt = new org.pangrova.connector.api.commit.WriteAuthorizationReceipt(
                new byte[]{1},
                new org.pangrova.connector.api.commit.WriteDeclarationSnapshot(
                        new org.pangrova.connector.api.metadata.table.QualifiedTableName(
                                "catalog", "schema", "target"),
                        "0".repeat(64), "0".repeat(64), false));
        var plan = new CdcWriteAuthority.CdcWritePlan(new Handle(),
                org.pangrova.protocol.v2.WriteOutputBinding.newBuilder()
                        .setConnector(org.pangrova.protocol.v2.ConnectorWriteBinding.getDefaultInstance())
                        .build(), receipt,
                new org.pangrova.coordinator.query.task.write.WriteAllocationIntent(
                        "MERGE", java.util.Map.of()));
        plan.register(tracker, "q-cdc", "catalog", new Session(), source);

        assertThat(tracker.peekWriteState("q-cdc").cdcSource()).isEqualTo(source);
    }

    private static final class Handle
            implements org.pangrova.connector.api.metadata.ConnectorOutputTableHandle { }

    private static final class Session implements org.pangrova.connector.api.session.ConnectorSession {
        @Override public String getUser() { return "creator"; }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of("catalog"); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.of("schema"); }
        @Override public java.util.Map<String, String> getProperties() {
            return java.util.Map.of("tenant_id", "tenant-a");
        }
        @Override public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority>
                getQueryAuthority() {
            return java.util.Optional.empty();
        }
    }

    private static StreamState state(String privileges) {
        Map<String, String> properties = privileges == null
                ? Map.of()
                : Map.of(CdcWriteAuthority.PRIVILEGES_PROPERTY, privileges);
        return new StreamState("stream", "tenant-a", "checkpoint-1",
                StreamLifecycleState.RUNNING, "catalog.schema.target", "kafka",
                Map.of("topic", "orders"), "debezium-json", properties,
                "creator", "coordinator-a", 1L, 1, null,
                0L, 0L, 0L, null, Instant.EPOCH, Instant.EPOCH);
    }
}
