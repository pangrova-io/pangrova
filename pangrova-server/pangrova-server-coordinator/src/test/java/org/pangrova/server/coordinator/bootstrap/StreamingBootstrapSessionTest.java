package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.enums.StreamLifecycleState;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamingBootstrapSessionTest {
    @Test
    void usesThePersistedStreamCreatorAndTenantForConnectorWrites() {
        var session = StreamingBootstrap.sessionFor(stream("stream-owner"));

        assertThat(session.getUser()).isEqualTo("stream-owner");
        assertThat(session.getCatalog()).contains("iceberg");
        assertThat(session.getSchema()).contains("orders");
        assertThat(session.getProperties()).containsExactlyEntriesOf(Map.of("tenant_id", "tenant-a"));
        assertThat(session.getQueryAuthority()).isEmpty();
    }

    @Test
    void rejectsAStreamWithoutACreatorIdentity() {
        assertThatThrownBy(() -> StreamingBootstrap.sessionFor(stream(null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("creator identity");
    }

    private static StreamState stream(String creator) {
        return StreamState.builder().streamName("orders-stream").sourceProvider("kafka")
                .tenantId("tenant-a").targetTable("iceberg.orders.events").sourceFormat("debezium-json")
                .lifecycleState(StreamLifecycleState.RUNNING).sourceProperties(Map.of())
                .creatorIdentity(creator).createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build();
    }
}
