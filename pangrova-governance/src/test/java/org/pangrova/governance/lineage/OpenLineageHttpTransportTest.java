package org.pangrova.governance.lineage;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.Transport;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Validates runId normalization (#182): non-UUID query/stage ids must not throw or poison
 * the head-of-line retry queue, and must map deterministically to a valid OpenLineage UUID.
 */
class OpenLineageHttpTransportTest {

    /** Capturing transport — records the last RunEvent instead of hitting the network. */
    private static final class CapturingTransport extends Transport {
        final AtomicReference<OpenLineage.RunEvent> last = new AtomicReference<>();

        @Override
        public void emit(OpenLineage.RunEvent runEvent) {
            last.set(runEvent);
        }
    }

    private static LineageEvent event(String runId) {
        return new LineageEvent(LineageEvent.EventType.START, runId, "pangrova-query", "SELECT 1",
                List.of(new DatasetRef("pangrova", "db.t_in", List.of())),
                List.of(new DatasetRef("pangrova", "db.t_out", List.of())),
                Map.of(), Instant.now());
    }

    @Test
    void normalizeRunIdPassesValidUuidThrough() {
        UUID uuid = UUID.randomUUID();
        assertThat(OpenLineageHttpTransport.normalizeRunId(uuid.toString())).isEqualTo(uuid);
    }

    @Test
    void normalizeRunIdIsDeterministicForNonUuid() {
        UUID a = OpenLineageHttpTransport.normalizeRunId("q-1");
        UUID b = OpenLineageHttpTransport.normalizeRunId("q-1");
        UUID c = OpenLineageHttpTransport.normalizeRunId("q-2");
        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isEqualTo(UUID.nameUUIDFromBytes("q-1".getBytes(UTF_8)));
    }

    @Test
    void normalizeRunIdHandlesNullAndBlank() {
        assertThat(OpenLineageHttpTransport.normalizeRunId(null)).isNotNull();
        assertThat(OpenLineageHttpTransport.normalizeRunId("  ")).isNotNull();
    }

    @Test
    void sendWithNonUuidRunIdDoesNotThrowAndEmitsValidUuid() {
        CapturingTransport transport = new CapturingTransport();
        OpenLineageHttpTransport sut =
                new OpenLineageHttpTransport(new OpenLineageClient(transport), "pangrova");

        assertThatNoException().isThrownBy(() -> sut.send(event("q-not-a-uuid")));

        OpenLineage.RunEvent captured = transport.last.get();
        assertThat(captured).isNotNull();
        assertThat(captured.getRun().getRunId())
                .isEqualTo(UUID.nameUUIDFromBytes("q-not-a-uuid".getBytes(UTF_8)));
    }
}
