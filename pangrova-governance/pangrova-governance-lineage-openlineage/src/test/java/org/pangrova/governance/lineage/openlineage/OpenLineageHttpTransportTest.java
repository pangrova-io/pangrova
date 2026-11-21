package org.pangrova.governance.lineage.openlineage;

import org.pangrova.governance.api.enums.LineageEventType;
import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Validates runId normalization (#182): non-UUID query/stage ids must not throw or poison
 * the head-of-line retry queue, and must map deterministically to a valid OpenLineage UUID.
 */
@Tag("integration")
class OpenLineageHttpTransportTest {

    private static MarquezTestBackend backend;

    @BeforeAll
    static void startReferenceBackend() { backend = new MarquezTestBackend(); }

    @AfterAll
    static void closeReferenceBackend() { if (backend != null) backend.close(); }

    private static LineageEvent event(String runId) {
        return new LineageEvent(LineageEventType.START, runId, "pangrova-query", "SELECT 1",
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
    void sendWithNonUuidRunIdDoesNotThrowAndEmitsValidUuid() throws Exception {
        try (OpenLineageHttpTransport sut = backend.transport()) {
            assertThatNoException().isThrownBy(() -> sut.send(event("q-not-a-uuid")));
        }
        var events = backend.events("q-not-a-uuid");
        assertThat(events).hasSize(1);
        var captured = events.get(0);
        assertThat(captured).isNotNull();
        assertThat(captured.at("/run/runId").asText())
                .isEqualTo(UUID.nameUUIDFromBytes("q-not-a-uuid".getBytes(UTF_8)).toString());
        assertThat(captured.at("/inputs/0/name").asText()).isEqualTo("db.t_in");
        assertThat(captured.at("/outputs/0/name").asText()).isEqualTo("db.t_out");

    }
}
