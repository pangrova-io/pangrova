package org.pangrova.governance.lineage.provider.openlineage;

import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageBackend;
import org.pangrova.governance.api.lineage.LineageEvent;

import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.client.transports.HttpConfig;

import java.util.List;
import java.util.UUID;

/** Real Marquez HTTP ingestion and its actual PostgreSQL event journal; no replacement transport. */
public final class MarquezTestBackend implements AutoCloseable {
    private final org.pangrova.testing.testkit.lineage.ReferenceLineageBackend reference =
            new org.pangrova.testing.testkit.lineage.ReferenceLineageBackend();
    private final String namespace = reference.namespace();

    public MarquezTestBackend() {
        try { verifyIngestionReadiness(); }
        catch (RuntimeException | Error failure) {
            try { close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void verifyIngestionReadiness() {
        String runId = UUID.randomUUID().toString();
        try (var transport = transport(20_000)) {
            var time = java.time.Instant.now();
            var input = new DatasetRef(namespace, "readiness.source", List.of());
            var source = new DatasetRef.SourceColumnRef(namespace, "readiness.source", "value");
            var columns = java.util.Arrays.stream(
                    org.pangrova.spi.observability.event.QueryLineageMetadata.DerivationKind.values())
                    .map(kind -> new DatasetRef.ColumnLineage(kind.name(), List.of(source), kind)).toList();
            var output = new DatasetRef(namespace, "readiness.target", columns);
            transport.send(new LineageEvent(LineageEvent.EventType.START, runId, "fixture-readiness",
                    "SELECT 1", List.of(input), List.of(output), java.util.Map.of(), time));
            transport.send(new LineageEvent(LineageEvent.EventType.COMPLETE, runId, "fixture-readiness",
                    null, List.of(input), List.of(output), java.util.Map.of(), time.plusMillis(1)));
            var accepted = events(runId);
            if (accepted.size() != 2 || !"START".equals(accepted.get(0).path("eventType").asText())
                    || !"COMPLETE".equals(accepted.get(1).path("eventType").asText())) {
                throw new IllegalStateException("Marquez startup did not establish actual durable ingestion");
            }
            var persistedColumns = accepted.get(1).at("/outputs/0/facets/columnLineage/fields");
            if (persistedColumns.size() != 3) {
                throw new IllegalStateException("Marquez column lineage ingestion is not ready");
            }
        } catch (Exception failure) {
            throw new IllegalStateException("Marquez actual HTTP ingestion is not ready", failure);
        }
    }

    public OpenLineageHttpTransport transport() {
        return transport(5000);
    }

    private OpenLineageHttpTransport transport(int timeoutMs) {
        var config = new HttpConfig();
        config.setUrl(reference.endpoint());
        config.setTimeoutInMillis(timeoutMs);
        return new OpenLineageHttpTransport(config, namespace);
    }

    /** Reads the reference backend's table, not a fake HTTP recorder or a delivery invocation count. */
    public List<JsonNode> events(String runId) throws Exception {
        return reference.events(OpenLineageHttpTransport.normalizeRunId(runId));
    }

    @Override
    public void close() { reference.close(); }
}
