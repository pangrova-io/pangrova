package org.pangrova.governance.lineage;

import org.pangrova.persistence.query.api.lineage.QueryLineageOutbox;
import org.pangrova.governance.core.lineage.LineageEventEmitter;
import org.pangrova.governance.lineage.log.LogLineageBackend;
import org.pangrova.governance.lineage.openlineage.OpenLineageHttpTransport;

/** Explicit deployment selection; HTTP requires durable facts and never changes to log delivery. */
public final class LineageBackendConfiguration {
    private LineageBackendConfiguration() { }

    /** Creates the emitter selected by explicit deployment configuration. */
    public static LineageEventEmitter createEmitter() { return createEmitter(null); }

    /** HTTP delivery borrows the sole migrated metadata outbox; LOG remains explicit transient telemetry. */
    public static LineageEventEmitter createEmitter(QueryLineageOutbox outbox) {
        String kind = System.getProperty("pangrova.lineage.backend", "LOG");
        return switch (kind) {
            case "LOG" -> new LineageEventEmitter(new LogLineageBackend());
            case "HTTP" -> {
                if (outbox == null) throw new IllegalStateException("HTTP lineage requires the metadata outbox");
                String namespace = System.getProperty("pangrova.lineage.namespace", "pangrova");
                yield new LineageEventEmitter(new OpenLineageHttpTransport(namespace), outbox, namespace);
            }
            default -> throw new IllegalArgumentException("Unknown lineage backend: " + kind);
        };
    }
}
