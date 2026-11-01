package org.pangrova.governance.core.lineage;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.pangrova.spi.observability.event.QueryCompletedEvent;
import org.pangrova.spi.observability.event.QueryCreatedEvent;
import org.pangrova.spi.observability.event.QueryEventListener;
import org.pangrova.spi.observability.event.QueryLineageMetadata;
import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageEvent;

/** Publishes logical query admission and terminal events through the OpenLineage emitter. */
public final class LineageQueryEventListener implements QueryEventListener {
    private final LineageEventEmitter emitter;

    public LineageQueryEventListener(LineageEventEmitter emitter) {
        this.emitter = Objects.requireNonNull(emitter, "emitter");
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        emitter.emitStartEvent(event.queryId(), event.sql(), List.of(), List.of());
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        var type = switch (event.finalState()) {
            case FINISHED -> LineageEvent.EventType.COMPLETE;
            case FAILED -> LineageEvent.EventType.FAIL;
            case CANCELLED -> LineageEvent.EventType.ABORT;
            default -> throw new IllegalArgumentException("Query lineage requires a terminal state");
        };
        emitter.emitTerminalEvent(type, event.queryId(), event.sql(),
                datasets(event.lineage().inputs()), datasets(event.lineage().outputs()),
                event.failureMessage().map(message -> Map.of("error", message)).orElseGet(Map::of));
    }

    private static List<DatasetRef> datasets(List<QueryLineageMetadata.Dataset> datasets) {
        return datasets.stream().map(dataset -> new DatasetRef(dataset.namespace(), dataset.name(),
                dataset.columns().stream().map(column -> new DatasetRef.ColumnLineage(column.name(),
                        column.sources().stream().map(source -> new DatasetRef.SourceColumnRef(
                                source.namespace(), source.dataset(), source.column())).toList(),
                        column.derivation())).toList())).toList();
    }
}
