package org.pangrova.governance.core.lineage;

import org.pangrova.spi.query.lineage.QueryLineageFact;
import org.pangrova.spi.observability.event.QueryLineageMetadata;
import org.pangrova.governance.api.lineage.DatasetRef;
import org.pangrova.governance.api.lineage.LineageEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** One conversion between the frozen SPI fact and the existing OpenLineage adapter. */
final class LineageFacts {
    private LineageFacts() { }

    static LineageEvent event(QueryLineageFact fact) {
        LineageEvent.EventType type = switch (fact.phase()) {
            case START -> LineageEvent.EventType.START;
            case COMPLETE -> LineageEvent.EventType.COMPLETE;
            case FAIL -> LineageEvent.EventType.FAIL;
            case ABORT -> LineageEvent.EventType.ABORT;
        };
        return new LineageEvent(type, fact.queryId(), "pangrova-query", fact.sql(),
                datasets(fact.lineage().inputs()),
                datasets(fact.lineage().outputs()), fact.failureMessage() == null ? Map.of()
                        : Map.of("error", fact.failureMessage()), Instant.ofEpochMilli(fact.timestampMs()));
    }

    static QueryLineageFact fact(String namespace, LineageEvent event) {
        return new QueryLineageFact(namespace, event.runId(), switch (event.eventType()) {
                case START -> QueryLineageFact.Phase.START;
                case COMPLETE -> QueryLineageFact.Phase.COMPLETE;
                case FAIL -> QueryLineageFact.Phase.FAIL;
                case ABORT -> QueryLineageFact.Phase.ABORT;
            }, event.timestamp().toEpochMilli(), event.sql(), event.facets().get("error"),
                new QueryLineageMetadata(metadata(namespace, event.inputDatasets()),
                        metadata(namespace, event.outputDatasets())));
    }

    private static List<DatasetRef> datasets(List<QueryLineageMetadata.Dataset> datasets) {
        return datasets.stream().map(dataset -> new DatasetRef(dataset.namespace(), dataset.name(),
                dataset.columns().stream().map(column -> new DatasetRef.ColumnLineage(column.name(),
                        column.sources().stream().map(source -> new DatasetRef.SourceColumnRef(
                                source.namespace(), source.dataset(), source.column())).toList(),
                        column.derivation())).toList())).toList();
    }

    private static List<QueryLineageMetadata.Dataset> metadata(String namespace, List<DatasetRef> datasets) {
        return datasets.stream().map(dataset -> new QueryLineageMetadata.Dataset(
                dataset.namespace() == null ? namespace : dataset.namespace(), dataset.name(),
                dataset.columnLineage().stream().map(column -> new QueryLineageMetadata.Column(column.outputColumn(),
                        column.sourceColumns().stream().map(source -> new QueryLineageMetadata.SourceColumn(
                                source.namespace() == null ? namespace : source.namespace(),
                                source.dataset(), source.column())).toList(),
                        column.derivation())).toList())).toList();
    }
}
