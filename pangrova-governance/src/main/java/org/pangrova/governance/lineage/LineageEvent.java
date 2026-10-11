package org.pangrova.governance.lineage;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Represents an OpenLineage-compatible run event.
 */
public record LineageEvent(
    EventType eventType,
    String runId,
    String jobName,
    String sql,
    List<DatasetRef> inputDatasets,
    List<DatasetRef> outputDatasets,
    Map<String, String> facets,
    Instant timestamp
) {
    public LineageEvent {
        inputDatasets = inputDatasets == null ? List.of() : List.copyOf(inputDatasets);
        outputDatasets = outputDatasets == null ? List.of() : List.copyOf(outputDatasets);
        facets = facets == null ? Map.of() : Map.copyOf(facets);
    }

    public enum EventType { START, COMPLETE, FAIL, ABORT }
}
