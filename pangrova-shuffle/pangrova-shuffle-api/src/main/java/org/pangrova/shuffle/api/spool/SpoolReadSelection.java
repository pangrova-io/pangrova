package org.pangrova.shuffle.api.spool;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Exact segments selected by the exchange owner, in producer order. No highest-attempt lookup.
 * This storage selection is not a publication receipt, authorization or generation fence.
 */
@Experimental(reason = "Local spool primitive; generic session and publication binding remain pending")
public record SpoolReadSelection(String queryId, int stageId, int partitionId,
                                 Map<Integer, Long> producerAttempts) {
    public SpoolReadSelection {
        Objects.requireNonNull(queryId, "queryId");
        Objects.requireNonNull(producerAttempts, "producerAttempts");
        if (queryId.isBlank() || stageId < 0 || partitionId < 0) {
            throw new IllegalArgumentException("Invalid spool partition identity");
        }
        var selected = new TreeMap<Integer, Long>();
        producerAttempts.forEach((producer, attempt) -> {
            if (producer == null || attempt == null || producer < 0 || attempt < 0) {
                throw new IllegalArgumentException("Producer and attempt must be non-negative");
            }
            selected.put(producer, attempt);
        });
        producerAttempts = Collections.unmodifiableMap(selected);
    }
}
