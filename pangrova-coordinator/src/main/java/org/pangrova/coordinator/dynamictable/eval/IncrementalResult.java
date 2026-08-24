package org.pangrova.coordinator.dynamictable.eval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable result of an incremental evaluation pass.
 *
 * <p>Contains the output rows produced by applying the defining query's
 * filter/projection/aggregation logic to a batch of incremental changes.</p>
 *
 * @param records           output rows (each row is a column-name → value map), immutable
 * @param processedRows     total number of input rows processed
 * @param aggregationMerges number of aggregation merge/retract operations performed
 */
public record IncrementalResult(
        List<Map<String, Object>> records,
        long processedRows,
        int aggregationMerges
) {

    public IncrementalResult {
        Objects.requireNonNull(records, "records must not be null");
        records = List.copyOf(records);
        if (processedRows < 0) {
            throw new IllegalArgumentException("processedRows must be non-negative");
        }
        if (aggregationMerges < 0) {
            throw new IllegalArgumentException("aggregationMerges must be non-negative");
        }
    }

    /** Convenience factory for an empty result (no output, no processing). */
    public static IncrementalResult empty() {
        return new IncrementalResult(List.of(), 0, 0);
    }
}
