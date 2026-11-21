package org.pangrova.protocol.rest.task;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Per-operator execution statistics mapped from the canonical Protobuf contract.
 *
 * <p>Tracks input/output rows and bytes, CPU and wall time, memory usage,
 * and spill at the individual operator level (TableScan, Project, Filter, etc.).</p>
 *
 * <p>Fields plan_node_id, memory_bytes, peak_memory_bytes, spill_bytes were added
 * per design Req 21 to correlate stats to plan nodes and track per-operator
 * memory and spill metrics.</p>
 */
public record OperatorStats(
    @JsonProperty("operator_type") String operatorType,
    @JsonProperty("input_rows") long inputRows,
    @JsonProperty("output_rows") long outputRows,
    @JsonProperty("input_bytes") long inputBytes,
    @JsonProperty("output_bytes") long outputBytes,
    @JsonProperty("cpu_nanos") long cpuNanos,
    @JsonProperty("wall_nanos") long wallNanos,
    @JsonProperty("plan_node_id") String planNodeId,
    @JsonProperty("memory_bytes") long memoryBytes,
    @JsonProperty("peak_memory_bytes") long peakMemoryBytes,
    @JsonProperty("spill_bytes") long spillBytes
) {
}
