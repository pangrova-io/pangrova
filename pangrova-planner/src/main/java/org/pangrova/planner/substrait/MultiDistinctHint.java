package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.relation.Extension;

import java.util.List;

/**
 * Optimization hint for multi-DISTINCT aggregation patterns.
 *
 * <p>Attached to the AggregateRel's advanced_extension when the planner detects
 * multiple DISTINCT aggregate measures on different columns. The native engine
 * uses this hint to insert MarkDistinctNode operators for each distinct column,
 * enabling single-pass aggregation with boolean marker masking.</p>
 *
 * <p>When only a single DISTINCT aggregate is present, this hint is NOT emitted
 * because the native engine's AggregationNode handles single-DISTINCT natively.</p>
 *
 * @param distinctColumns the list of distinct column descriptors
 */
public record MultiDistinctHint(
        List<DistinctColumn> distinctColumns) implements Extension.Optimization {

    /** Marker key for identifying multi-distinct hints in the plan. */
    static final String MARKER = "pangrova:multi_distinct_hint";

    /**
     * Describes a single distinct column within a multi-DISTINCT aggregation.
     *
     * @param measureIndex the index of the measure in the AggregateRel's measures list
     * @param columnIndex the input column index that the DISTINCT operates on
     * @param markerName the suggested boolean marker column name for MarkDistinctNode
     */
    public record DistinctColumn(int measureIndex, int columnIndex, String markerName) {}

    @Override
    public Any toProto() {
        var json = new StringBuilder("{");
        json.append("\"op\":\"multi_distinct_hint\"");
        json.append(",\"distinct_columns\":[");
        for (int i = 0; i < distinctColumns.size(); i++) {
            if (i > 0) json.append(",");
            DistinctColumn col = distinctColumns.get(i);
            json.append("{\"measure_index\":").append(col.measureIndex());
            json.append(",\"column_index\":").append(col.columnIndex());
            json.append(",\"marker_name\":\"").append(col.markerName()).append("\"}");
        }
        json.append("]");
        json.append("}");
        return Any.pack(StringValue.of(json.toString()));
    }
}
