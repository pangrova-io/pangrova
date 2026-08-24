package org.pangrova.spi.connector.metadata;

import java.util.List;

import org.pangrova.spi.annotation.Experimental;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;

/**
 * Describes an aggregation pushdown request: the aggregate functions to compute and the
 * grouping columns (empty for a global aggregate).
 *
 * <p>The engine offers this to a connector via
 * {@link ConnectorMetadata#applyAggregation} so that connectors capable of computing
 * aggregates close to storage (e.g. partition-level pre-aggregation, manifest/Puffin stats,
 * or an OLAP source) can absorb the GROUP BY and return only aggregated rows.</p>
 */
@Experimental
public record AggregationApplication(List<AggregateFunction> aggregates,
                                     List<ColumnHandle> groupingColumns) {

    public AggregationApplication {
        if (aggregates == null || aggregates.isEmpty()) {
            throw new IllegalArgumentException("aggregates must not be null or empty");
        }
        aggregates = List.copyOf(aggregates);
        groupingColumns = groupingColumns == null ? List.of() : List.copyOf(groupingColumns);
    }

    /** True for a global (no GROUP BY) aggregate. */
    public boolean isGlobal() {
        return groupingColumns.isEmpty();
    }

    /**
     * A single aggregate function invocation.
     *
     * @param functionName the canonical function name (e.g. {@code "count"}, {@code "sum"})
     * @param arguments    the column arguments (empty for {@code count(*)})
     * @param distinct     whether the aggregate is a DISTINCT aggregate
     */
    public record AggregateFunction(String functionName,
                                    List<ColumnHandle> arguments,
                                    boolean distinct) {
        public AggregateFunction {
            if (functionName == null || functionName.isBlank()) {
                throw new IllegalArgumentException("functionName must not be null or blank");
            }
            arguments = arguments == null ? List.of() : List.copyOf(arguments);
        }
    }
}
