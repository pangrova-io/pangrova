package org.pangrova.materialization.domain;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Maps source partition columns onto the definition columns that expose them.
 *
 * <p>Partition replacement rewrites the definition only for the partitions the
 * connector reported. The rewrite filters the definition result by output
 * column, so every source partition column must have exactly one directly
 * derived output column. For an aggregate definition the partition column must
 * also be a grouping key; otherwise a single changed partition could alter
 * groups owned by unaffected partitions.
 *
 * @param shape definition shape a partition rewrite can evaluate
 * @param columns source-to-output mappings for directly derived columns
 */
public record PartitionAlignment(Shape shape, List<Column> columns) {

    /** Definition shape that can be recomputed one partition at a time. */
    public enum Shape {
        /** Single-table projection/filter scan. */
        SCAN,
        /** Single-table aggregate whose grouping keys are the partition columns. */
        AGGREGATE,
        /** Shape that cannot be rewritten per partition. */
        UNSUPPORTED
    }

    /**
     * One directly derived output column.
     *
     * @param sourceColumn source column the connector partitions by
     * @param outputColumn output column that exposes the source value
     * @param groupingKey whether the output column is a grouping key
     */
    public record Column(String sourceColumn, String outputColumn, boolean groupingKey) {

        public Column {
            if (sourceColumn == null || sourceColumn.isBlank()) {
                throw new IllegalArgumentException("sourceColumn must not be blank");
            }
            if (outputColumn == null || outputColumn.isBlank()) {
                throw new IllegalArgumentException("outputColumn must not be blank");
            }
        }
    }

    /** Alignment for a definition that cannot be rewritten per partition. */
    public static final PartitionAlignment NONE =
            new PartitionAlignment(Shape.UNSUPPORTED, List.of());

    public PartitionAlignment {
        if (shape == null) {
            throw new IllegalArgumentException("shape must not be null");
        }
        columns = columns == null ? List.of() : List.copyOf(columns);
        Set<String> seen = new HashSet<>();
        for (Column column : columns) {
            if (!seen.add(column.sourceColumn().toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "source column " + column.sourceColumn()
                                + " is exposed by more than one output column");
            }
        }
    }

    /**
     * Returns the mapping for a source partition column.
     *
     * @param sourceColumn source column name
     * @return mapping when the column is directly exposed by the definition
     */
    public Optional<Column> columnFor(String sourceColumn) {
        if (sourceColumn == null) {
            return Optional.empty();
        }
        String normalized = sourceColumn.toLowerCase(Locale.ROOT);
        return columns.stream()
                .filter(column -> column.sourceColumn().toLowerCase(Locale.ROOT)
                        .equals(normalized))
                .findFirst();
    }
}
