package org.pangrova.materialization.api.domain;

import org.pangrova.materialization.api.enums.PartitionAlignmentShape;

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
public record PartitionAlignment(PartitionAlignmentShape shape, List<Column> columns) {

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
            new PartitionAlignment(PartitionAlignmentShape.UNSUPPORTED, List.of());

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
