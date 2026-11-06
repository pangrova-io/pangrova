package org.pangrova.planner.core.runtimefilter;

/**
 * Represents a partition's (or row group's) value range for a column.
 *
 * @param partitionId unique identifier
 * @param min         minimum value in the range
 * @param max         maximum value in the range
 */
public record PartitionRange(String partitionId, Comparable<?> min, Comparable<?> max) {
}
