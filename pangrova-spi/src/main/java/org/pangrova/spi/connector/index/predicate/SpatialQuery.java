package org.pangrova.spi.connector.index.predicate;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Predicate for spatial queries evaluated against an R-Tree index.
 *
 * @param column target column name (must not be blank)
 * @param minX   minimum X coordinate of the query bounding box
 * @param minY   minimum Y coordinate of the query bounding box
 * @param maxX   maximum X coordinate of the query bounding box
 * @param maxY   maximum Y coordinate of the query bounding box
 * @param op     spatial operation to apply
 */
@Experimental
public record SpatialQuery(
        String column,
        double minX,
        double minY,
        double maxX,
        double maxY,
        SpatialOp op
) implements IndexPredicate {

    public SpatialQuery {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("column must not be blank");
        }
        Objects.requireNonNull(op, "op must not be null");
        if (minX > maxX) {
            throw new IllegalArgumentException("minX must not exceed maxX");
        }
        if (minY > maxY) {
            throw new IllegalArgumentException("minY must not exceed maxY");
        }
    }
}
