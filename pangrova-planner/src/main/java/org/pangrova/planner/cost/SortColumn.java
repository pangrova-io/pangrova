package org.pangrova.planner.cost;

import org.pangrova.planner.enums.SortDirection;

import java.util.Objects;

/**
 * A column with sort direction, describing part of a data distribution's sort order.
 */
public record SortColumn(String column, SortDirection direction) {

    public SortColumn {
        Objects.requireNonNull(column, "column must not be null");
        Objects.requireNonNull(direction, "direction must not be null");
    }
}
