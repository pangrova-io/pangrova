package org.pangrova.batch.api.page;

import org.pangrova.common.api.annotation.Experimental;

import java.util.List;

import org.pangrova.types.value.TypedRow;

/**
 * Represents a batch of rows read from a connector page source.
 * Each row is a TypedRow providing type-safe column access.
 *
 * <p>This is the canonical data exchange unit between connectors and the engine
 * for Java SDK adapters and integration tests. It does not authorize
 * Coordinator-side relational row execution.</p>
 *
 * @param columns ordered column names for this page
 * @param rows    list of typed rows
 */
@Experimental
public record Page(List<String> columns, List<TypedRow> rows) {

    public Page {
        columns = List.copyOf(columns);
        rows = List.copyOf(rows);
    }

    /** Returns the number of rows in this page. */
    public int getPositionCount() {
        return rows.size();
    }

    /** Returns true if this page contains no rows. */
    public boolean isEmpty() {
        return rows.isEmpty();
    }
}
