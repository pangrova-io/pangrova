package org.pangrova.spi.connector.data.page;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

import org.pangrova.spi.connector.data.TypedRow;

/**
 * Represents a batch of rows read from a connector page source.
 * Each row is a TypedRow providing type-safe column access.
 *
 * <p>This is the canonical data exchange unit between connectors and the engine
 * for Java-side data reading (e.g., integration tests, DML evaluation).</p>
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
