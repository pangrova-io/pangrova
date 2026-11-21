package org.pangrova.query.api.session;

import java.util.Objects;

import org.pangrova.common.api.annotation.Experimental;

/** Session-scoped branch route for one table. A null branch removes the route. */
@Experimental
public record BranchSelection(String table, String branch) {
    public static final String PROPERTY_PREFIX = "branch.routing.";

    public BranchSelection {
        if (Objects.requireNonNull(table, "table").isBlank() || branch != null && branch.isBlank()) {
            throw new IllegalArgumentException("Invalid table or branch in session selection");
        }
    }

    public String propertyKey() {
        return PROPERTY_PREFIX + table;
    }
}
