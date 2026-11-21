package org.pangrova.common.client.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Session-scoped server acknowledgement. A null branch removes the table's route. */
public record BranchSelection(@JsonProperty("table") String table, @JsonProperty("branch") String branch) {
    public static final String PROPERTY_PREFIX = "branch.routing.";

    public BranchSelection {
        if (Objects.requireNonNull(table, "table").isBlank() || branch != null && branch.isBlank()) {
            throw new IllegalArgumentException("Invalid table or branch in session selection");
        }
    }

    public String propertyKey() { return PROPERTY_PREFIX + table; }
}
