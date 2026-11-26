package org.pangrova.query.api.journal;

import java.util.Objects;
import org.pangrova.common.api.annotation.Experimental;

/** Durable acceptance result; a replay never transfers execution responsibility. */
@Experimental
public record QueryAcceptanceAdmission(boolean newExecution, QueryAcceptance acceptance) {
    public QueryAcceptanceAdmission {
        Objects.requireNonNull(acceptance, "acceptance must not be null");
    }
}
