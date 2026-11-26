package org.pangrova.query.api.journal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.query.api.error.QueryFailure;

/** Recoverable terminal result kind and client-safe failure carried by the same terminal fact. */
@Experimental
public record QueryTerminalOutcome(ResultDisposition resultDisposition, QueryFailure error) {
    public QueryTerminalOutcome {
        if (resultDisposition != ResultDisposition.NONE && resultDisposition != ResultDisposition.UNAVAILABLE) {
            throw new IllegalArgumentException("Terminal recovery cannot retain an endpoint");
        }
        if (error != null && error.stackTrace() != null) {
            throw new IllegalArgumentException("Terminal recovery must not retain stack traces");
        }
    }

    boolean matchesState(String state) {
        return switch (state) {
            case "FINISHED" -> error == null;
            case "FAILED" -> resultDisposition == ResultDisposition.NONE;
            case "CANCELLED" -> resultDisposition == ResultDisposition.NONE && error == null;
            default -> false;
        };
    }
}
