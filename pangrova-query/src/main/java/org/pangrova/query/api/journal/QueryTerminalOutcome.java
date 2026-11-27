package org.pangrova.query.api.journal;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.session.BranchSelection;

/** Recoverable terminal result kind and client-safe failure carried by the same terminal fact. */
@Experimental
public record QueryTerminalOutcome(ResultDisposition resultDisposition, QueryFailure error,
        BranchSelection branchSelection) {
    public QueryTerminalOutcome {
        if (resultDisposition != ResultDisposition.NONE && resultDisposition != ResultDisposition.UNAVAILABLE) {
            throw new IllegalArgumentException("Terminal recovery cannot retain an endpoint");
        }
        if (error != null && error.stackTrace() != null) {
            throw new IllegalArgumentException("Terminal recovery must not retain stack traces");
        }
        if (branchSelection != null && (resultDisposition != ResultDisposition.NONE || error != null)) {
            throw new IllegalArgumentException("Branch selection requires successful resultless completion");
        }
    }

    boolean matchesState(String state) {
        if (branchSelection != null && !"FINISHED".equals(state)) return false;
        return switch (state) {
            case "FINISHED" -> error == null;
            case "FAILED" -> resultDisposition == ResultDisposition.NONE;
            case "CANCELLED" -> resultDisposition == ResultDisposition.NONE && error == null;
            default -> false;
        };
    }
}
