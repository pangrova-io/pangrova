package org.pangrova.spi.resource.guard;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;
import java.util.Objects;

/**
 * A guard's verdict on a single query: the recommended action plus the
 * dimensions that were breached.
 *
 * @param queryId the offending query (never null or blank)
 * @param action  the recommended action (never null)
 * @param reasons human-readable breach reasons, one per exceeded dimension (never empty)
 */
@Experimental(reason = "G-RES-06: runaway query guard SPI")
public record GuardVerdict(String queryId, GuardAction action, List<String> reasons) {

    public GuardVerdict {
        Objects.requireNonNull(queryId, "queryId must not be null");
        if (queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(reasons, "reasons must not be null");
        if (reasons.isEmpty()) {
            throw new IllegalArgumentException("reasons must not be empty");
        }
        reasons = List.copyOf(reasons);
    }

    /** Creates a KILL verdict. */
    public static GuardVerdict kill(String queryId, List<String> reasons) {
        return new GuardVerdict(queryId, GuardAction.KILL, reasons);
    }

    /** Creates a WARN verdict. */
    public static GuardVerdict warn(String queryId, List<String> reasons) {
        return new GuardVerdict(queryId, GuardAction.WARN, reasons);
    }
}
