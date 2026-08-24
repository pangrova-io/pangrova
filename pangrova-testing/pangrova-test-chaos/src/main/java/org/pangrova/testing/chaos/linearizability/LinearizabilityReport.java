package org.pangrova.testing.chaos.linearizability;

import java.util.List;
import java.util.Objects;

/**
 * Result of a linearizability check on a concurrent operation history.
 *
 * @param linearizable true if a valid linearization was found
 * @param history      the operations that were checked
 * @param witness      if linearizable, one valid sequential ordering; otherwise empty
 * @param violation    if not linearizable, a description of the violation; otherwise null
 */
public record LinearizabilityReport(
        boolean linearizable,
        List<Operation> history,
        List<Operation> witness,
        String violation
) {
    public LinearizabilityReport {
        Objects.requireNonNull(history, "history must not be null");
        Objects.requireNonNull(witness, "witness must not be null");
        history = List.copyOf(history);
        witness = List.copyOf(witness);
    }

    /**
     * Creates a passing report with the given witness ordering.
     */
    public static LinearizabilityReport pass(List<Operation> history, List<Operation> witness) {
        return new LinearizabilityReport(true, history, witness, null);
    }

    /**
     * Creates a failing report with the given violation description.
     */
    public static LinearizabilityReport fail(List<Operation> history, String violation) {
        return new LinearizabilityReport(false, history, List.of(), violation);
    }
}
