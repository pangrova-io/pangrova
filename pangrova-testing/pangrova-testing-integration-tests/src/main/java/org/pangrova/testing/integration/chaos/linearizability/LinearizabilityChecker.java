package org.pangrova.testing.integration.chaos.linearizability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Checks whether a concurrent operation history is linearizable with respect to a sequential specification.
 * Uses a backtracking search (Wing & Gong algorithm) to find a valid linearization.
 *
 * <p>The specification is provided as a predicate that determines whether applying an operation
 * to a given state produces the observed result. The state is modeled as a set of existing objects
 * (e.g., table names for metadata operations).
 */
public final class LinearizabilityChecker {

    private static final Logger LOG = LoggerFactory.getLogger(LinearizabilityChecker.class);

    private final BiPredicate<Set<String>, Operation> specification;

    /**
     * Creates a checker with the given sequential specification.
     * The specification takes the current state (set of existing objects) and an operation,
     * and returns true if the operation's result is consistent with executing against that state.
     *
     * @param specification validates whether an operation result is consistent with the state
     */
    public LinearizabilityChecker(BiPredicate<Set<String>, Operation> specification) {
        this.specification = Objects.requireNonNull(specification, "specification must not be null");
    }

    /**
     * Checks whether the given history is linearizable.
     * Returns a report indicating success (with a witness linearization) or failure (with violation details).
     */
    public LinearizabilityReport check(List<Operation> history) {
        Objects.requireNonNull(history, "history must not be null");
        if (history.isEmpty()) {
            return LinearizabilityReport.pass(history, List.of());
        }

        LOG.debug("Checking linearizability of history with {} operations", history.size());

        List<Operation> linearization = new ArrayList<>();
        Set<String> state = new HashSet<>();
        Set<Integer> used = new HashSet<>();

        if (search(history, linearization, state, used)) {
            LOG.debug("History is linearizable, witness has {} operations", linearization.size());
            return LinearizabilityReport.pass(history, List.copyOf(linearization));
        }

        String violation = buildViolationDescription(history);
        LOG.warn("History is NOT linearizable: {}", violation);
        return LinearizabilityReport.fail(history, violation);
    }

    private boolean search(
            List<Operation> history,
            List<Operation> linearization,
            Set<String> state,
            Set<Integer> used
    ) {
        if (used.size() == history.size()) {
            return true;
        }

        for (int i = 0; i < history.size(); i++) {
            if (used.contains(i)) {
                continue;
            }

            Operation candidate = history.get(i);

            if (!canLinearizeNext(history, linearization, candidate)) {
                continue;
            }

            if (!specification.test(state, candidate)) {
                continue;
            }

            // Commit this choice
            used.add(i);
            linearization.add(candidate);
            Set<String> savedState = new HashSet<>(state);
            applyToState(state, candidate);

            if (search(history, linearization, state, used)) {
                return true;
            }

            // Backtrack
            used.remove(i);
            linearization.remove(linearization.size() - 1);
            state.clear();
            state.addAll(savedState);
        }

        return false;
    }

    /**
     * Checks whether the candidate can be linearized next without violating real-time ordering.
     * A candidate cannot be placed after an operation that it happens-before.
     */
    private boolean canLinearizeNext(List<Operation> history, List<Operation> linearization, Operation candidate) {
        for (int i = 0; i < history.size(); i++) {
            Operation other = history.get(i);
            if (other == candidate) {
                continue;
            }
            // If 'other' happens-before 'candidate' and 'other' is not yet linearized, we cannot pick candidate
            if (other.happensBefore(candidate) && !linearization.contains(other)) {
                return false;
            }
            // If 'candidate' happens-before 'other' and 'other' is already linearized, ordering is violated
            if (candidate.happensBefore(other) && linearization.contains(other)) {
                return false;
            }
        }
        return true;
    }

    private void applyToState(Set<String> state, Operation operation) {
        switch (operation.type()) {
            case "createTable" -> state.add(operation.argument());
            case "dropTable" -> state.remove(operation.argument());
            default -> { /* read-only operations do not modify state */ }
        }
    }

    private String buildViolationDescription(List<Operation> history) {
        var sb = new StringBuilder("No valid linearization found for ")
                .append(history.size())
                .append(" operations. Operations: ");
        for (int i = 0; i < history.size(); i++) {
            Operation op = history.get(i);
            sb.append('[').append(op.clientId()).append(':')
                    .append(op.type()).append('(').append(op.argument()).append(')')
                    .append("->").append(op.result()).append(']');
            if (i < history.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
