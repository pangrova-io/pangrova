package org.pangrova.common.exception;

import java.util.List;

/**
 * Thrown by {@link SubsystemActivationCheck#runOrAbort()} when one or more registered activation
 * predicates fail at bootstrap. Carries the offending inventory IDs so startup logs name exactly
 * which subsystems are wired-but-inert (Requirement T14, design principle 4).
 */
public final class SubsystemActivationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<String> failedIds;

    public SubsystemActivationException(List<String> failedIds) {
        super("Subsystem activation self-check failed for IDs: " + failedIds
                + " — these subsystems are wired but not active. Fix the wiring or remove the inert path.");
        this.failedIds = List.copyOf(failedIds);
    }

    public List<String> failedIds() {
        return failedIds;
    }
}
