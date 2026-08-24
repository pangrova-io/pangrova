package org.pangrova.operations.recovery;

/**
 * Functional interface for SOP recovery actions.
 */
@FunctionalInterface
public interface RecoveryAction {

    void execute(ErrorContext context);
}
