package org.pangrova.worker.exception;

import org.pangrova.protocol.v2.ErrorClass;

/** Distinguishes admission/deadline/cancellation from backend failures without hiding usage. */
public final class CopyPreparationHaltedException extends IllegalStateException {
    private final ErrorClass category;

    public CopyPreparationHaltedException(ErrorClass category, String message) {
        super(message);
        this.category = category;
    }
    public ErrorClass category() { return category; }
}
