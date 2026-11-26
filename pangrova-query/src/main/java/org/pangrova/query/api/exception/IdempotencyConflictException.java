package org.pangrova.query.api.exception;

/** An idempotency key was already accepted for a different canonical request. */
public final class IdempotencyConflictException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public IdempotencyConflictException() {
        super("Idempotency key was already used for a different request");
    }
}
