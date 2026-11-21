package org.pangrova.shuffle.server.exception;

import org.pangrova.shuffle.server.enums.FailureKind;

public final class ShuffleServiceException extends RuntimeException {

    private final FailureKind kind;

    public ShuffleServiceException(FailureKind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public FailureKind kind() {
        return kind;
    }
}
