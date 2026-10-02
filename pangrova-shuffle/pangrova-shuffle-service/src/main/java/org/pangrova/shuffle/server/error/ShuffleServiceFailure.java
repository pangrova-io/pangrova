package org.pangrova.shuffle.server.error;

public final class ShuffleServiceFailure extends RuntimeException {

    private final FailureKind kind;

    public ShuffleServiceFailure(FailureKind kind, String message) {
        super(message);
        this.kind = kind;
    }

    public FailureKind kind() {
        return kind;
    }
}
