package org.pangrova.shuffle.pangrova.client;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;

/** Explicit Worker-owned ManagedChannel exhaustion; no implicit waiting or alternate route. */
public final class RemoteShuffleAdmissionException extends PangrovaException {
    private final String scope;
    private final int occupied;
    private final int limit;

    RemoteShuffleAdmissionException(String scope, int occupied, int limit) {
        super(PangrovaErrorCode.RUNTIME_RESOURCE_EXHAUSTED,
                "Shuffle ManagedChannel admission rejected: scope=" + scope
                        + " occupied=" + occupied + " limit=" + limit);
        this.scope = scope;
        this.occupied = occupied;
        this.limit = limit;
    }

    public String scope() { return scope; }
    public int occupied() { return occupied; }
    public int limit() { return limit; }

    public ExecutionError executionError() {
        return ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_ADMISSION)
                .setCode("SHUFFLE_MANAGED_CHANNEL_LIMIT").setMessage(getMessage())
                .setRetryable(true).setFailedComponent("remote-shuffle-channel-admission").build();
    }
}
