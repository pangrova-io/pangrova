package org.pangrova.observability.api.exception;

/** Requested profiling is not implemented or enabled by this node. */
public final class ProfilingUnavailableException extends UnsupportedOperationException {
    private static final long serialVersionUID = 1L;

    public ProfilingUnavailableException(String message) { super(message); }
}
