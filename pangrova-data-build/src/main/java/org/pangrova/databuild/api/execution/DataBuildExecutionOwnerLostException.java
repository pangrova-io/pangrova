package org.pangrova.databuild.api.execution;

import org.pangrova.common.api.annotation.Internal;

/** Stale Artifact execution ownership; storage failure and corrupt routing are different failures. */
@Internal
public final class DataBuildExecutionOwnerLostException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public DataBuildExecutionOwnerLostException(String message, Throwable cause) {
        super(message, cause);
    }
}
