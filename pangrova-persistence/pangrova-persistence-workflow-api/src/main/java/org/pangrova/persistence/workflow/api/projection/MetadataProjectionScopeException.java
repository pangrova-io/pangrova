package org.pangrova.persistence.workflow.api.projection;

import org.pangrova.common.api.annotation.Experimental;

/** Invalid routing from a source handle; retry or full-sync fallback must not bless another target. */
@Experimental
public final class MetadataProjectionScopeException extends IllegalArgumentException {
    public MetadataProjectionScopeException(String message) {
        super(message);
    }
}
