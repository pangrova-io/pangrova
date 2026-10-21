package org.pangrova.metadata.dynamictable;

import org.pangrova.spi.coordination.ExecutionAnchor;

import java.util.Objects;

/** Exact server-issued execution owner, control term and authenticated refresh principal. */
public record DynamicTableRefreshIssuer(ExecutionAnchor owner, long controlEpoch, String principal) {
    public DynamicTableRefreshIssuer {
        Objects.requireNonNull(owner, "owner");
        if (controlEpoch <= 0 || principal == null || principal.isBlank()) {
            throw new IllegalArgumentException("Dynamic Table refresh requires a control term and principal");
        }
    }
}
