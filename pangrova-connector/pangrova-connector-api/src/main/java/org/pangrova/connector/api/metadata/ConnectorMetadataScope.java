package org.pangrova.connector.api.metadata;

import org.pangrova.connector.api.metadata.ConnectorQueryCleanupResult;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.query.ConnectorQueryAuthority;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Query-state-bound owner of resolved connector metadata and its cleanup.
 *
 * <p>The scope is the same authority passed to connector operations, not a token
 * recovered by query ID. A raw task-issuance authority does not own metadata.</p>
 */
@Experimental
public interface ConnectorMetadataScope extends ConnectorQueryAuthority {
    /** Retains and begins the actual resolved metadata instance with this same public lifetime. */
    ConnectorMetadata beginMetadata(String resolvedCatalog, Supplier<ConnectorMetadata> lookup);

    /** Releases retained metadata after physical drain; failed cleanup remains owned for retry. */
    ConnectorQueryCleanupResult cleanupMetadata();

    /**
     * Requires metadata ownership without creating or resolving a replacement authority.
     *
     * @param authority the exact authority carried by the admitted connector session
     * @return that same object with its metadata capability
     * @throws IllegalStateException if only task-issuance ownership is available
     */
    static ConnectorMetadataScope require(ConnectorQueryAuthority authority) {
        Objects.requireNonNull(authority, "authority");
        if (authority instanceof ConnectorMetadataScope scope) {
            return scope;
        }
        throw new IllegalStateException("Connector metadata requires the public query state authority");
    }
}
