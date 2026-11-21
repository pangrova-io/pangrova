package org.pangrova.metadata.api.catalog;

import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable descriptor for a registered catalog.
 *
 * <p>As a pure data model (value object), the record canonical constructor
 * is exempt from the ≤4 constructor param governance rule — it carries
 * no behavioural dependencies, only descriptive fields.</p>
 */
public record CatalogDescriptor(
        String name,
        CatalogType type,
        String provider,
        CatalogState state,
        Map<String, String> properties,
        String comment,
        Set<String> allowedTenants,
        String incarnationId,
        long revision) {

    /** Creates the first revision of a new catalog incarnation. */
    public CatalogDescriptor(
            String name,
            CatalogType type,
            String provider,
            CatalogState state,
            Map<String, String> properties,
            String comment,
            Set<String> allowedTenants) {
        this(name, type, provider, state, properties, comment, allowedTenants,
                UUID.randomUUID().toString(), 1);
    }

    public CatalogDescriptor {
        checkNotNull(name, "name must not be null");
        checkNotNull(type, "type must not be null");
        checkNotNull(provider, "provider must not be null");
        checkNotNull(state, "state must not be null");
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        comment = comment == null ? "" : comment;
        allowedTenants = allowedTenants == null ? Set.of() : Set.copyOf(allowedTenants);
        if (incarnationId == null || incarnationId.isBlank()) {
            throw new IllegalArgumentException("incarnationId must not be blank");
        }
        if (revision <= 0) {
            throw new IllegalArgumentException("revision must be positive");
        }
    }

    /** The immutable revision that created a catalog handle or background operation. */
    public CatalogDeploymentIdentity identity() {
        return new CatalogDeploymentIdentity(name, incarnationId, revision);
    }
}
