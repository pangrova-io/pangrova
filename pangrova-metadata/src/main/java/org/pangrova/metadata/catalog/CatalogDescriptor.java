package org.pangrova.metadata.catalog;

import java.util.Map;
import java.util.Set;

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
        Set<String> allowedTenants) {

    public CatalogDescriptor {
        checkNotNull(name, "name must not be null");
        checkNotNull(type, "type must not be null");
        checkNotNull(provider, "provider must not be null");
        checkNotNull(state, "state must not be null");
        properties = properties == null ? Map.of() : Map.copyOf(properties);
        comment = comment == null ? "" : comment;
        allowedTenants = allowedTenants == null ? Set.of() : Set.copyOf(allowedTenants);
    }
}
