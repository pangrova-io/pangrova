package org.pangrova.metadata.core.catalog;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;

/**
 * Factory for creating federation-backed catalog handles.
 */
@FunctionalInterface
public interface FederationHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
