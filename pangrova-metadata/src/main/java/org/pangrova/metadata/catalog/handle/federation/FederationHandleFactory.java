package org.pangrova.metadata.catalog.handle.federation;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;

/**
 * Factory for creating federation-backed catalog handles.
 */
@FunctionalInterface
public interface FederationHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
