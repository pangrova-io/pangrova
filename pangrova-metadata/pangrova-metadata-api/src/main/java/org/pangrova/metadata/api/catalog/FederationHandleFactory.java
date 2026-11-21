package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Factory for creating federation-backed catalog handles.
 */
@FunctionalInterface
@Experimental
public interface FederationHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
