package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;

/**
 * Factory for creating federation-backed catalog handles.
 */
@FunctionalInterface
@Experimental
public interface FederationHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
