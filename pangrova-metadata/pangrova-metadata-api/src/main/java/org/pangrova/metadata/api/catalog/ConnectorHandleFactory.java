package org.pangrova.metadata.api.catalog;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Factory for creating connector-backed catalog handles.
 */
@FunctionalInterface
@Experimental
public interface ConnectorHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
