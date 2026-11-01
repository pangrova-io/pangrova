package org.pangrova.metadata.core.catalog;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;

/**
 * Factory for creating connector-backed catalog handles.
 */
@FunctionalInterface
public interface ConnectorHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
