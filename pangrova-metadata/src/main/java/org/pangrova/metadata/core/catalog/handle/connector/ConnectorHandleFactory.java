package org.pangrova.metadata.core.catalog.handle.connector;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;

/**
 * Factory for creating connector-backed catalog handles.
 */
@FunctionalInterface
public interface ConnectorHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
