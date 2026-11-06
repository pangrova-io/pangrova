package org.pangrova.metadata.spi.catalog;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;

/**
 * Factory for creating connector-backed catalog handles.
 */
@FunctionalInterface
@Experimental
public interface ConnectorHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
