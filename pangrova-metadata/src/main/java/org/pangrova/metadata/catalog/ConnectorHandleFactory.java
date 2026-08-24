package org.pangrova.metadata.catalog;

/**
 * Factory for creating connector-backed catalog handles.
 */
@FunctionalInterface
public interface ConnectorHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
