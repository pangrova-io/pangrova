package org.pangrova.metadata.catalog;

/**
 * Factory for creating federation-backed catalog handles.
 */
@FunctionalInterface
public interface FederationHandleFactory {
    CatalogHandle create(CatalogDescriptor descriptor);
}
