package org.pangrova.metadata.api.catalog;

/**
 * Lifecycle states of a catalog.
 */
public enum CatalogState {
    CREATING,
    ACTIVE,
    DEGRADED,
    SUSPENDED,
    DROPPING
}
