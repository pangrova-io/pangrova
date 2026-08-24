package org.pangrova.metadata.catalog;

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
