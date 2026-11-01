package org.pangrova.spi.security;

import org.pangrova.common.api.annotation.Stable;

/**
 * Access control SPI for imperative privilege checks and declarative policy retrieval.
 * Stable contract — implementations reside in pangrova-security.
 *
 * <p>Composed from focused sub-interfaces for catalog, schema, table, and column
 * access control. Consumers that only need a subset of checks can depend on the
 * narrower sub-interface instead.
 */
@Stable(since = "1.0")
public interface AccessControl
        extends CatalogAccessControl, SchemaAccessControl, TableAccessControl, ColumnAccessControl,
        StorageProfileAccessControl, FileSpaceAccessControl {
}
