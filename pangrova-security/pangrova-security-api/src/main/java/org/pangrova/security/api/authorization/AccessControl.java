package org.pangrova.security.api.authorization;

import org.pangrova.common.api.annotation.Stable;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.api.mask.TableColumnMasks;

import java.util.Optional;

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
        StorageProfileAccessControl, FileSpaceAccessControl, ViewAccessControl {

    /** Binds one synchronous plan authorization pass to its authenticated subject. */
    default ReadAuthorization beginReadAuthorization(UserIdentity user) {
        return new ReadAuthorization() {
            @Override
            public void requireSelect(String catalog, String schema, String table) {
                checkCanSelectFromTable(user, catalog, schema, table);
            }

            @Override
            public void requireUnfilteredMetadata(String catalog, String schema, String table) {
                checkCanReadTableMetadata(user, catalog, schema, table);
            }

            @Override
            public Optional<String> rowFilter(String catalog, String schema, String table) {
                return getRowFilter(user, catalog, schema, table);
            }

            @Override
            public TableColumnMasks columnMasks(String catalog, String schema, String table) {
                return getColumnMasks(user, catalog, schema, table);
            }
        };
    }

    /** Read privilege and policy evaluation for the same authenticated subject and plan. */
    interface ReadAuthorization {
        void requireSelect(String catalog, String schema, String table);

        void requireUnfilteredMetadata(String catalog, String schema, String table);

        Optional<String> rowFilter(String catalog, String schema, String table);

        TableColumnMasks columnMasks(String catalog, String schema, String table);
    }
}
