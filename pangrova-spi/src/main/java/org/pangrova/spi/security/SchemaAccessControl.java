package org.pangrova.spi.security;

import org.pangrova.spi.annotation.Experimental;

/**
 * Access control for schema-level operations including creation, deletion,
 * alteration, and listing of schemas and tables within a schema.
 */
@Experimental
public interface SchemaAccessControl {

    void checkCanAccessSchema(UserIdentity user, String catalog, String schema);

    void checkCanCreateSchema(UserIdentity user, String catalog, String schema);

    void checkCanDropSchema(UserIdentity user, String catalog, String schema);

    void checkCanAlterSchema(UserIdentity user, String catalog, String schema);

    void checkCanShowSchemas(UserIdentity user, String catalog);

    void checkCanShowTables(UserIdentity user, String catalog, String schema);
}
