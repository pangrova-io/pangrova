package org.pangrova.table.api.ownership;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.connector.api.metadata.table.TableIdentity;

import java.util.Optional;

/** Current table ownership used to fence connector reads and cached read bindings. */
@Internal
public interface TableReadAuthority {
    boolean enabled();

    Optional<TableControlRecord> find(String catalog, String schema, String table);

    /** Rejects a record belonging to a retired catalog deployment. */
    void requireCurrentCatalog(TableControlRecord record, String operation);

    /** Rejects a live physical table whose identity differs from the captured owner. */
    void requireReadIdentity(TableControlRecord expected, Optional<TableIdentity> identity, String operation);

    /** Rechecks the captured identity and its active state before reusing a bound read. */
    void requireReadOwner(TableControlRecord expected, String operation);
}
