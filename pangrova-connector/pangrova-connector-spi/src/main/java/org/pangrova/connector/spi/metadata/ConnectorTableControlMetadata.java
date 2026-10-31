package org.pangrova.connector.spi.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.TableCapabilities;
import org.pangrova.connector.spi.metadata.table.TableIdentity;
import org.pangrova.connector.spi.metadata.table.TableVersionToken;

import java.util.Optional;

/**
 * Optional table-control evidence a connector can publish.
 *
 * <p>Both methods are fail-closed: an empty result means the connector cannot
 * prove identity or version, which callers must treat as "unknown", never as a
 * match. Implementations load the current table state; they must not reuse a
 * stale handle snapshot as identity evidence.</p>
 */
@Experimental
public interface ConnectorTableControlMetadata {

    /** Returns the format-level identity of the physical table, or empty if unsupported. */
    default Optional<TableIdentity> getTableIdentity(
            ConnectorSession session, ConnectorTableHandle handle) {
        return Optional.empty();
    }

    /** Returns the current version token of the physical table, or empty if unsupported. */
    default Optional<TableVersionToken> observeVersion(
            ConnectorSession session, ConnectorTableHandle handle) {
        return Optional.empty();
    }

    /**
     * Returns the capabilities this connector can prove for the table.
     *
     * <p>Undeclared is unsupported: callers must reject write, delete,
     * maintenance, and equality-delete reads the connector has not declared.</p>
     */
    default TableCapabilities getTableCapabilities(
            ConnectorSession session, ConnectorTableHandle handle) {
        return TableCapabilities.UNSUPPORTED;
    }
}
