package org.pangrova.connector.api.session;

import org.pangrova.connector.api.query.ConnectorQueryAuthority;

import org.pangrova.common.api.annotation.Stable;

import java.util.Map;
import java.util.Optional;

/**
 * Provides session context to connector operations.
 *
 * <p>Implementations supply user identity, catalog/schema scope, and
 * session-level properties that may influence connector behavior.</p>
 */
@Stable(since = "1.0")
public interface ConnectorSession {

    /**
     * Returns the authenticated user name for this session.
     *
     * @return the user name, never null
     */
    String getUser();

    /**
     * Returns the current catalog name, if set.
     *
     * @return an optional containing the catalog name
     */
    Optional<String> getCatalog();

    /**
     * Returns the current schema name, if set.
     *
     * @return an optional containing the schema name
     */
    Optional<String> getSchema();

    /**
     * Returns the session properties as an unmodifiable map.
     *
     * @return session properties, never null
     */
    Map<String, String> getProperties();

    /**
     * Returns the actual engine-issued query lifetime, when this is an admitted query session.
     * Query-free metadata discovery sessions have no authority; properties cannot substitute for one.
     *
     * @return the query creation owner's authority, preserved by session copies
     */
    Optional<ConnectorQueryAuthority> getQueryAuthority();
}
