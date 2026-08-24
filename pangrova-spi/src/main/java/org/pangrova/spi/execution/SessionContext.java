package org.pangrova.spi.execution;

import org.pangrova.spi.annotation.Internal;

import java.util.Map;
import java.util.Optional;

/**
 * Provides session-level context for query execution.
 *
 * <p>Carries user identity, optional catalog/schema scope, and session
 * properties that influence query planning and execution behavior.</p>
 */
@Internal
public interface SessionContext {

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
}
