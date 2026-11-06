package org.pangrova.persistence.catalog.api.invalidation;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Prunes expired invalidation entries within the currently observed processing floor.
 * The provider must retain entries that any registered consumer may still need.
 * This is best-effort maintenance, not an acknowledgement that all expired entries were removed.
 * The caller borrows this port and does not acquire ownership of its scheduler or DataSource.
 */
@Experimental
@FunctionalInterface
public interface InvalidationLogMaintenance {
    /** Returns the count removed in this pass; zero does not establish catch-up or readiness. */
    int cleanup();
}
