package org.pangrova.coordinator.ai.branch;

import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.spi.security.AccessControl;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Immutable dependency bundle for BranchDdlExecutor.
 *
 * @param catalogRegistry catalog registry for connector resolution
 * @param accessControl   access control for RBAC enforcement (nullable in tests)
 */
public record BranchDdlContext(
        CatalogRegistry catalogRegistry,
        AccessControl accessControl) {

    public BranchDdlContext {
        checkNotNull(catalogRegistry, "catalogRegistry");
    }
}
