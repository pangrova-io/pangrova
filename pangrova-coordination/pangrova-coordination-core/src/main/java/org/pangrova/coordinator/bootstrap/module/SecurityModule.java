package org.pangrova.coordinator.bootstrap.module;

import org.pangrova.coordinator.query.security.AccessControlEnforcer;
import org.pangrova.planner.security.ColumnMaskApplier;
import org.pangrova.planner.security.RowFilterApplier;
import org.pangrova.security.api.AccessControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Security module (priority 10).
 * Binds access control, column masking, and row filtering components
 * when an {@link AccessControl} instance is available.
 */
public final class SecurityModule implements CoordinatorModule {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityModule.class);

    private final AccessControl accessControl;

    public SecurityModule(AccessControl accessControl) {
        this.accessControl = accessControl;
    }

    @Override
    public void configure(CoordinatorModuleContext context) {
        if (accessControl == null) {
            LOG.info("event=module_skipped module=Security reason=no_access_control");
            return;
        }
        context.bind(AccessControlEnforcer.class, new AccessControlEnforcer(accessControl));
        context.bind(ColumnMaskApplier.class, new ColumnMaskApplier(accessControl));
        context.bind(RowFilterApplier.class, new RowFilterApplier(accessControl));
        LOG.info("event=module_configured module=Security");
    }

    @Override
    public int priority() {
        return 10;
    }
}
