package org.pangrova.security.spi.authorization;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.identity.UserIdentity;

/**
 * Checks current relation-scoped view authority before any connector operation.
 * Creation requires CREATE_VIEW; replacement additionally requires ALTER, even
 * when the target does not yet exist. DROP requires DROP, including IF EXISTS.
 * These checks do not grant definer rights to read the view's source relations.
 */
@Experimental
public interface ViewAccessControl {
    void checkCanCreateView(UserIdentity user, String catalog, String schema, String view, boolean replace);

    void checkCanDropView(UserIdentity user, String catalog, String schema, String view);
}
