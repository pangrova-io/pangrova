package org.pangrova.security.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Change requested for one or more privilege grants. */
@Experimental
public enum PrivilegeOperation {
    GRANT,
    REVOKE,
    REVOKE_GRANT_OPTION
}
