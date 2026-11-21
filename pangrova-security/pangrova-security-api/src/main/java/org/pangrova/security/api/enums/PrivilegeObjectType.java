package org.pangrova.security.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Kind of authority object named in a privilege grant. */
@Experimental
public enum PrivilegeObjectType {
    CATALOG,
    SCHEMA,
    TABLE,
    VOLUME,
    STAGE,
    STORAGE_PROFILE
}
