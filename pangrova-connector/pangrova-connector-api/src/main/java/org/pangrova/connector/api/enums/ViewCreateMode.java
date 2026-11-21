package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Atomic conflict semantics for one view definition; unsupported modes must fail before mutation. */
@Experimental
public enum ViewCreateMode {
    /** Fail if the name already exists; never overwrite it. */
    CREATE,
    /** Preserve an existing definition, including a different definition. */
    IF_NOT_EXISTS,
    /** Create or atomically replace the existing definition. Caller must authorize both operations. */
    CREATE_OR_REPLACE
}
