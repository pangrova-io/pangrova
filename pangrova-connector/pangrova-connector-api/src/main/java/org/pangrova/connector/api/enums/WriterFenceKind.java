package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Evidence required to fence a write against a replaced physical table. */
@Experimental
public enum WriterFenceKind {
    /** No fence is available; writes must not be admitted. */
    NONE,
    /** The handle must match the registered format identity. */
    FORMAT_IDENTITY,
    /** The handle must match the format identity and a pinned base version. */
    FORMAT_IDENTITY_AND_BASE_VERSION
}
