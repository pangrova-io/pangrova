package org.pangrova.connector.api.enums;

/** Row-kind semantics a CDC connector sink must preserve. */
public enum CdcRowKind {
    INSERT,
    UPDATE_BEFORE,
    UPDATE_AFTER,
    DELETE
}
