package org.pangrova.client.jdbc.enums;

import org.pangrova.common.api.annotation.Internal;

/** Top-level SQL keyword classification used by the JDBC statement. */
@Internal
public enum StatementKeywordKind {
    DML,
    SELECT,
    OTHER
}
