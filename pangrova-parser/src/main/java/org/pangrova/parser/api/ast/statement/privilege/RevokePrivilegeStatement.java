package org.pangrova.parser.api.ast.statement.privilege;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

import java.util.List;
import java.util.Objects;

/** Parsed privilege statement. Name resolution and current authorization belong to the execution boundary. */
@Experimental
public record RevokePrivilegeStatement(List<String> privileges, String objectType, Identifier objectName,
        List<String> roleNames, boolean grantOptionOnly, boolean cascade) implements Statement {
    public RevokePrivilegeStatement {
        privileges = List.copyOf(privileges);
        roleNames = List.copyOf(roleNames);
        Objects.requireNonNull(objectType, "objectType");
        Objects.requireNonNull(objectName, "objectName");
        if (privileges.isEmpty() || roleNames.isEmpty()) {
            throw new IllegalArgumentException("Privilege and recipient lists must not be empty");
        }
    }

    @Override
    public StatementType type() { return StatementType.REVOKE_PRIVILEGE; }
}
