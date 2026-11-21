package org.pangrova.parser.api.ast.statement.maintenance;

import java.util.List;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents CALL procedure(args...) statement.
 */
public record CallStatement(Identifier procedureName, List<Expression> arguments) implements Statement {

    public CallStatement {
        arguments = List.copyOf(arguments);
    }

    @Override
    public StatementType type() {
        return StatementType.CALL;
    }
}
