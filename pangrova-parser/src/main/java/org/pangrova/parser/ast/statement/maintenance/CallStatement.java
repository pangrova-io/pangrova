package org.pangrova.parser.ast.statement.maintenance;

import java.util.List;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents CALL procedure(args...) statement.
 */
public record CallStatement(Identifier procedureName, List<Expression> arguments) implements Statement {

    public CallStatement {
        arguments = List.copyOf(arguments);
    }
}
