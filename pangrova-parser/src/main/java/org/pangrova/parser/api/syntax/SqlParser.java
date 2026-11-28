package org.pangrova.parser.api.syntax;

import org.pangrova.parser.api.enums.SqlDialect;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.exception.ParseException;

/**
 * Thread-safe SQL parsing contract. Strict parsing returns a complete AST or throws
 * ParseException; recovery reports syntax errors and an optional partial AST.
 * Resource-limit rejection throws in both modes. No result owns external resources.
 * Parsing is synchronous and does not promise asynchronous cancellation.
 */
@Experimental
public interface SqlParser {
    Statement parse(String sql) throws ParseException;
    ParseResult parseWithRecovery(String sql) throws ParseException;
    SqlDialect dialect();
}
