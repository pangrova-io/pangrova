package org.pangrova.parser.ast.statement;

import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

/**
 * Represents {@code CREATE [INTERNAL | EXTERNAL] STAGE [IF NOT EXISTS] name ...}.
 */
public record CreateStageStatement(
        Identifier name,
        boolean external,
        boolean ifNotExists,
        Optional<String> url,
        Optional<String> storageProfile,
        boolean directoryEnabled,
        Optional<String> comment) implements Statement {
}
