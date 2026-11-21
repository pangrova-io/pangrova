package org.pangrova.parser.api.ast.statement;

import java.util.Optional;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

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

    @Override
    public StatementType type() {
        return StatementType.CREATE_STAGE;
    }
}
