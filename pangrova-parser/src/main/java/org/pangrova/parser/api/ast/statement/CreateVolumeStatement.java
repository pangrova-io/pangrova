package org.pangrova.parser.api.ast.statement;

import java.util.Optional;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents {@code CREATE [MANAGED | EXTERNAL] VOLUME [IF NOT EXISTS] name ...}.
 */
public record CreateVolumeStatement(
        Identifier name,
        boolean external,
        boolean ifNotExists,
        Optional<String> location,
        Optional<String> storageProfile,
        Optional<String> comment) implements Statement {

    @Override
    public StatementType type() {
        return StatementType.CREATE_VOLUME;
    }
}
