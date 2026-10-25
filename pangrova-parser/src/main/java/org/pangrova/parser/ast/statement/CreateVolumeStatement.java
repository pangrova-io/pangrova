package org.pangrova.parser.ast.statement;

import java.util.Optional;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;

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
}
