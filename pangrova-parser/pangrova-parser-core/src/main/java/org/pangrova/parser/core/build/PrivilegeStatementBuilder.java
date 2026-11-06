package org.pangrova.parser.core.build;

import org.pangrova.parser.api.ast.statement.GrantPrivilegeStatement;
import org.pangrova.parser.api.ast.statement.ShowPrivilegeMutationStatement;
import org.pangrova.parser.api.diagnostic.ParseException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.api.ast.statement.RevokePrivilegeStatement;
import org.pangrova.parser.core.build.antlr.PangrovaSqlParser;

import java.util.Locale;
import java.util.stream.IntStream;

/** Privilege grammar-to-value conversion; does not resolve or authorize roles. */
final class PrivilegeStatementBuilder {
    private final AstBuilder parent;

    PrivilegeStatementBuilder(AstBuilder parent) { this.parent = parent; }

    ShowPrivilegeMutationStatement receipt(PangrovaSqlParser.ShowPrivilegeMutationContext context) {
        var token = context.STRING_LITERAL().getSymbol();
        String literal = token.getText();
        String text = literal.substring(1, literal.length() - 1);
        try {
            java.util.UUID id = java.util.UUID.fromString(text);
            if (!id.toString().equalsIgnoreCase(text)) throw new IllegalArgumentException("Noncanonical UUID");
            return new ShowPrivilegeMutationStatement(id);
        } catch (IllegalArgumentException invalid) {
            throw new ParseException(PangrovaErrorCode.PARSER_SYNTAX_ERROR, "Expected canonical request UUID",
                    token.getLine(), token.getCharPositionInLine());
        }
    }

    GrantPrivilegeStatement grant(PangrovaSqlParser.GrantPrivilegeContext context) {
        return new GrantPrivilegeStatement(context.privilegeName().stream().map(this::privilege).toList(),
                objectType(context.privilegeObjectType()), parent.visitQualifiedName(context.qualifiedName()),
                context.identifier().stream().map(parent::extractIdentifierText).toList(), context.OPTION() != null);
    }

    RevokePrivilegeStatement revoke(PangrovaSqlParser.RevokePrivilegeContext context) {
        return new RevokePrivilegeStatement(context.privilegeName().stream().map(this::privilege).toList(),
                objectType(context.privilegeObjectType()), parent.visitQualifiedName(context.qualifiedName()),
                context.identifier().stream().map(parent::extractIdentifierText).toList(),
                context.OPTION() != null, context.CASCADE() != null);
    }

    private String privilege(PangrovaSqlParser.PrivilegeNameContext context) {
        if (context.ALL() != null) return "ALL";
        if (context.identifier() != null) {
            return parent.extractIdentifierText(context.identifier()).toUpperCase(Locale.ROOT);
        }
        return IntStream.range(0, context.getChildCount()).mapToObj(i -> context.getChild(i).getText())
                .collect(java.util.stream.Collectors.joining("_")).toUpperCase(Locale.ROOT);
    }

    private static String objectType(PangrovaSqlParser.PrivilegeObjectTypeContext context) {
        return context.STORAGE() != null ? "STORAGE_PROFILE" : context.getText().toUpperCase(Locale.ROOT);
    }
}
