package org.pangrova.parser.parse;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.expression.SubscriptExpression;
import org.pangrova.parser.api.ast.statement.CreateStorageProfileStatement;
import org.pangrova.parser.api.ast.statement.DropStorageProfileStatement;
import org.pangrova.parser.api.ast.statement.ShowStorageProfilesStatement;
import org.pangrova.parser.api.ast.statement.ShowTableControlsStatement;

import static org.assertj.core.api.Assertions.assertThat;

class AstNodeTypeIdentityTest {
    @ParameterizedTest
    @MethodSource("statements")
    void existingStatementsExposeTheirOwnType(Statement statement, String expected) {
        assertThat(statement.type().name()).isEqualTo(expected);
    }

    static Stream<Arguments> statements() {
        return Stream.of(
                Arguments.of(new org.pangrova.parser.api.ast.statement.privilege.GrantPrivilegeStatement(
                        List.of("SELECT"), "TABLE", Identifier.of("t"), List.of("analyst"), false), "GRANT_PRIVILEGE"),
                Arguments.of(new org.pangrova.parser.api.ast.statement.privilege.RevokePrivilegeStatement(
                        List.of("SELECT"), "TABLE", Identifier.of("t"), List.of("analyst"), false, false), "REVOKE_PRIVILEGE"),
                Arguments.of(new CreateStorageProfileStatement("local", false, Map.of()), "CREATE_STORAGE_PROFILE"),
                Arguments.of(new DropStorageProfileStatement("local", false), "DROP_STORAGE_PROFILE"),
                Arguments.of(new ShowStorageProfilesStatement(), "SHOW_STORAGE_PROFILES"),
                Arguments.of(new ShowTableControlsStatement(List.of()), "SHOW_TABLE_CONTROLS"));
    }

    @Test
    void subscriptExposesItsOwnExpressionType() {
        var expression = new SubscriptExpression(Identifier.of("items"), Literal.ofInteger(1));
        assertThat(expression.type().name()).isEqualTo("SUBSCRIPT");
    }
}
