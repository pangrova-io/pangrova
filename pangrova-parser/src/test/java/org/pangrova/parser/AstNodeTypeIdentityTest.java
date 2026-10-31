package org.pangrova.parser;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.expression.SubscriptExpression;
import org.pangrova.parser.ast.statement.CreateStorageProfileStatement;
import org.pangrova.parser.ast.statement.DropStorageProfileStatement;
import org.pangrova.parser.ast.statement.GrantPrivilegeStatement;
import org.pangrova.parser.ast.statement.RevokePrivilegeStatement;
import org.pangrova.parser.ast.statement.ShowStorageProfilesStatement;
import org.pangrova.parser.ast.statement.ShowTableControlsStatement;

import static org.assertj.core.api.Assertions.assertThat;

class AstNodeTypeIdentityTest {
    @ParameterizedTest
    @MethodSource("statements")
    void existingStatementsExposeTheirOwnType(Statement statement, String expected) {
        assertThat(statement.type().name()).isEqualTo(expected);
    }

    static Stream<Arguments> statements() {
        var table = Identifier.of("orders");
        var role = Identifier.of("analyst");
        return Stream.of(
                Arguments.of(new GrantPrivilegeStatement("SELECT", "TABLE", table, role, false), "GRANT_PRIVILEGE"),
                Arguments.of(new RevokePrivilegeStatement("SELECT", "TABLE", table, role, false), "REVOKE_PRIVILEGE"),
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
