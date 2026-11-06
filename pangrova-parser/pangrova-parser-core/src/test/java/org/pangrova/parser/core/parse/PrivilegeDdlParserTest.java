package org.pangrova.parser.core.parse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.parser.api.diagnostic.ParseException;
import org.pangrova.parser.api.ast.statement.GrantPrivilegeStatement;
import org.pangrova.parser.api.ast.statement.RevokePrivilegeStatement;
import org.pangrova.parser.core.format.AstPrinter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrivilegeDdlParserTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "SHOW PRIVILEGE MUTATION '81d4b111-c82e-4cfe-8ea2-fbb0dcfd1935'",
            "GRANT SELECT, INSERT ON TABLE lake.sales.orders TO ROLE analyst, reader WITH GRANT OPTION",
            "GRANT ALL PRIVILEGES ON SCHEMA lake.sales TO analyst",
            "GRANT CREATE TABLE, CREATE VIEW ON SCHEMA sales TO analyst",
            "GRANT READ_VOLUME ON VOLUME lake.sales.files TO analyst",
            "GRANT READ_STAGE ON STAGE lake.sales.raw TO analyst",
            "GRANT MANAGE_STORAGE_PROFILE ON STORAGE PROFILE local TO analyst",
            "REVOKE SELECT ON TABLE lake.sales.orders FROM analyst",
            "REVOKE GRANT OPTION FOR SELECT ON TABLE lake.sales.orders FROM ROLE analyst CASCADE",
            "REVOKE ALL PRIVILEGES ON CATALOG lake FROM analyst, reader RESTRICT",
            "GRANT SELECT ON TABLE \"Lake.One\".\"Sales\".\"Orders\" TO ROLE \"Analyst.Role\""
    })
    void parsePrintParseRetainsFullStatement(String sql) {
        var first = parser.parse(sql);
        assertThat(parser.parse(new AstPrinter().print(first))).isEqualTo(first);
        assertThat(parser.parseWithRecovery(sql).hasErrors()).isFalse();
    }

    @Test
    void multiPrivilegeGrantRetainsNamesAndOption() {
        var statement = (GrantPrivilegeStatement) parser.parse(
                "grant select, insert on table lake.sales.orders to role analyst, reader with grant option");
        assertThat(statement.privileges()).containsExactly("SELECT", "INSERT");
        assertThat(statement.roleNames()).containsExactly("analyst", "reader");
        assertThat(statement.objectName().parts()).containsExactly("lake", "sales", "orders");
        assertThat(statement.withGrantOption()).isTrue();
    }

    @Test
    void revokeDefaultsToRestrict() {
        var statement = (RevokePrivilegeStatement) parser.parse("REVOKE SELECT ON TABLE t FROM r");
        assertThat(statement.cascade()).isFalse();
        assertThat(statement.grantOptionOnly()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SHOW PRIVILEGE MUTATION 'not-a-uuid'",
            "SHOW PRIVILEGE MUTATION '1-1-1-1-1'",
            "GRANT SELECT ON TABLE t TO",
            "GRANT SELECT ON TABLE t TO r WITH GRANT",
            "REVOKE SELECT ON TABLE t FROM r WITH GRANT OPTION",
            "GRANT SELECT ON TABLE t TO r CASCADE",
            "GRANT SELECT ON TABLE * TO r"
    })
    void malformedPrivilegeStatementsAreRejected(String sql) {
        assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(ParseException.class);
        assertThat(parser.parseWithRecovery(sql).hasErrors()).isTrue();
    }
}
