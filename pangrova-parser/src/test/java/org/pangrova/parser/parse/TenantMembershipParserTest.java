package org.pangrova.parser.parse;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.ast.statement.tenant.GrantTenantStatement;
import org.pangrova.parser.api.ast.statement.tenant.RevokeTenantStatement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantMembershipParserTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void quotedTenantAndPrincipalRemainSingleExactComponents() {
        var grant = (GrantTenantStatement) parser.parse("GRANT TENANT \"Case.Tenant\" TO \"User.Name\"");
        assertThat(grant.tenantName().parts()).containsExactly("Case.Tenant");
        assertThat(grant.principal().parts()).containsExactly("User.Name");
        var revoke = (RevokeTenantStatement) parser.parse("REVOKE TENANT \"Case.Tenant\" FROM \"User.Name\"");
        assertThat(revoke.tenantName()).isEqualTo(grant.tenantName());
        assertThat(revoke.principal()).isEqualTo(grant.principal());
        var printer = new org.pangrova.parser.format.AstPrinter();
        assertThat(parser.parse(printer.print(grant))).isEqualTo(grant);
        assertThat(parser.parse(printer.print(revoke))).isEqualTo(revoke);
    }

    @Test
    void qualifiedOrMultiplePrincipalsAreNotSilentlyTruncated() {
        for (String sql : java.util.List.of("GRANT TENANT a.b TO user", "GRANT TENANT a TO b.c",
                "REVOKE TENANT a FROM b, c")) {
            assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(org.pangrova.parser.api.exception.ParseException.class);
        }
    }
}
