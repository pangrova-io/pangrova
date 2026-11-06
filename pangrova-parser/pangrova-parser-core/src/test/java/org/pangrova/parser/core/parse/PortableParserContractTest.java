package org.pangrova.parser.core.parse;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.parse.SqlParser;
import org.pangrova.parser.api.parse.SqlDialect;
import org.pangrova.parser.api.ast.StatementType;
import org.pangrova.parser.api.diagnostic.ParseException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PortableParserContractTest {
    @Test
    void realImplementationSupportsStrictAndRecoveryThroughPortableContract() {
        SqlParser parser = new AntlrSqlParser();
        assertThat(parser.dialect()).isEqualTo(SqlDialect.PANGROVA);
        assertThat(parser.parse("SHOW CATALOGS").type()).isEqualTo(StatementType.SHOW_CATALOGS);
        assertThat(parser.parseWithRecovery("SHOW CATALOGS").isSuccess()).isTrue();
        assertThat(parser.parseWithRecovery("SELECT FROM WHERE").hasErrors()).isTrue();
        assertThatThrownBy(() -> parser.parse("SELECT FROM WHERE")).isInstanceOf(ParseException.class);
        SqlParser bounded = new AntlrSqlParser(16, 8);
        assertThatThrownBy(() -> bounded.parseWithRecovery("SELECT " + "a".repeat(20)))
                .isInstanceOf(ParseException.class);
    }
}
