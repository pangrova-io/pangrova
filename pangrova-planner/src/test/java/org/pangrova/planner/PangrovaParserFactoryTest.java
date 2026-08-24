package org.pangrova.planner;

import org.pangrova.parser.ast.Statement;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaParserFactoryTest {

    private final PangrovaParserFactory factory = new PangrovaParserFactory();

    @Test
    void parseToSqlNodeSelect1() {
        SqlNode node = factory.parseToSqlNode("SELECT 1");
        assertThat(node).isInstanceOf(SqlSelect.class);
        assertThat(node.toString()).contains("1");
    }

    @Test
    void parseToSqlNodeSelectString() {
        SqlNode node = factory.parseToSqlNode("SELECT 'hello'");
        assertThat(node.toString()).contains("'hello'");
    }

    @Test
    void parseToSqlNodeSelectArithmetic() {
        SqlNode node = factory.parseToSqlNode("SELECT 1 + 2");
        assertThat(node.toString()).contains("1 + 2");
    }

    @Test
    void parseToAstReturnsStatement() {
        Statement stmt = factory.parseToAst("SELECT 1");
        assertThat(stmt).isNotNull();
    }

    @Test
    void getParserThrowsUnsupported() {
        assertThatThrownBy(() -> factory.getParser(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void parseToSqlNodeCompact() {
        SqlNode node = factory.parseToSqlNode("COMPACT my_table");
        assertThat(node).isNotNull();
    }
}
