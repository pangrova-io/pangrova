package org.pangrova.planner.analysis;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaParserFactoryTest {

    @ParameterizedTest
    @ValueSource(strings = {"SELECT UUID '00112233-4455-6677-8899-aabbccddeeff'",
            "SELECT CAST(NULL AS UUID)",
            "SELECT ARRAY[UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', CAST(NULL AS UUID)]"})
    void uuidSyntaxReachesTheNativeRelationalTypeSystem(String sql) {
        SqlNode node = factory.parseToSqlNode(sql);
        assertThat(node).isInstanceOf(SqlSelect.class);
        assertThat(node.toString()).contains("UUID");
    }

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
