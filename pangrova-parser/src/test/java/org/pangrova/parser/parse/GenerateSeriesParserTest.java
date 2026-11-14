package org.pangrova.parser.parse;


import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.TableFunctionRef;

import static org.assertj.core.api.Assertions.assertThat;

class GenerateSeriesParserTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void acceptsDirectAndWrappedThreeArgumentCalls() {
        for (String table : new String[]{"GENERATE_SERIES(10, 1, -3)",
                "TABLE(GENERATE_SERIES(10, 1, -3))"}) {
            var query = (Query) parser.parse("SELECT n FROM " + table + " AS s(n)");
            TableFunctionRef function = query.from().get(0).tableFunctionRef().orElseThrow();
            assertThat(function.functionType()).isEqualTo(TableFunctionRef.TableFunctionType.GENERATE_SERIES);
            assertThat(function.endArgument()).isPresent();
            assertThat(function.stepArgument()).isPresent();
            assertThat(function.alias()).contains("s");
            assertThat(function.columnAliases()).containsExactly("n");
        }
    }

    @Test
    void twoArgumentCallKeepsDefaultStep() {
        var query = (Query) parser.parse("SELECT * FROM GENERATE_SERIES(1, 10)");
        assertThat(query.from().get(0).tableFunctionRef().orElseThrow().stepArgument()).isEmpty();
    }
}
