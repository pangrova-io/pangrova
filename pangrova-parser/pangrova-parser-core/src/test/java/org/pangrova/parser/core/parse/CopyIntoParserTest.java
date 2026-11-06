package org.pangrova.parser.core.parse;


import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopyIntoParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void subqueryUsesTheInsertSelectWritePath() {
        InsertSelectStatement statement = (InsertSelectStatement) parser.parse(
                "COPY INTO lake.analytics.target FROM "
                        + "(SELECT id, name FROM lake.analytics.source WHERE active = true)");
        assertThat(statement.tableName().parts())
                .containsExactly("lake", "analytics", "target");
        assertThat(statement.overwrite()).isFalse();
        assertThat(statement.columns()).isEmpty();
        assertThat(statement.selectQuery().queries()).hasSize(1);
    }

    @Test
    void uriSourceFailsClosed() {
        assertThatThrownBy(() -> parser.parse(
                "COPY INTO t FROM 's3://bucket/data.parquet' FILE_FORMAT = 'PARQUET'"))
                .isInstanceOfSatisfying(PangrovaException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE);
                    assertThat(error.getMessage()).contains("COPY INTO from a URI is unsupported");
                });
    }

    @Test
    void fileOptionsOnSubqueryFailClosed() {
        assertThatThrownBy(() -> parser.parse(
                "COPY INTO t FROM (SELECT id FROM src) PATTERN = '*.parquet'"))
                .isInstanceOfSatisfying(PangrovaException.class, error -> {
                    assertThat(error.getErrorCode())
                            .isEqualTo(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE);
                    assertThat(error.getMessage()).contains("does not support file-loading options");
                });
    }
}
