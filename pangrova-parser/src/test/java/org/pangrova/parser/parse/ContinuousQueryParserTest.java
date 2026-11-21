package org.pangrova.parser.parse;

import org.pangrova.parser.api.ast.CreateContinuousQueryStatement;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.exception.ParseException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContinuousQueryParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void parsesScheduledContinuousQuery() {
        var statement = parser.parse(
                "CREATE CONTINUOUS QUERY cat.sch.cq REFRESH EVERY 'PT5M' AS SELECT * FROM source");

        assertThat(statement).isInstanceOf(CreateContinuousQueryStatement.class);
        var continuous = (CreateContinuousQueryStatement) statement;
        assertThat(continuous.name().parts()).containsExactly("cat", "sch", "cq");
        assertThat(continuous.refreshInterval()).isEqualTo(Duration.ofMinutes(5));
        assertThat(continuous.query()).isNotNull();
    }

    @Test
    void acceptsUnqualifiedName() {
        var continuous = (CreateContinuousQueryStatement) parser.parse(
                "CREATE CONTINUOUS QUERY cq REFRESH EVERY 'PT1H' AS SELECT 1");

        assertThat(continuous.name()).isEqualTo(Identifier.of("cq"));
    }

    @Test
    void rejectsZeroRefreshInterval() {
        assertThatThrownBy(() -> parser.parse(
                "CREATE CONTINUOUS QUERY cq REFRESH EVERY 'PT0S' AS SELECT 1"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void rejectsInvalidDuration() {
        assertThatThrownBy(() -> parser.parse(
                "CREATE CONTINUOUS QUERY cq REFRESH EVERY 'five minutes' AS SELECT 1"))
                .isInstanceOf(ParseException.class);
    }
}
