package org.pangrova.planner.parse;

import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pangrova.parser.ast.LiteralValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemporalLiteralSqlNodesTest {
    @ParameterizedTest
    @CsvSource({"2026-10-01 12:34:56,0", "2026-10-01 12:34:56.123,3",
            "2026-10-01 12:34:56.123456,6", "2026-10-01 12:34:56.123456789,9"})
    void preservesExactFractionalPrecision(String value, int precision) {
        var literal = (SqlTimestampLiteral) TemporalLiteralSqlNodes.timestamp(new LiteralValue.TimestampValue(value));
        assertThat(literal.getPrec()).isEqualTo(precision);
        assertThat(literal.getValueAs(TimestampString.class).toString()).isEqualTo(value);
    }

    @Test
    void rejectsPrecisionThatTheCanonicalTimestampCannotRepresent() {
        assertThatThrownBy(() -> TemporalLiteralSqlNodes.timestamp(
                new LiteralValue.TimestampValue("2026-10-01 12:34:56.1234567890")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exceeds 9");
    }

    @Test
    void bindsCalendarDateThroughTheActualSqlLiteral() {
        var literal = (SqlLiteral) TemporalLiteralSqlNodes.date(new LiteralValue.DateValue("2024-02-29"));
        assertThat(literal.getValueAs(DateString.class).toString()).isEqualTo("2024-02-29");
    }
}
