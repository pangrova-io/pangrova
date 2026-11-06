package org.pangrova.planner.core.parse;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.pangrova.parser.api.ast.LiteralValue;

/** Converts validated temporal literals without truncating their declared fractional precision. */
final class TemporalLiteralSqlNodes {
    private TemporalLiteralSqlNodes() {}

    static SqlNode date(LiteralValue.DateValue literal) {
        String[] parts = literal.value().split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        int day = Integer.parseInt(parts[2]);
        return SqlLiteral.createDate(new DateString(year, month, day), SqlParserPos.ZERO);
    }

    static SqlNode timestamp(LiteralValue.TimestampValue literal) {
        String value = literal.value();
        int decimalPoint = value.indexOf('.');
        int precision = decimalPoint < 0 ? 0 : value.length() - decimalPoint - 1;
        if (precision > 9) {
            throw new IllegalArgumentException("TIMESTAMP fractional precision exceeds 9");
        }
        return SqlLiteral.createTimestamp(new TimestampString(value), precision, SqlParserPos.ZERO);
    }
}
