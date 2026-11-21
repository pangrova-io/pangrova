package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class SortDirectionMapperTest {

    @ParameterizedTest
    @CsvSource({"ASCENDING, FIRST, ASC_NULLS_FIRST", "ASCENDING, LAST, ASC_NULLS_LAST",
            "DESCENDING, FIRST, DESC_NULLS_FIRST", "DESCENDING, LAST, DESC_NULLS_LAST",
            "ASCENDING, UNSPECIFIED, ASC_NULLS_LAST", "DESCENDING, UNSPECIFIED, DESC_NULLS_FIRST"})
    void preservesExplicitNullOrderingAndUsesDirectionDefaultWhenUnspecified(
            RelFieldCollation.Direction direction, RelFieldCollation.NullDirection nullDirection,
            Expression.SortDirection expected) {
        assertThat(SortDirectionMapper.map(direction, nullDirection)).isEqualTo(expected);
    }
}
