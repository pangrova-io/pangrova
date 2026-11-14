package org.pangrova.security.api.rowfilter;

import org.pangrova.security.api.rowfilter.ParameterizedFilter;
import org.pangrova.security.api.rowfilter.ParameterizedFilter.BoundParameter;
import org.pangrova.security.api.enums.FilterParameterType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ParameterizedFilter}, including regression test for F-SEC-06:
 * placeholder replacement must not corrupt bindings when parameters >= 10
 * (e.g., $1 must not match inside $10 or $11).
 */
class ParameterizedFilterTest {

    @Test
    void singleParameterReplacement() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(owner = $1)",
                List.of(new BoundParameter("current_user", "alice", FilterParameterType.STRING)));

        assertThat(filter.toSafeExpression()).isEqualTo("(owner = 'alice')");
    }

    @Test
    void multipleParametersReplacedCorrectly() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(owner = $1 AND dept = $2)",
                List.of(
                        new BoundParameter("current_user", "alice", FilterParameterType.STRING),
                        new BoundParameter("department", "engineering", FilterParameterType.STRING)));

        assertThat(filter.toSafeExpression())
                .isEqualTo("(owner = 'alice' AND dept = 'engineering')");
    }

    /**
     * Regression test for F-SEC-06: with 10+ parameters, $1 must NOT match
     * the prefix of $10, $11, etc. Each placeholder must be replaced independently
     * based on its full numeric index.
     */
    @Test
    void tenOrMoreParametersDoNotCorruptBindings() {
        // Build a template with 12 parameters: $1 through $12
        String template = "col1=$1 AND col2=$2 AND col3=$3 AND col4=$4 AND col5=$5 "
                + "AND col6=$6 AND col7=$7 AND col8=$8 AND col9=$9 AND col10=$10 "
                + "AND col11=$11 AND col12=$12";

        List<BoundParameter> params = new ArrayList<>();
        for (int i = 1; i <= 12; i++) {
            params.add(new BoundParameter("p" + i, "value" + i, FilterParameterType.STRING));
        }

        ParameterizedFilter filter = new ParameterizedFilter(template, params);
        String result = filter.toSafeExpression();

        // Verify each parameter is correctly substituted
        assertThat(result).isEqualTo(
                "col1='value1' AND col2='value2' AND col3='value3' AND col4='value4' AND col5='value5' "
                + "AND col6='value6' AND col7='value7' AND col8='value8' AND col9='value9' AND col10='value10' "
                + "AND col11='value11' AND col12='value12'");

        // Explicitly verify $10 was NOT corrupted by $1 replacement
        assertThat(result).contains("col10='value10'");
        assertThat(result).doesNotContain("'value1'0");
        assertThat(result).contains("col11='value11'");
        assertThat(result).doesNotContain("'value1'1");
    }

    /**
     * Verifies that adjacent placeholders like $1$10 are correctly parsed as
     * two separate placeholders (param 1 and param 10), not as one corrupted result.
     */
    @Test
    void adjacentPlaceholdersWithOverlappingPrefixes() {
        String template = "$1$10";
        List<BoundParameter> params = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            params.add(new BoundParameter("p" + i, "v" + i, FilterParameterType.STRING));
        }

        ParameterizedFilter filter = new ParameterizedFilter(template, params);
        String result = filter.toSafeExpression();

        assertThat(result).isEqualTo("'v1''v10'");
    }

    @Test
    void duplicatePlaceholderReferencesResolveCorrectly() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(a = $1 OR b = $1)",
                List.of(new BoundParameter("user", "bob", FilterParameterType.STRING)));

        assertThat(filter.toSafeExpression()).isEqualTo("(a = 'bob' OR b = 'bob')");
    }

    @Test
    void outOfRangeParameterIndexThrows() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(a = $1 AND b = $3)",
                List.of(
                        new BoundParameter("p1", "v1", FilterParameterType.STRING),
                        new BoundParameter("p2", "v2", FilterParameterType.STRING)));

        assertThatThrownBy(filter::toSafeExpression)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("$3")
                .hasMessageContaining("2 parameters are bound");
    }

    @Test
    void zeroIndexParameterThrows() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(a = $0)",
                List.of(new BoundParameter("p1", "v1", FilterParameterType.STRING)));

        assertThatThrownBy(filter::toSafeExpression)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("$0");
    }

    @Test
    void templateWithNoPlaceholdersReturnsVerbatim() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(status = 'active')",
                List.of());

        assertThat(filter.toSafeExpression()).isEqualTo("(status = 'active')");
    }

    @Test
    void stringListParameterTypeRenderedCorrectly() {
        ParameterizedFilter filter = new ParameterizedFilter(
                "(role IN ($1))",
                List.of(new BoundParameter("groups", "admin,user,viewer",
                        FilterParameterType.STRING_LIST)));

        assertThat(filter.toSafeExpression()).isEqualTo("(role IN ('admin,user,viewer'))");
    }
}
