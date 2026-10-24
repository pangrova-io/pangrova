package org.pangrova.planner;

import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlannedQueryTimestampSchemaTest {

    private final SqlCompiler compiler = new SqlCompiler();

    @Test
    void ctasPreservesNanosecondOutputInsteadOfCreatingMicrosecondColumn() {
        assertThat(compiler.compileToPlannedQuery(
                "SELECT TIMESTAMP '2025-01-01 00:00:00.123456789' AS event_time")
                .queryOutputColumnTypeNames()).containsExactly("TIMESTAMP_NS");
        assertThat(compiler.compileToPlannedQuery(
                "SELECT TIMESTAMP '2025-01-01 00:00:00.123456' AS event_time")
                .queryOutputColumnTypeNames()).containsExactly("TIMESTAMP");
    }

    @Test
    void ctasRejectsTimezoneTypesUntilDestinationSchemaCanPreserveThem() {
        var cluster = compiler.compileToPlannedQuery("SELECT 1").plan().getCluster();
        for (SqlTypeName sqlType : List.of(SqlTypeName.TIMESTAMP_TZ,
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            for (int precision : List.of(6, 9)) {
                var rowType = cluster.getTypeFactory().builder()
                        .add("event_time", cluster.getTypeFactory().createSqlType(sqlType, precision)).build();
                var query = new PlannedQuery(LogicalValues.createEmpty(cluster, rowType),
                        List.of("event_time"), List.of(), java.util.Map.of());
                assertThatThrownBy(query::queryOutputColumnTypeNames)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("cannot preserve timezone timestamp precision");
            }
        }
    }
}
