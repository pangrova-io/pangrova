package org.pangrova.planner.core.substrait;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.analysis.PangrovaPlanner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SubstraitFieldNamesTest {

    @Test
    void flattensNestedNamesDepthFirstWhilePreservingTopLevelAliases() {
        JavaTypeFactoryImpl factory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType details = factory.builder()
                .add("code", SqlTypeName.VARCHAR)
                .build();
        RelDataType payload = factory.builder()
                .add("id", SqlTypeName.BIGINT)
                .add("details", details)
                .build();
        RelDataType event = factory.builder()
                .add("timestamp", SqlTypeName.TIMESTAMP)
                .build();
        RelDataType rowType = factory.builder()
                .add("payload", payload)
                .add("events", factory.createArrayType(event, -1))
                .build();

        assertThat(SubstraitFieldNames.flatten(rowType, List.of("result", "history")))
                .containsExactly(
                        "result", "id", "details", "code",
                        "history", "timestamp");
    }

    @Test
    void converterEmitsEveryNestedRootNameRequiredBySubstrait() {
        var seed = new PangrovaPlanner().plan("SELECT 1").plan();
        var factory = seed.getCluster().getTypeFactory();
        RelDataType nested = factory.builder()
                .add("id", SqlTypeName.BIGINT)
                .add("details", factory.builder()
                        .add("code", SqlTypeName.VARCHAR)
                        .build())
                .build();
        RelDataType rowType = factory.builder()
                .add("payload", nested)
                .build();
        LogicalValues values = LogicalValues.createEmpty(seed.getCluster(), rowType);

        var plan = new SubstraitConverter().convert(values, List.of("result"));

        assertThat(plan.getRoots().get(0).getNames())
                .containsExactly("result", "id", "details", "code");
    }
}
