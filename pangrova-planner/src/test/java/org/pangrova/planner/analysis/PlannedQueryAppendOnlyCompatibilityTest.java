/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner.analysis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

/** Eligibility of a plan for append-only incremental refresh. */
class PlannedQueryAppendOnlyCompatibilityTest {

    @Test
    void singleTableScanIsAppendOnlyCompatible() {
        var builder = builder();

        assertThat(query(builder.scan("orders").build()).isAppendOnlyScanCompatible())
                .isTrue();
    }

    @Test
    void projectionAndFilterOverASingleScanAreAppendOnlyCompatible() {
        var builder = builder();
        RelNode plan = builder.scan("orders")
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("id"),
                        builder.literal(10L)))
                .project(builder.field("id"), builder.field("name"))
                .build();

        assertThat(query(plan).isAppendOnlyScanCompatible()).isTrue();
    }

    @Test
    void joinsAggregatesAndUnionsAreNotAppendOnlyCompatible() {
        var join = builder().scan("orders").scan("customers")
                .join(org.apache.calcite.rel.core.JoinRelType.INNER, "id").build();
        var aggregateBuilder = builder();
        RelNode aggregate = aggregateBuilder.scan("orders")
                .aggregate(aggregateBuilder.groupKey(aggregateBuilder.field("id")),
                        aggregateBuilder.countStar("cnt"))
                .build();
        var union = builder().scan("orders").scan("customers").union(true).build();

        assertThat(query(join).isAppendOnlyScanCompatible()).isFalse();
        assertThat(query(aggregate).isAppendOnlyScanCompatible()).isFalse();
        assertThat(query(union).isAppendOnlyScanCompatible()).isFalse();
    }

    @Test
    void plansWithoutAScanAreNotAppendOnlyCompatible() {
        var builder = builder();

        assertThat(query(builder.values(new String[] {"x"}, 1).build())
                .isAppendOnlyScanCompatible()).isFalse();
    }

    private static PlannedQuery query(RelNode node) {
        return new PlannedQuery(node, List.of(), java.util.Map.of());
    }

    private static RelBuilder builder() {
        var schema = Frameworks.createRootSchema(true);
        schema.add("orders", table());
        schema.add("customers", table());
        return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema).build());
    }

    private static AbstractTable table() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder()
                        .add("id", SqlTypeName.BIGINT)
                        .add("name", SqlTypeName.VARCHAR)
                        .build();
            }
        };
    }
}
