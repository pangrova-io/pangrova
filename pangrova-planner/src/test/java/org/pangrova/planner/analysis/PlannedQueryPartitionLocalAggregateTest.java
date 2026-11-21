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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

/** Eligibility of a plan for partition-at-a-time aggregate recomputation. */
class PlannedQueryPartitionLocalAggregateTest {

    @Test
    void groupingAggregateOverASingleScanIsPartitionLocal() {
        var builder = builder();
        RelNode plan = builder.scan("orders")
                .aggregate(builder.groupKey(builder.field("id")),
                        builder.countStar("cnt"))
                .build();

        assertThat(query(plan).isPartitionLocalAggregate()).isTrue();
    }

    @Test
    void projectionOverAGroupingAggregateIsPartitionLocal() {
        var builder = builder();
        RelNode plan = builder.scan("orders")
                .aggregate(builder.groupKey(builder.field("id")),
                        builder.countStar("cnt"))
                .project(builder.field("id"), builder.field("cnt"))
                .build();

        assertThat(query(plan).isPartitionLocalAggregate()).isTrue();
    }

    @Test
    void globalAggregatesAreNotPartitionLocal() {
        var builder = builder();
        RelNode plan = builder.scan("orders")
                .aggregate(builder.groupKey(), builder.countStar("cnt"))
                .build();

        assertThat(query(plan).isPartitionLocalAggregate()).isFalse();
    }

    @Test
    void aggregatesOverJoinsAreNotPartitionLocal() {
        var builder = builder();
        RelNode plan = builder.scan("orders").scan("customers")
                .join(org.apache.calcite.rel.core.JoinRelType.INNER, "id")
                .aggregate(builder.groupKey(builder.field("id")), builder.countStar("cnt"))
                .build();

        assertThat(query(plan).isPartitionLocalAggregate()).isFalse();
    }

    @Test
    void sortingOrLimitingAnAggregateIsNotPartitionLocal() {
        var sorted = builder();
        RelNode sortPlan = sorted.scan("orders")
                .aggregate(sorted.groupKey(sorted.field("id")), sorted.countStar("cnt"))
                .sort(sorted.field("id"))
                .build();
        var limited = builder();
        RelNode limitPlan = limited.scan("orders")
                .aggregate(limited.groupKey(limited.field("id")), limited.countStar("cnt"))
                .limit(0, 10)
                .build();

        assertThat(query(sortPlan).isPartitionLocalAggregate()).isFalse();
        assertThat(query(limitPlan).isPartitionLocalAggregate()).isFalse();
    }

    @Test
    void scansAreNotAggregates() {
        assertThat(query(builder().scan("orders").build()).isPartitionLocalAggregate()).isFalse();
    }

    private static PlannedQuery query(RelNode node) {
        return new PlannedQuery(node, List.of(), List.of(), java.util.Map.of());
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
