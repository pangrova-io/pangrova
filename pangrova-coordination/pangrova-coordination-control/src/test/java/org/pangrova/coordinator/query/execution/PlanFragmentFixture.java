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
package org.pangrova.coordinator.query.execution;

import org.pangrova.planner.analysis.PlanFragment;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;

/**
 * Shared helpers for tests that need a minimal {@link PlanFragment}
 * placeholder (e.g. when constructing {@code StageNode} graphs whose
 * fragment payload is irrelevant to the scenario being verified).
 *
 * <p>The fragment wraps a trivial Calcite {@code LogicalValues.createOneRow}
 * RelNode and reports {@code -1} for both estimated shuffle bytes and rows,
 * matching the convention {@link PlanFragment} uses for "unavailable".</p>
 */
public final class PlanFragmentFixture {

    private PlanFragmentFixture() {
        // utility
    }

    /**
     * Returns a fresh {@link PlanFragment} backed by a single-row
     * {@code LogicalValues} RelNode. Safe to call repeatedly; each call
     * builds an independent cluster + RelNode, so callers may construct
     * multiple stages without aliasing.
     */
    public static PlanFragment dummyFragment() {
        RelOptCluster cluster = RelOptCluster.create(
                new VolcanoPlanner(),
                new RexBuilder(new JavaTypeFactoryImpl()));
        return new PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
