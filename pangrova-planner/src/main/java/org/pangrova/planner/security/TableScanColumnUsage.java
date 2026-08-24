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
package org.pangrova.planner.security;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per-TableScan column-usage analyzer.
 *
 * <p>Walks a Calcite {@link RelNode} plan top-down and, for every
 * {@link TableScan} encountered, computes the set of column names of the underlying
 * table that are actually referenced by the surrounding plan — taking projection,
 * filter conditions, join conditions, group keys and aggregate arguments,
 * sort keys, and DML write expressions into account.
 *
 * <p>Thread-safe: stateless beyond stack/local maps; instances may be reused
 * across queries without synchronization.
 */
public final class TableScanColumnUsage {

    private TableScanColumnUsage() {}

    /**
     * Collects, for every {@link TableScan} in {@code root}, the set of column
     * names of that scan that are read by the plan.
     */
    public static Map<TableScan, Set<String>> collect(RelNode root) {
        Map<TableScan, BitSet> ordinalsByScan = new IdentityHashMap<>();
        BitSet rootLive = new BitSet();
        rootLive.set(0, root.getRowType().getFieldCount());
        propagate(root, rootLive, ordinalsByScan);

        Map<TableScan, Set<String>> result = new IdentityHashMap<>();
        for (Map.Entry<TableScan, BitSet> e : ordinalsByScan.entrySet()) {
            List<String> fieldNames = e.getKey().getRowType().getFieldNames();
            Set<String> names = new LinkedHashSet<>();
            BitSet bits = e.getValue();
            for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
                if (i < fieldNames.size()) {
                    names.add(fieldNames.get(i));
                }
            }
            result.put(e.getKey(), Collections.unmodifiableSet(names));
        }
        return result;
    }

    private static void propagate(RelNode node, BitSet live,
                                  Map<TableScan, BitSet> out) {
        if (node instanceof TableScan ts) {
            out.computeIfAbsent(ts, k -> new BitSet()).or(live);
        } else if (node instanceof Project project) {
            propagateProject(project, live, out);
        } else if (node instanceof Filter filter) {
            propagateFilter(filter, live, out);
        } else if (node instanceof Join join) {
            propagateJoin(join, live, out);
        } else if (node instanceof Aggregate aggregate) {
            propagateAggregate(aggregate, out);
        } else if (node instanceof Sort sort) {
            propagateSort(sort, live, out);
        } else if (node instanceof TableModify modify) {
            propagateAllLive(modify.getInput(), out);
        } else {
            for (RelNode input : node.getInputs()) {
                propagateAllLive(input, out);
            }
        }
    }

    private static void propagateProject(Project project, BitSet live,
                                         Map<TableScan, BitSet> out) {
        BitSet inputLive = new BitSet();
        List<RexNode> projects = project.getProjects();
        for (int i = 0; i < projects.size(); i++) {
            if (live.get(i)) {
                addInputRefs(projects.get(i), inputLive);
            }
        }
        propagate(project.getInput(), inputLive, out);
    }

    private static void propagateFilter(Filter filter, BitSet live,
                                        Map<TableScan, BitSet> out) {
        BitSet inputLive = (BitSet) live.clone();
        addInputRefs(filter.getCondition(), inputLive);
        propagate(filter.getInput(), inputLive, out);
    }

    private static void propagateJoin(Join join, BitSet live,
                                      Map<TableScan, BitSet> out) {
        int leftCount = join.getLeft().getRowType().getFieldCount();
        int totalInputs = leftCount + join.getRight().getRowType().getFieldCount();

        BitSet condRefs = new BitSet();
        addInputRefs(join.getCondition(), condRefs);

        BitSet leftLive = new BitSet();
        BitSet rightLive = new BitSet();
        splitOrdinals(live, leftCount, totalInputs, leftLive, rightLive);
        splitOrdinals(condRefs, leftCount, totalInputs, leftLive, rightLive);

        propagate(join.getLeft(), leftLive, out);
        propagate(join.getRight(), rightLive, out);
    }

    private static void splitOrdinals(BitSet source, int leftCount, int totalInputs,
                                      BitSet leftLive, BitSet rightLive) {
        for (int i = source.nextSetBit(0); i >= 0; i = source.nextSetBit(i + 1)) {
            if (i < leftCount) {
                leftLive.set(i);
            } else if (i < totalInputs) {
                rightLive.set(i - leftCount);
            }
        }
    }

    private static void propagateAggregate(Aggregate aggregate,
                                           Map<TableScan, BitSet> out) {
        BitSet inputLive = new BitSet();
        for (int g : aggregate.getGroupSet()) {
            inputLive.set(g);
        }
        for (ImmutableBitSet gs : aggregate.getGroupSets()) {
            for (int g : gs) {
                inputLive.set(g);
            }
        }
        for (AggregateCall call : aggregate.getAggCallList()) {
            for (int arg : call.getArgList()) {
                inputLive.set(arg);
            }
            if (call.filterArg >= 0) {
                inputLive.set(call.filterArg);
            }
        }
        propagate(aggregate.getInput(), inputLive, out);
    }

    private static void propagateSort(Sort sort, BitSet live,
                                      Map<TableScan, BitSet> out) {
        BitSet inputLive = (BitSet) live.clone();
        sort.getCollation().getFieldCollations()
                .forEach(fc -> inputLive.set(fc.getFieldIndex()));
        addInputRefs(sort.fetch, inputLive);
        addInputRefs(sort.offset, inputLive);
        propagate(sort.getInput(), inputLive, out);
    }

    private static void propagateAllLive(RelNode input, Map<TableScan, BitSet> out) {
        BitSet allLive = new BitSet();
        allLive.set(0, input.getRowType().getFieldCount());
        propagate(input, allLive, out);
    }

    private static void addInputRefs(RexNode expr, BitSet into) {
        if (expr == null) {
            return;
        }
        expr.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                into.set(inputRef.getIndex());
                return inputRef;
            }
        });
    }
}
