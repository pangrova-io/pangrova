package org.pangrova.planner.lineage;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts column-level lineage from a Calcite RelNode plan tree.
 * Walks the plan bottom-up, tracking which output columns derive from which source columns.
 *
 * <p>Supports: TableScan, Project, Filter, Join, Aggregate, Union.
 * Unknown node types are treated as pass-through (all inputs contribute to all outputs).</p>
 */
public final class ColumnLineageExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnLineageExtractor.class);

    /**
     * Extracts column derivations from the given optimized plan.
     *
     * @param plan the optimized RelNode plan
     * @return list of column derivations for each output column
     */
    public List<ColumnDerivation> extract(RelNode plan) {
        List<List<ColumnDerivation.SourceColumn>> sources = resolveNode(plan);
        List<RelDataTypeField> outputFields = plan.getRowType().getFieldList();
        List<ColumnDerivation> derivations = new ArrayList<>(outputFields.size());

        for (int i = 0; i < outputFields.size(); i++) {
            String outputName = outputFields.get(i).getName();
            List<ColumnDerivation.SourceColumn> columnSources =
                    i < sources.size() ? sources.get(i) : List.of();
            ColumnDerivation.DerivationType type = inferDerivationType(plan, i);
            derivations.add(new ColumnDerivation(outputName, columnSources, type));
        }
        return derivations;
    }

    /**
     * Collects all source table names referenced in the plan.
     *
     * @param plan the RelNode plan
     * @return set of qualified table names
     */
    public Set<String> extractSourceTables(RelNode plan) {
        Set<String> tables = new LinkedHashSet<>();
        collectTables(plan, tables);
        return tables;
    }

    private void collectTables(RelNode node, Set<String> tables) {
        if (node instanceof TableScan scan) {
            tables.add(String.join(".", scan.getTable().getQualifiedName()));
        }
        for (RelNode input : node.getInputs()) {
            collectTables(input, tables);
        }
    }

    private List<List<ColumnDerivation.SourceColumn>> resolveNode(RelNode node) {
        if (node instanceof TableScan scan) {
            return resolveTableScan(scan);
        } else if (node instanceof Project project) {
            return resolveProject(project);
        } else if (node instanceof Filter filter) {
            return resolveNode(filter.getInput());
        } else if (node instanceof Join join) {
            return resolveJoin(join);
        } else if (node instanceof Aggregate aggregate) {
            return resolveAggregate(aggregate);
        } else if (node instanceof Union union) {
            return resolveUnion(union);
        } else if (!node.getInputs().isEmpty()) {
            return resolveNode(node.getInputs().get(0));
        } else {
            return emptySourcesForFields(node);
        }
    }

    private List<List<ColumnDerivation.SourceColumn>> resolveTableScan(TableScan scan) {
        String tableName = String.join(".", scan.getTable().getQualifiedName());
        List<RelDataTypeField> fields = scan.getRowType().getFieldList();
        List<List<ColumnDerivation.SourceColumn>> result = new ArrayList<>(fields.size());
        for (RelDataTypeField field : fields) {
            result.add(List.of(new ColumnDerivation.SourceColumn(tableName, field.getName())));
        }
        return result;
    }

    private List<List<ColumnDerivation.SourceColumn>> resolveProject(Project project) {
        List<List<ColumnDerivation.SourceColumn>> inputSources = resolveNode(project.getInput());
        List<List<ColumnDerivation.SourceColumn>> result = new ArrayList<>(project.getProjects().size());

        for (RexNode expr : project.getProjects()) {
            result.add(resolveExpression(expr, inputSources));
        }
        return result;
    }

    private List<List<ColumnDerivation.SourceColumn>> resolveJoin(Join join) {
        List<List<ColumnDerivation.SourceColumn>> leftSources = resolveNode(join.getLeft());
        List<List<ColumnDerivation.SourceColumn>> rightSources = resolveNode(join.getRight());
        List<List<ColumnDerivation.SourceColumn>> combined = new ArrayList<>(
                leftSources.size() + rightSources.size());
        combined.addAll(leftSources);
        combined.addAll(rightSources);
        return combined;
    }

    private List<List<ColumnDerivation.SourceColumn>> resolveAggregate(Aggregate aggregate) {
        List<List<ColumnDerivation.SourceColumn>> inputSources = resolveNode(aggregate.getInput());
        List<List<ColumnDerivation.SourceColumn>> result = new ArrayList<>();

        // Group-by keys pass through directly
        for (int groupKey : aggregate.getGroupSet()) {
            if (groupKey < inputSources.size()) {
                result.add(inputSources.get(groupKey));
            } else {
                result.add(List.of());
            }
        }

        // Aggregate calls reference their argument columns
        for (var aggCall : aggregate.getAggCallList()) {
            Set<ColumnDerivation.SourceColumn> aggSources = new HashSet<>();
            for (int argIndex : aggCall.getArgList()) {
                if (argIndex < inputSources.size()) {
                    aggSources.addAll(inputSources.get(argIndex));
                }
            }
            result.add(List.copyOf(aggSources));
        }
        return result;
    }

    private List<List<ColumnDerivation.SourceColumn>> resolveUnion(Union union) {
        // Union merges sources from all inputs for each position
        List<List<ColumnDerivation.SourceColumn>> merged = null;
        for (RelNode input : union.getInputs()) {
            List<List<ColumnDerivation.SourceColumn>> inputSources = resolveNode(input);
            if (merged == null) {
                merged = new ArrayList<>();
                for (var s : inputSources) {
                    merged.add(new ArrayList<>(s));
                }
            } else {
                for (int i = 0; i < Math.min(merged.size(), inputSources.size()); i++) {
                    Set<ColumnDerivation.SourceColumn> combined = new LinkedHashSet<>(merged.get(i));
                    combined.addAll(inputSources.get(i));
                    merged.set(i, List.copyOf(combined));
                }
            }
        }
        return merged != null ? merged : List.of();
    }

    private List<ColumnDerivation.SourceColumn> resolveExpression(
            RexNode expr, List<List<ColumnDerivation.SourceColumn>> inputSources) {
        if (expr instanceof RexInputRef ref) {
            int index = ref.getIndex();
            if (index < inputSources.size()) {
                return inputSources.get(index);
            }
            return List.of();
        } else if (expr instanceof RexCall call) {
            Set<ColumnDerivation.SourceColumn> sources = new LinkedHashSet<>();
            for (RexNode operand : call.getOperands()) {
                sources.addAll(resolveExpression(operand, inputSources));
            }
            return List.copyOf(sources);
        } else if (expr instanceof RexLiteral) {
            return List.of();
        } else {
            // Unknown expression type — conservatively collect all input refs
            Set<ColumnDerivation.SourceColumn> sources = new LinkedHashSet<>();
            collectAllInputRefs(expr, inputSources, sources);
            return List.copyOf(sources);
        }
    }

    private void collectAllInputRefs(RexNode expr,
                                     List<List<ColumnDerivation.SourceColumn>> inputSources,
                                     Set<ColumnDerivation.SourceColumn> result) {
        if (expr instanceof RexInputRef ref) {
            int index = ref.getIndex();
            if (index < inputSources.size()) {
                result.addAll(inputSources.get(index));
            }
        } else if (expr instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                collectAllInputRefs(operand, inputSources, result);
            }
        }
    }

    private ColumnDerivation.DerivationType inferDerivationType(RelNode plan, int columnIndex) {
        if (plan instanceof Aggregate aggregate) {
            int groupCount = aggregate.getGroupCount();
            return columnIndex < groupCount
                    ? ColumnDerivation.DerivationType.DIRECT
                    : ColumnDerivation.DerivationType.AGGREGATE;
        } else if (plan instanceof Project project) {
            RexNode expr = project.getProjects().get(columnIndex);
            if (expr instanceof RexInputRef) {
                return ColumnDerivation.DerivationType.DIRECT;
            }
            return ColumnDerivation.DerivationType.TRANSFORM;
        }
        return ColumnDerivation.DerivationType.DIRECT;
    }

    private List<List<ColumnDerivation.SourceColumn>> emptySourcesForFields(RelNode node) {
        int fieldCount = node.getRowType().getFieldCount();
        List<List<ColumnDerivation.SourceColumn>> result = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            result.add(List.of());
        }
        return result;
    }
}
