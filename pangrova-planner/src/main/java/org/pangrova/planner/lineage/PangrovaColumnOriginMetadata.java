package org.pangrova.planner.lineage;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

import java.util.HashSet;
import java.util.Set;

/** Adds window origins and strict expression completeness to Calcite's metadata chain. */
public final class PangrovaColumnOriginMetadata implements MetadataHandler<BuiltInMetadata.ColumnOrigin> {
    private final java.util.Map<org.apache.calcite.rel.core.CorrelationId, RelNode> correlations;

    public PangrovaColumnOriginMetadata() {
        this(java.util.Map.of());
    }

    PangrovaColumnOriginMetadata(java.util.Map<org.apache.calcite.rel.core.CorrelationId, RelNode> correlations) {
        this.correlations = java.util.Map.copyOf(correlations);
    }

    @Override
    public MetadataDef<BuiltInMetadata.ColumnOrigin> getDef() {
        return BuiltInMetadata.ColumnOrigin.DEF;
    }

    /** Returns the provider installed in the existing planning cluster. */
    public RelMetadataProvider asProvider() {
        return ReflectiveRelMetadataProvider.reflectiveSource(this, BuiltInMetadata.ColumnOrigin.Handler.class);
    }

    /** Resolves all referenced inputs; unknown inputs cannot disappear from an expression. */
    public Set<RelColumnOrigin> getColumnOrigins(Project project, RelMetadataQuery query, int output) {
        RexNode expression = project.getProjects().get(output);
        var sources = expressionOrigins(expression, project.getInput(), query, 0);
        return expression instanceof RexInputRef ? sources : derived(sources);
    }

    /** Expands Calc's actual local references before resolving dependencies. */
    public Set<RelColumnOrigin> getColumnOrigins(Calc calc, RelMetadataQuery query, int output) {
        RexNode expression = calc.getProgram().expandLocalRef(calc.getProgram().getProjectList().get(output));
        var sources = expressionOrigins(expression, calc.getInput(), query, 0);
        return expression instanceof RexInputRef ? sources : derived(sources);
    }

    /** Includes actual operands, partition keys, order keys and frame-bound dependencies. */
    public Set<RelColumnOrigin> getColumnOrigins(Window window, RelMetadataQuery query, int output) {
        int inputs = window.getInput().getRowType().getFieldCount();
        if (output < inputs) {
            return requireOrigins(window.getInput(), query, output);
        }
        int offset = output - inputs;
        for (var group : window.groups) {
            if (offset < group.aggCalls.size()) {
                Set<RelColumnOrigin> sources = new HashSet<>();
                for (var operand : group.aggCalls.get(offset).getOperands()) {
                    sources.addAll(expressionOrigins(operand, window.getInput(), query, window.constants.size()));
                }
                for (int key : group.keys) {
                    sources.addAll(requireOrigins(window.getInput(), query, key));
                }
                for (var ordering : group.orderKeys.getFieldCollations()) {
                    sources.addAll(requireOrigins(window.getInput(), query, ordering.getFieldIndex()));
                }
                for (var bound : java.util.List.of(group.lowerBound, group.upperBound)) {
                    if (bound.getOffset() != null) {
                        sources.addAll(expressionOrigins(bound.getOffset(), window.getInput(), query,
                                window.constants.size()));
                    }
                }
                return derived(sources);
            }
            offset -= group.aggCalls.size();
        }
        throw new IllegalArgumentException("Window output is outside the actual aggregate fields: " + output);
    }

    /** Retains the original input origins across Pangrova's distribution-only exchange. */
    public Set<RelColumnOrigin> getColumnOrigins(
            org.pangrova.planner.cost.distribution.PangrovaExchange exchange,
            RelMetadataQuery query, int output) {
        return requireOrigins(exchange.getInput(), query, output);
    }

    /** Resolves the actual right outputs in the same query correlation scope. */
    public Set<RelColumnOrigin> getColumnOrigins(org.apache.calcite.rel.core.Correlate correlate,
                                                RelMetadataQuery query, int output) {
        int left = correlate.getLeft().getRowType().getFieldCount();
        if (output < left) {
            return requireOrigins(correlate.getLeft(), query, output);
        }
        var origins = requireOrigins(correlate.getRight(), query, output - left);
        return correlate.getJoinType().generatesNullsOnRight() ? derived(origins) : origins;
    }

    /** Assigns ARRAY/MAP output and ordinality origins from the declared collection layout. */
    public Set<RelColumnOrigin> getColumnOrigins(org.apache.calcite.rel.core.Uncollect uncollect,
                                                RelMetadataQuery query, int output) {
        Set<RelColumnOrigin> origins = new HashSet<>();
        for (int input : UncollectColumnLayout.inputs(uncollect, output)) {
            origins.addAll(requireOrigins(uncollect.getInput(), query, input));
        }
        return derived(origins);
    }

    private Set<RelColumnOrigin> expressionOrigins(
            RexNode expression, RelNode input, RelMetadataQuery query, int constants) {
        if (expression instanceof RexInputRef reference) {
            int count = input.getRowType().getFieldCount();
            if (reference.getIndex() >= count && reference.getIndex() < (long) count + constants) {
                return Set.of();
            }
            return requireOrigins(input, query, reference.getIndex());
        }
        if (expression instanceof RexLiteral || expression instanceof RexDynamicParam) {
            return Set.of();
        }
        if (expression instanceof RexFieldAccess access
                && access.getReferenceExpr() instanceof org.apache.calcite.rex.RexCorrelVariable variable) {
            var owner = QueryColumnOrigins.requireCorrelationOwner(correlations, variable, access);
            return requireOrigins(owner, query, access.getField().getIndex());
        }
        if (expression instanceof RexFieldAccess access) {
            return expressionOrigins(access.getReferenceExpr(), input, query, constants);
        }
        if (expression instanceof RexCall call && !(expression instanceof RexSubQuery)) {
            Set<RelColumnOrigin> sources = new HashSet<>();
            for (var operand : call.getOperands()) {
                sources.addAll(expressionOrigins(operand, input, query, constants));
            }
            return sources;
        }
        throw new IllegalArgumentException("Column lineage has an unresolved expression: " + expression.getKind());
    }

    static Set<RelColumnOrigin> requireOrigins(RelNode input, RelMetadataQuery query, int field) {
        if (field < 0 || field >= input.getRowType().getFieldCount()) {
            throw new IllegalArgumentException("Column lineage input index is outside its actual row type: " + field);
        }
        var origins = query.getColumnOrigins(input, field);
        if (origins == null) {
            throw new IllegalArgumentException("Calcite column origins are unknown for " + input.getRelTypeName()
                    + " field " + field);
        }
        return origins;
    }

    private static Set<RelColumnOrigin> derived(Set<RelColumnOrigin> sources) {
        Set<RelColumnOrigin> result = new HashSet<>();
        for (var origin : sources) {
            result.add(new RelColumnOrigin(origin.getOriginTable(), origin.getOriginColumnOrdinal(), true));
        }
        return result;
    }
}
