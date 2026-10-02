package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.ExtensionSingle;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Lowers Calcite's Cartesian Uncollect to sequential, typed Velox Unnest nodes. */
final class UnnestConverter {
    private final TypeMapper types;
    private final FunctionMapper functions;
    private final ExpressionConverter expressions;
    private final Function<RelNode, Rel> visitor;
    private int nextUnnestId;

    UnnestConverter(TypeMapper types, FunctionMapper functions,
                    ExpressionConverter expressions, Function<RelNode, Rel> visitor) {
        this.types = types;
        this.functions = functions;
        this.expressions = expressions;
        this.visitor = visitor;
    }

    Rel convert(LogicalCorrelate correlate, Uncollect uncollect, LogicalProject rightProject) {
        boolean outer = correlate.getJoinType() == JoinRelType.LEFT;
        if (!outer && correlate.getJoinType() != JoinRelType.INNER) {
            throw new IllegalArgumentException("UNNEST correlate requires INNER or LEFT join");
        }
        var state = new State(visitor.apply(correlate.getLeft()));
        int leftCount = state.size();
        List<RexNode> arguments = arguments(uncollect);
        List<Integer> collections = appendCollections(state, arguments, outer);
        if (outer) collections = guardEmptyProduct(state, collections);
        var expanded = new ArrayList<Expression>();
        var ordinals = new ArrayList<Integer>();
        String planBase = "unnest_" + nextUnnestId++;
        for (int i = 0; i < collections.size(); i++) {
            expand(state, planBase, uncollect, arguments.get(i).getType(),
                    collections.get(i), i, outer, expanded, ordinals);
        }
        if (uncollect.withOrdinality) {
            expanded.add(productOrdinal(state, collections, ordinals, outer));
        }
        boolean projectOuter = outer && rightProject != null;
        Rel result = projectDeclaredOutput(state, leftCount, uncollect, expanded, outer, projectOuter);
        return rightProject == null ? result
                : projectRight(result, leftCount, rightProject, correlate);
    }

    Rel convert(Uncollect uncollect) {
        var state = new State(visitor.apply(uncollect.getInput()));
        var helpers = new ArrayList<Expression>();
        for (int i = 0; i < state.size(); i++) {
            Type type = state.types.get(i);
            if (!(type instanceof Type.ListType) && !(type instanceof Type.Map)) {
                throw new IllegalArgumentException("UNNEST argument must be ARRAY or MAP");
            }
            helpers.add(cast(state.ref(i), type));
        }
        if (helpers.isEmpty()) throw new IllegalArgumentException("UNNEST requires a collection");
        List<Integer> collections = state.append(helpers);
        var expanded = new ArrayList<Expression>();
        var ordinals = new ArrayList<Integer>();
        String planBase = "unnest_" + nextUnnestId++;
        for (int i = 0; i < collections.size(); i++) {
            expand(state, planBase, uncollect,
                    uncollect.getInput().getRowType().getFieldList().get(i).getType(),
                    collections.get(i), i, false, expanded, ordinals);
        }
        if (uncollect.withOrdinality) expanded.add(productOrdinal(state, collections, ordinals, false));
        return projectDeclaredOutput(state, 0, uncollect, expanded, false, false);
    }

    private static List<RexNode> arguments(Uncollect uncollect) {
        if (!(uncollect.getInput() instanceof LogicalProject project)
                || !(project.getInput() instanceof LogicalValues values)
                || values.getTuples().size() != 1) {
            throw new IllegalArgumentException("Correlated UNNEST requires its canonical singleton argument project");
        }
        if (project.getProjects().isEmpty()) {
            throw new IllegalArgumentException("UNNEST requires at least one collection");
        }
        RexShuttle singleton = new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef inputRef) {
                return values.getTuples().get(0).get(inputRef.getIndex());
            }
        };
        return project.getProjects().stream().map(argument -> argument.accept(singleton)).toList();
    }

    private List<Integer> appendCollections(State state, List<RexNode> arguments, boolean outer) {
        var values = new ArrayList<Expression>();
        for (RexNode argument : arguments) {
            Type type = types.map(argument.getType());
            if (!(type instanceof Type.ListType) && !(type instanceof Type.Map)) {
                throw new IllegalArgumentException("UNNEST argument must be ARRAY or MAP");
            }
            // The explicit cast makes helper type/nullability and its anonymous
            // nested field names authoritative, independent of source SQL aliases.
            values.add(cast(expressions.convert(argument), type.withNullable(outer || type.nullable())));
        }
        return state.append(values);
    }

    private List<Integer> guardEmptyProduct(State state, List<Integer> collections) {
        var predicates = new ArrayList<Expression>();
        for (int collection : collections) {
            Expression length = cardinality(state.ref(collection));
            predicates.add(call("comparison", "gt", TypeCreator.NULLABLE.BOOLEAN,
                    length, ExpressionCreator.i64(false, 0)));
        }
        Expression alive = predicates.size() == 1 ? predicates.get(0)
                : call("boolean", "and", TypeCreator.NULLABLE.BOOLEAN, predicates);
        int aliveIndex = state.append(List.of(alive)).get(0);
        state.productAlive = aliveIndex;
        var guarded = new ArrayList<Expression>();
        for (int collection : collections) {
            Expression value = state.ref(collection);
            guarded.add(ImmutableExpression.IfThen.builder()
                    .addIfClauses(ImmutableExpression.IfClause.builder()
                            .condition(state.ref(aliveIndex)).then(value).build())
                    .elseClause(ExpressionCreator.typedNull(value.getType())).build());
        }
        // NULL or empty in any argument nulls every helper before expansion.
        // Sequential outer Unnest then emits exactly one all-NULL right row.
        return state.append(guarded);
    }

    private void expand(State state, String planBase, Uncollect uncollect,
            RelDataType argumentType, int collection, int argumentIndex, boolean outer,
            List<Expression> expanded, List<Integer> ordinals) {
        Type collectionType = state.types.get(collection);
        List<Type> produced = collectionType instanceof Type.ListType list
                ? List.of(list.elementType()) : List.of(((Type.Map) collectionType).key(),
                        ((Type.Map) collectionType).value());
        int first = state.size();
        var additions = new ArrayList<>(produced);
        if (uncollect.withOrdinality) additions.add(TypeCreator.of(outer).I64);
        state.addFields(additions, "unnest_" + argumentIndex + "_");
        var schema = NamedStruct.of(state.schema.getNamesList(), TypeCreator.REQUIRED.struct(state.types));
        var detail = new UnnestRelDetail(planBase + "_" + argumentIndex,
                collection, uncollect.withOrdinality, outer, schema);
        state.rel = ExtensionSingle.from(detail, state.rel).build();
        appendExpanded(state, uncollect, argumentType, first, produced.size(), expanded);
        if (uncollect.withOrdinality) ordinals.add(first + produced.size());
    }

    private static void appendExpanded(State state, Uncollect uncollect, RelDataType argumentType,
            int first, int count, List<Expression> expanded) {
        RelDataType component = argumentType.getComponentType();
        if (argumentType.getSqlTypeName() == SqlTypeName.ARRAY && component.isStruct()
                && uncollect.getItemAliases().isEmpty()) {
            var row = (FieldReference) state.ref(first);
            for (int i = 0; i < component.getFieldCount(); i++) expanded.add(row.dereferenceStruct(i));
        } else {
            for (int i = 0; i < count; i++) expanded.add(state.ref(first + i));
        }
    }

    private Expression productOrdinal(State state, List<Integer> collections,
            List<Integer> ordinals, boolean outer) {
        Type number = TypeCreator.of(outer).I64;
        Expression ordinal = state.ref(ordinals.get(0));
        for (int i = 1; i < ordinals.size(); i++) {
            Expression preceding = call("arithmetic", "subtract", number,
                    ordinal, ExpressionCreator.i64(false, 1));
            Expression offset = call("arithmetic", "multiply", number,
                    preceding, cardinality(state.ref(collections.get(i))));
            ordinal = call("arithmetic", "add", number, offset, state.ref(ordinals.get(i)));
        }
        return ordinal;
    }

    private Rel projectDeclaredOutput(State state, int leftCount, Uncollect uncollect,
            List<Expression> expanded, boolean outer, boolean preserveAlive) {
        if (expanded.size() != uncollect.getRowType().getFieldCount()) {
            throw new IllegalArgumentException("UNNEST expansion disagrees with Calcite declared output");
        }
        var projection = new ArrayList<Expression>();
        for (int i = 0; i < leftCount; i++) projection.add(state.ref(i));
        for (int i = 0; i < expanded.size(); i++) {
            // In particular, Velox ordinality is BIGINT and Calcite declares INTEGER.
            Type declared = types.map(uncollect.getRowType().getFieldList().get(i).getType());
            declared = declared.withNullable(outer || declared.nullable());
            projection.add(cast(expanded.get(i), declared));
        }
        if (preserveAlive) projection.add(state.ref(state.productAlive));
        return replaceOutput(state.rel, projection);
    }

    private Rel projectRight(Rel input, int leftCount, LogicalProject project,
            LogicalCorrelate correlate) {
        var projection = new ArrayList<Expression>();
        for (int i = 0; i < leftCount; i++) {
            projection.add(FieldReference.newRootStructReference(i, input.getRecordType().fields().get(i)));
        }
        RexShuttle shift = new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef inputRef) {
                return new RexInputRef(Math.addExact(leftCount, inputRef.getIndex()), inputRef.getType());
            }
        };
        for (int i = 0; i < project.getProjects().size(); i++) {
            Type declared = types.map(correlate.getRowType().getFieldList().get(leftCount + i).getType());
            Expression value = cast(expressions.convert(project.getProjects().get(i).accept(shift)), declared);
            if (correlate.getJoinType() == JoinRelType.LEFT) {
                int alive = input.getRecordType().fields().size() - 1;
                // LEFT applies after the complete right relation. In particular,
                // a constant right projection must become NULL for an empty product.
                value = ImmutableExpression.IfThen.builder()
                        .addIfClauses(ImmutableExpression.IfClause.builder()
                                .condition(FieldReference.newRootStructReference(alive,
                                        input.getRecordType().fields().get(alive))).then(value).build())
                        .elseClause(ExpressionCreator.typedNull(declared)).build();
            }
            projection.add(value);
        }
        return replaceOutput(input, projection);
    }

    private Expression cardinality(Expression collection) {
        return call("collection", "cardinality", TypeCreator.of(collection.getType().nullable()).I64,
                collection);
    }

    private Expression call(String namespace, String name, Type output, Expression... arguments) {
        return call(namespace, name, output, List.of(arguments));
    }

    private Expression call(String namespace, String name, Type output, List<Expression> arguments) {
        return ImmutableExpression.ScalarFunctionInvocation.builder()
                .declaration(functions.resolveScalarVariant(new FunctionReference(namespace, name)))
                .outputType(output).arguments(arguments).build();
    }

    private static Expression cast(Expression expression, Type target) {
        return ImmutableExpression.Cast.builder().input(expression).type(target)
                .failureBehavior(Expression.FailureBehavior.THROW_EXCEPTION).build();
    }

    private static Rel replaceOutput(Rel input, List<Expression> projection) {
        return ImmutableProject.builder().input(input).expressions(projection)
                .remap(Rel.Remap.offset(input.getRecordType().fields().size(), projection.size())).build();
    }

    private static final class State {
        private final RelProtoConverter converter = new RelProtoConverter(new ExtensionCollector());
        private final List<Type> types;
        private io.substrait.proto.NamedStruct schema;
        private Rel rel;
        private int productAlive = -1;

        State(Rel input) {
            rel = input;
            types = new ArrayList<>(input.getRecordType().fields());
            // Use the one protocol layout authority, never SQL labels as internal names.
            schema = SubstraitRelationLayout.describe(converter.toProto(input)).outputSchema();
        }

        int size() { return types.size(); }

        Expression ref(int ordinal) {
            return FieldReference.newRootStructReference(ordinal, types.get(ordinal));
        }

        List<Integer> append(List<Expression> values) {
            int start = size();
            rel = ImmutableProject.builder().input(rel).expressions(values).build();
            addFields(values.stream().map(Expression::getType).toList(), "proj_");
            var ordinals = new ArrayList<Integer>();
            for (int i = 0; i < values.size(); i++) ordinals.add(start + i);
            return List.copyOf(ordinals);
        }

        void addFields(List<Type> additions, String prefix) {
            var names = new ArrayList<String>();
            for (int i = 0; i < additions.size(); i++) names.add(prefix + i);
            var declared = additions.stream().map(converter.getTypeProtoConverter()::toProto).toList();
            schema = SubstraitRelationLayout.appendComputedFields(schema, names, declared);
            types.addAll(additions);
        }
    }
}
