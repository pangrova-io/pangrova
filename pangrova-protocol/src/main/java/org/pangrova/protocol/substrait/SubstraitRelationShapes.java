package org.pangrova.protocol.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.List;

/** Canonical operator output shapes before the operator's common emit. */
final class SubstraitRelationShapes {
    private SubstraitRelationShapes() {}

    static NamedStruct concatenate(List<NamedStruct> inputs) {
        List<SubstraitSchemaFields.Field> fields = new ArrayList<>();
        inputs.forEach(input -> fields.addAll(SubstraitSchemaFields.fields(input)));
        return SubstraitSchemaFields.schema(fields);
    }

    static NamedStruct direct(Rel relation, List<NamedStruct> inputs, NamedStruct context) {
        return switch (relation.getRelTypeCase()) {
            case READ -> read(relation);
            case PROJECT -> project(relation, unary(inputs));
            case FILTER -> filter(relation, unary(inputs));
            case AGGREGATE -> SubstraitAggregateShape.describe(relation.getAggregate(), unary(inputs));
            case WINDOW -> window(relation, unary(inputs));
            case JOIN, HASH_JOIN, MERGE_JOIN, NESTED_LOOP_JOIN ->
                    SubstraitJoinShape.describe(relation, inputs, context);
            case SET -> SubstraitSetShape.describe(relation.getSet(), inputs);
            default -> remaining(relation, inputs, context);
        };
    }

    private static NamedStruct remaining(Rel relation, List<NamedStruct> inputs, NamedStruct context) {
        return switch (relation.getRelTypeCase()) {
            case SORT -> sorted(relation.getSort().getSortsList(), unary(inputs));
            case FETCH -> unary(inputs);
            case EXCHANGE -> exchange(relation, unary(inputs));
            case CROSS -> binary(inputs, context);
            case EXTENSION_LEAF, EXTENSION_SINGLE, EXTENSION_MULTI -> SubstraitExtensionShape.describe(relation);
            case EXPAND -> expand(relation, unary(inputs));
            case TOP_N -> sorted(relation.getTopN().getSortsList(), unary(inputs));
            default -> structural(relation, inputs);
        };
    }

    private static NamedStruct structural(Rel relation, List<NamedStruct> inputs) {
        if (relation.hasWrite()) {
            NamedStruct input = unary(inputs);
            return switch (relation.getWrite().getOutput()) {
                case OUTPUT_MODE_NO_OUTPUT -> concatenate(List.of());
                case OUTPUT_MODE_MODIFIED_RECORDS -> input;
                default -> throw new IllegalArgumentException("Write requires its output mode");
            };
        }
        if (relation.hasDdl()) return concatenate(List.of());
        if (relation.hasUpdate()) return concatenate(List.of());
        throw new IllegalArgumentException("Relation requires a declared schema context: " + relation.getRelTypeCase());
    }

    private static NamedStruct read(Rel relation) {
        var read = relation.getRead();
        if (!read.hasBaseSchema()) throw new IllegalArgumentException("Read requires an explicit base schema");
        NamedStruct base = read.getBaseSchema();
        SubstraitSchemaFields.fields(base).forEach(field -> SubstraitExpressionTypes.declared(field.type()));
        if (read.hasFilter()) requireBoolean(read.getFilter(), base);
        if (read.hasBestEffortFilter()) requireBoolean(read.getBestEffortFilter(), base);
        return read.hasProjection() ? SubstraitMasks.project(base, read.getProjection()) : base;
    }

    private static NamedStruct project(Rel relation, NamedStruct input) {
        List<SubstraitSchemaFields.Field> fields = new ArrayList<>(SubstraitSchemaFields.fields(input));
        var expressions = relation.getProject().getExpressionsList();
        for (int i = 0; i < expressions.size(); i++) {
            fields.add(expressionField(expressions.get(i), input, "proj_" + i));
        }
        return SubstraitSchemaFields.schema(fields, input.getStruct());
    }

    static SubstraitSchemaFields.Field expressionField(Expression expression, NamedStruct input, String name) {
        if (expression.hasSelection()) {
            var reference = expression.getSelection();
            if (!reference.hasExpression() && !reference.hasOuterReference() && !reference.hasLambdaParameterReference()
                    && reference.hasDirectReference()
                    && reference.getDirectReference().hasStructField()
                    && !reference.getDirectReference().getStructField().hasChild()) {
                int index = reference.getDirectReference().getStructField().getField();
                var fields = SubstraitSchemaFields.fields(input);
                if (index < 0 || index >= fields.size()) {
                    throw new IllegalArgumentException("Expression field is out of bounds");
                }
                return fields.get(index);
            }
        }
        return SubstraitSchemaFields.computed(name, SubstraitExpressionTypes.typeOf(expression, input));
    }

    private static NamedStruct filter(Rel relation, NamedStruct input) {
        requireBoolean(relation.getFilter().getCondition(), input);
        return input;
    }

    private static NamedStruct window(Rel relation, NamedStruct input) {
        List<SubstraitSchemaFields.Field> fields = new ArrayList<>(SubstraitSchemaFields.fields(input));
        var window = relation.getWindow();
        window.getPartitionExpressionsList().forEach(expression -> SubstraitExpressionTypes.typeOf(expression, input));
        sorted(window.getSortsList(), input);
        for (int i = 0; i < window.getWindowFunctionsCount(); i++) {
            fields.add(SubstraitSchemaFields.computed("window_" + i,
                    SubstraitExpressionTypes.declared(window.getWindowFunctions(i).getOutputType())));
        }
        return SubstraitSchemaFields.schema(fields, input.getStruct());
    }

    private static NamedStruct sorted(List<io.substrait.proto.SortField> sorts, NamedStruct input) {
        sorts.forEach(sort -> SubstraitExpressionTypes.typeOf(sort.getExpr(), input));
        return input;
    }

    private static NamedStruct exchange(Rel relation, NamedStruct input) {
        var exchange = relation.getExchange();
        exchange.getScatterByFields().getFieldsList().forEach(field -> SubstraitExpressionTypes.typeOf(
                Expression.newBuilder().setSelection(field).build(), input));
        if (exchange.hasSingleTarget()) {
            SubstraitExpressionTypes.typeOf(exchange.getSingleTarget().getExpression(), input);
        }
        if (exchange.hasMultiTarget()) {
            SubstraitExpressionTypes.typeOf(exchange.getMultiTarget().getExpression(), input);
        }
        return input;
    }

    private static NamedStruct expand(Rel relation, NamedStruct input) {
        List<SubstraitSchemaFields.Field> fields = new ArrayList<>(SubstraitSchemaFields.fields(input));
        var expand = relation.getExpand();
        if (expand.getFieldsCount() > fields.size()) {
            throw new IllegalArgumentException("Expand exceeds its input arity");
        }
        for (int i = 0; i < expand.getFieldsCount(); i++) {
            var field = expand.getFields(i);
            Type type = field.hasConsistentField() ? SubstraitExpressionTypes.typeOf(field.getConsistentField(), input)
                    : SubstraitExpressionTypes.homogeneous(field.getSwitchingField().getDuplicatesList().stream()
                            .map(expression -> SubstraitExpressionTypes.typeOf(expression, input)).toList());
            fields.set(i, SubstraitSchemaFields.computed(fields.get(i).name(), type));
        }
        fields.add(SubstraitSchemaFields.computed("duplicate_index",
                SubstraitSchemaFields.primitive("i64", Type.Nullability.NULLABILITY_REQUIRED)));
        return SubstraitSchemaFields.schema(fields);
    }

    static void requireBoolean(Expression condition, NamedStruct context) {
        if (!SubstraitExpressionTypes.typeOf(condition, context).hasBool()) {
            throw new IllegalArgumentException("Relational condition must be BOOLEAN");
        }
    }

    private static NamedStruct unary(List<NamedStruct> inputs) {
        if (inputs.size() != 1) throw new IllegalArgumentException("Unary relation requires one input");
        return inputs.get(0);
    }

    private static NamedStruct binary(List<NamedStruct> inputs, NamedStruct context) {
        if (inputs.size() != 2) throw new IllegalArgumentException("Binary relation requires two inputs");
        return context;
    }
}
