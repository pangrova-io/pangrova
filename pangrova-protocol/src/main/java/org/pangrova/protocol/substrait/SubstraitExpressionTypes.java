package org.pangrova.protocol.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;

import java.util.ArrayList;
import java.util.List;

/** Type-only inspection; no literal payload decoding or implicit coercion. */
final class SubstraitExpressionTypes {
    private static final int MAX_DECIMAL_PRECISION = 38;

    private SubstraitExpressionTypes() {}

    static Type typeOf(Expression expression, NamedStruct schema) {
        return switch (expression.getRexTypeCase()) {
            case LITERAL -> literal(expression.getLiteral());
            case SELECTION -> reference(expression.getSelection(), schema);
            case SCALAR_FUNCTION -> declared(expression.getScalarFunction().getOutputType());
            case WINDOW_FUNCTION -> declared(expression.getWindowFunction().getOutputType());
            case CAST -> declared(expression.getCast().getType());
            case DYNAMIC_PARAMETER -> declared(expression.getDynamicParameter().getType());
            case NESTED -> nested(expression.getNested(), schema);
            default -> composite(expression, schema);
        };
    }

    static Type declared(Type type) {
        SubstraitSchemaFields.requireValueType(type);
        return declaredShape(type);
    }

    private static Type declaredShape(Type type) {
        if (type.getKindCase() == Type.KindCase.KIND_NOT_SET) {
            throw new IllegalArgumentException("Expression requires a declared output type");
        }
        if (type.hasDecimal() && (type.getDecimal().getPrecision() < 1
                || type.getDecimal().getPrecision() > MAX_DECIMAL_PRECISION || type.getDecimal().getScale() < 0
                || type.getDecimal().getScale() > type.getDecimal().getPrecision())) {
            throw new IllegalArgumentException("Invalid declared decimal precision/scale");
        }
        if (type.hasStruct()) type.getStruct().getTypesList().forEach(SubstraitExpressionTypes::declaredShape);
        if (type.hasList()) declaredShape(type.getList().getType());
        if (type.hasMap()) {
            declaredShape(type.getMap().getKey());
            declaredShape(type.getMap().getValue());
        }
        return type;
    }

    private static Type composite(Expression expression, NamedStruct schema) {
        return switch (expression.getRexTypeCase()) {
            case IF_THEN -> ifThen(expression.getIfThen(), schema);
            case SWITCH_EXPRESSION -> switchType(expression.getSwitchExpression(), schema);
            case SINGULAR_OR_LIST, MULTI_OR_LIST ->
                    SubstraitSchemaFields.primitive("bool", Type.Nullability.NULLABILITY_NULLABLE);
            case LAMBDA -> lambdaType(expression.getLambda(), schema);
            case LAMBDA_INVOCATION -> typeOf(expression.getLambdaInvocation().getLambda().getBody(),
                    parameterSchema(expression.getLambdaInvocation().getLambda().getParameters()));
            case SUBQUERY -> subqueryType(expression.getSubquery());
            case EXECUTION_CONTEXT_VARIABLE -> contextVariable(expression.getExecutionContextVariable());
            default -> throw new IllegalArgumentException("Expression has no canonical type: "
                    + expression.getRexTypeCase());
        };
    }

    private static Type reference(Expression.FieldReference reference, NamedStruct schema) {
        Type root;
        if (reference.hasExpression()) root = typeOf(reference.getExpression(), schema);
        else if (reference.hasRootReference()) {
            SubstraitSchemaFields.fields(schema);
            var container = schema.getStruct();
            // NamedStruct's outer container is not an expression ROW value.
            // Only this local root view may omit container nullability; all
            // actual fields were admitted above without adding defaults.
            if (container.getNullability() == Type.Nullability.NULLABILITY_UNSPECIFIED) {
                container = container.toBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED).build();
            }
            root = Type.newBuilder().setStruct(container).build();
            if (reference.hasDirectReference() && !reference.getDirectReference().hasStructField()) {
                throw new IllegalArgumentException("Root field reference requires a struct_field");
            }
        } else {
            throw new IllegalArgumentException("Field reference requires an explicit supported root: "
                    + reference.getRootTypeCase());
        }
        if (reference.hasMaskedReference()) return SubstraitMasks.projectType(root, reference.getMaskedReference());
        if (!reference.hasDirectReference()) throw new IllegalArgumentException("Field reference has no path");
        return segment(reference.getDirectReference(), root);
    }

    private static Type segment(Expression.ReferenceSegment segment, Type root) {
        if (segment.hasStructField()) {
            var field = segment.getStructField();
            int index = field.getField();
            if (!root.hasStruct() || index < 0 || index >= root.getStruct().getTypesCount()) {
                throw new IllegalArgumentException("Struct field reference is out of bounds: " + index);
            }
            Type selected = root.getStruct().getTypes(index);
            if (nullable(root)) {
                selected = SubstraitSchemaFields.withNullability(selected, Type.Nullability.NULLABILITY_NULLABLE);
            }
            return field.hasChild() ? segment(field.getChild(), selected) : selected;
        }
        if (segment.hasListElement()) {
            if (!root.hasList()) throw new IllegalArgumentException("List reference requires a list");
            var field = segment.getListElement();
            Type selected = SubstraitSchemaFields.withNullability(
                    root.getList().getType(), Type.Nullability.NULLABILITY_NULLABLE);
            return field.hasChild() ? segment(field.getChild(), selected) : selected;
        }
        if (segment.hasMapKey()) {
            if (!root.hasMap()) throw new IllegalArgumentException("Map reference requires a map");
            var field = segment.getMapKey();
            Type selected = SubstraitSchemaFields.withNullability(
                    root.getMap().getValue(), Type.Nullability.NULLABILITY_NULLABLE);
            return field.hasChild() ? segment(field.getChild(), selected) : selected;
        }
        throw new IllegalArgumentException("Field reference has no canonical segment");
    }

    private static Type ifThen(Expression.IfThen conditional, NamedStruct schema) {
        List<Type> branches = new ArrayList<>();
        for (var clause : conditional.getIfsList()) {
            if (!typeOf(clause.getIf(), schema).hasBool()) {
                throw new IllegalArgumentException("IF condition must be BOOLEAN");
            }
            branches.add(typeOf(clause.getThen(), schema));
        }
        branches.add(typeOf(conditional.getElse(), schema));
        return homogeneous(branches);
    }

    private static Type switchType(Expression.SwitchExpression conditional, NamedStruct schema) {
        List<Type> branches = conditional.getIfsList().stream()
                .map(clause -> typeOf(clause.getThen(), schema))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
        branches.add(typeOf(conditional.getElse(), schema));
        return homogeneous(branches);
    }

    static Type homogeneous(List<Type> types) {
        if (types.isEmpty()) throw new IllegalArgumentException("Empty values require an explicit element type");
        types.forEach(SubstraitExpressionTypes::declared);
        Type expected = types.get(0);
        if (expected.hasStruct() || expected.hasList() || expected.hasMap()) return SubstraitSetShape.merge(types);
        for (Type type : types) {
            if (!SubstraitSchemaFields.samePhysicalType(expected, type)) {
                throw new IllegalArgumentException("Expression values require matching canonical types");
            }
        }
        boolean nullable = types.stream().anyMatch(SubstraitExpressionTypes::nullable);
        return nullable ? SubstraitSchemaFields.withNullability(expected,
                Type.Nullability.NULLABILITY_NULLABLE) : expected;
    }

    static boolean nullable(Type type) {
        return SubstraitSchemaFields.nullable(type);
    }

    private static Type nested(Expression.Nested nested, NamedStruct schema) {
        Type result;
        if (nested.hasStruct()) {
            result = Type.newBuilder().setStruct(Type.Struct.newBuilder().addAllTypes(
                    nested.getStruct().getFieldsList().stream().map(field -> typeOf(field, schema)).toList())).build();
        } else if (nested.hasList()) {
            result = Type.newBuilder().setList(Type.List.newBuilder().setType(homogeneous(
                    nested.getList().getValuesList().stream().map(field -> typeOf(field, schema)).toList()))).build();
        } else if (nested.hasMap()) {
            var pairs = nested.getMap().getKeyValuesList();
            result = Type.newBuilder().setMap(Type.Map.newBuilder()
                    .setKey(homogeneous(pairs.stream().map(pair -> typeOf(pair.getKey(), schema)).toList()))
                    .setValue(homogeneous(pairs.stream()
                            .map(pair -> typeOf(pair.getValue(), schema)).toList()))).build();
        } else throw new IllegalArgumentException("Nested expression has no declared shape");
        return SubstraitSchemaFields.withVariation(SubstraitSchemaFields.withNullability(result,
                nested.getNullable() ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED),
                nested.getTypeVariationReference());
    }

    private static Type literal(Expression.Literal literal) {
        Type result = switch (literal.getLiteralTypeCase()) {
            case NULL -> declared(literal.getNull());
            case EMPTY_LIST -> Type.newBuilder().setList(literal.getEmptyList()).build();
            case EMPTY_MAP -> Type.newBuilder().setMap(literal.getEmptyMap()).build();
            case STRUCT -> Type.newBuilder().setStruct(Type.Struct.newBuilder().addAllTypes(
                    literal.getStruct().getFieldsList().stream()
                            .map(SubstraitExpressionTypes::literal).toList())).build();
            case LIST -> Type.newBuilder().setList(Type.List.newBuilder().setType(homogeneous(
                    literal.getList().getValuesList().stream()
                            .map(SubstraitExpressionTypes::literal).toList()))).build();
            case MAP -> mapLiteral(literal.getMap());
            default -> scalarLiteral(literal);
        };
        if (literal.hasNull() || literal.hasEmptyList() || literal.hasEmptyMap()) return declared(result);
        return declared(SubstraitSchemaFields.withVariation(SubstraitSchemaFields.withNullability(result,
                literal.getNullable() ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED),
                literal.getTypeVariationReference()));
    }

    private static Type mapLiteral(Expression.Literal.Map map) {
        return Type.newBuilder().setMap(Type.Map.newBuilder()
                .setKey(homogeneous(map.getKeyValuesList().stream().map(pair -> literal(pair.getKey())).toList()))
                .setValue(homogeneous(map.getKeyValuesList().stream()
                        .map(pair -> literal(pair.getValue())).toList()))).build();
    }

    private static Type scalarLiteral(Expression.Literal literal) {
        return switch (literal.getLiteralTypeCase()) {
            case DECIMAL -> Type.newBuilder().setDecimal(Type.Decimal.newBuilder()
                    .setPrecision(literal.getDecimal().getPrecision())
                    .setScale(literal.getDecimal().getScale())).build();
            case FIXED_CHAR -> Type.newBuilder().setFixedChar(Type.FixedChar.newBuilder()
                    .setLength(literal.getFixedChar().codePointCount(0, literal.getFixedChar().length()))).build();
            case VAR_CHAR -> Type.newBuilder().setVarchar(Type.VarChar.newBuilder()
                    .setLength(literal.getVarChar().getLength())).build();
            case FIXED_BINARY -> Type.newBuilder().setFixedBinary(Type.FixedBinary.newBuilder()
                    .setLength(literal.getFixedBinary().size())).build();
            case PRECISION_TIME -> Type.newBuilder().setPrecisionTime(Type.PrecisionTime.newBuilder()
                    .setPrecision(literal.getPrecisionTime().getPrecision())).build();
            case PRECISION_TIMESTAMP -> Type.newBuilder().setPrecisionTimestamp(Type.PrecisionTimestamp.newBuilder()
                    .setPrecision(literal.getPrecisionTimestamp().getPrecision())).build();
            case PRECISION_TIMESTAMP_TZ -> Type.newBuilder()
                    .setPrecisionTimestampTz(Type.PrecisionTimestampTZ.newBuilder()
                    .setPrecision(literal.getPrecisionTimestampTz().getPrecision())).build();
            default -> primitiveLiteral(literal);
        };
    }

    private static Type primitiveLiteral(Expression.Literal literal) {
        if (literal.hasUserDefined()) return userDefined(literal.getUserDefined());
        String kind = switch (literal.getLiteralTypeCase()) {
            case BOOLEAN -> "bool";
            case INTERVAL_YEAR_TO_MONTH -> "interval_year";
            case INTERVAL_DAY_TO_SECOND -> "interval_day";
            case LITERALTYPE_NOT_SET -> throw new IllegalArgumentException("Literal requires a type");
            default -> literal.getLiteralTypeCase().name().toLowerCase(java.util.Locale.ROOT);
        };
        return SubstraitSchemaFields.primitive(kind, Type.Nullability.NULLABILITY_REQUIRED);
    }

    private static Type userDefined(Expression.Literal.UserDefined literal) {
        var type = Type.UserDefined.newBuilder().addAllTypeParameters(literal.getTypeParametersList());
        if (literal.hasTypeReference()) {
            type.setTypeReference(literal.getTypeReference());
        } else if (literal.hasTypeAliasReference()) {
            return Type.newBuilder().setAlias(Type.TypeAliasReference.newBuilder()
                    .setTypeAliasReference(literal.getTypeAliasReference())).build();
        } else {
            throw new IllegalArgumentException("User-defined literal requires its type anchor");
        }
        return Type.newBuilder().setUserDefined(type).build();
    }

    private static Type contextVariable(Expression.ExecutionContextVariable variable) {
        return switch (variable.getExecutionContextVariableTypeCase()) {
            case CURRENT_TIMESTAMP -> Type.newBuilder().setPrecisionTimestampTz(variable.getCurrentTimestamp()).build();
            case CURRENT_TIMEZONE -> Type.newBuilder().setString(variable.getCurrentTimezone()).build();
            case CURRENT_DATE -> Type.newBuilder().setDate(variable.getCurrentDate()).build();
            default -> throw new IllegalArgumentException("Context variable requires its declared type");
        };
    }

    private static Type lambdaType(Expression.Lambda lambda, NamedStruct schema) {
        return Type.newBuilder().setFunc(Type.Func.newBuilder()
                .addAllParameterTypes(lambda.getParameters().getTypesList())
                .setReturnType(typeOf(lambda.getBody(), parameterSchema(lambda.getParameters())))
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
    }

    private static NamedStruct parameterSchema(Type.Struct parameters) {
        List<SubstraitSchemaFields.Field> fields = new ArrayList<>();
        for (int i = 0; i < parameters.getTypesCount(); i++) {
            fields.add(SubstraitSchemaFields.computed("arg_" + i, parameters.getTypes(i)));
        }
        return SubstraitSchemaFields.schema(fields);
    }

    private static Type subqueryType(Expression.Subquery subquery) {
        if (subquery.hasScalar()) {
            var layout = SubstraitRelationLayout.describe(subquery.getScalar().getInput());
            var types = layout.outputSchema().getStruct().getTypesList();
            if (types.size() != 1) throw new IllegalArgumentException("Scalar subquery must output one field");
            return SubstraitSchemaFields.withNullability(types.get(0), Type.Nullability.NULLABILITY_NULLABLE);
        }
        return SubstraitSchemaFields.primitive("bool", Type.Nullability.NULLABILITY_NULLABLE);
    }
}
