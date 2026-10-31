package org.pangrova.protocol.retrieval;

import java.util.Map;

import io.substrait.proto.Expression;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.common.api.annotation.Internal;

/** Rewrites a BM25 Top-K input to consume its materialized Native score. */
@Internal
final class Bm25MaterializedScoreBinder {

    private Bm25MaterializedScoreBinder() {}

    static Rel bind(
            Rel input, RetrievalTopKBinding binding,
            Map<Integer, String> functionNames) {
        int scoreFieldIndex = binding.scoreFieldIndex();
        if (!input.hasProject()) {
            throw new IllegalArgumentException(
                    "BM25 artifact retrieval requires a projected BM25_SCORE field");
        }
        var layout = SubstraitRelationLayout.describe(input);
        int inputFields = layout.expressionSchema().getStruct().getTypesCount();
        int expressionIndex = scoreFieldIndex < 0 || scoreFieldIndex >= layout.outputMapping().size()
                ? -1 : layout.outputMapping().get(scoreFieldIndex) - inputFields;
        if (expressionIndex < 0 || expressionIndex >= input.getProject().getExpressionsCount()) {
            throw new IllegalArgumentException("BM25 artifact retrieval requires a projected BM25_SCORE field");
        }
        Expression score = input.getProject().getExpressions(expressionIndex);
        if (!score.hasScalarFunction()
                || !"bm25_score".equals(functionNames.get(
                        score.getScalarFunction().getFunctionReference()))) {
            throw new IllegalArgumentException(
                    "BM25 artifact binding does not match the Top-K score expression");
        }
        Bm25Call scoreCall = requireBm25Call(score, "BM25_SCORE");
        if (!scoreCall.queryText().equals(stringOption(binding, "query_text"))) {
            throw new IllegalArgumentException(
                    "BM25 score expression and retrieval query option differ");
        }
        MaterializedInput materialized = appendMaterializedScore(
                input.getProject().getInput(), scoreCall, functionNames);
        var project = input.getProject().toBuilder()
                .setInput(materialized.relation())
                .setCommon(SubstraitRelationLayout.withAppendedInput(input.getProject().getCommon(),
                        inputFields, input.getProject().getExpressionsCount(), 1, false))
                .setExpressions(expressionIndex, materializedScoreExpression(materialized.fieldIndex(),
                        score.getScalarFunction().getOutputType()));
        return input.toBuilder().setProject(project).build();
    }

    private static MaterializedInput appendMaterializedScore(
            Rel relation, Bm25Call scoreCall,
            Map<Integer, String> functionNames) {
        if (relation.hasRead()) {
            return appendToRead(relation, scoreCall, functionNames);
        }
        if (relation.hasFilter()) {
            return appendToFilter(relation, scoreCall, functionNames);
        }
        if (relation.hasProject()) {
            return appendToProject(relation, scoreCall, functionNames);
        }
        throw new IllegalArgumentException(
                "BM25 materialized score supports only unary scan/filter/project inputs");
    }

    private static MaterializedInput appendToRead(Rel relation, Bm25Call scoreCall,
            Map<Integer, String> functionNames) {
        var read = relation.getRead().toBuilder();
        var layout = SubstraitRelationLayout.describe(relation);
        if (!read.hasBaseSchema() || !read.hasNamedTable()
                || read.getBaseSchema().getNamesList().contains(
                        BuiltInCapabilityCatalog.MATERIALIZED_RETRIEVAL_SCORE_COLUMN)) {
            throw new IllegalArgumentException(
                    "BM25 materialized score requires a collision-free named table scan");
        }
        int baseIndex = read.getBaseSchema().getStruct().getTypesCount();
        Bm25Call baseCall = remapCall(scoreCall, layout.outputMapping());
        int directFields = layout.directSchema().getStruct().getTypesCount();
        if (read.hasProjection()) {
            int field = SubstraitFieldReference.directFieldIndex(baseCall.column());
            var item = read.getProjection().getSelect().getStructItems(field);
            if (item.hasChild()) throw new IllegalArgumentException("BM25 requires an unmodified source text column");
            baseCall = new Bm25Call(SubstraitFieldReference.fieldReference(item.getField()), scoreCall.queryText());
            read.getProjectionBuilder().getSelectBuilder().addStructItems(
                    Expression.MaskExpression.StructItem.newBuilder().setField(baseIndex));
        }
        if (read.hasFilter()) read.setFilter(rewriteSatisfiedMatch(read.getFilter(), baseCall, functionNames));
        if (read.hasBestEffortFilter()) {
            read.setBestEffortFilter(rewriteSatisfiedMatch(read.getBestEffortFilter(), baseCall, functionNames));
        }
        var schema = read.getBaseSchema().toBuilder()
                .addNames(BuiltInCapabilityCatalog.MATERIALIZED_RETRIEVAL_SCORE_COLUMN);
        schema.getStructBuilder().addTypes(nullableFp64());
        read.setBaseSchema(schema);
        read.setCommon(SubstraitRelationLayout.withAppendedInput(read.getCommon(), directFields, 0, 1, true));
        return new MaterializedInput(
                relation.toBuilder().setRead(read).build(), layout.outputSchema().getStruct().getTypesCount());
    }

    private static MaterializedInput appendToFilter(
            Rel relation, Bm25Call scoreCall, Map<Integer, String> functionNames) {
        var layout = SubstraitRelationLayout.describe(relation);
        Bm25Call childCall = remapCall(scoreCall, layout.outputMapping());
        MaterializedInput child = appendMaterializedScore(
                relation.getFilter().getInput(), childCall, functionNames);
        Expression condition = rewriteSatisfiedMatch(
                relation.getFilter().getCondition(), childCall, functionNames);
        return new MaterializedInput(
                relation.toBuilder().setFilter(
                        relation.getFilter().toBuilder()
                                .setInput(child.relation())
                                .setCommon(SubstraitRelationLayout.withAppendedInput(relation.getFilter().getCommon(),
                                        layout.expressionSchema().getStruct().getTypesCount(), 0, 1, true))
                                .setCondition(condition))
                        .build(),
                layout.outputSchema().getStruct().getTypesCount());
    }

    private static MaterializedInput appendToProject(
            Rel relation, Bm25Call scoreCall, Map<Integer, String> functionNames) {
        var layout = SubstraitRelationLayout.describe(relation);
        int inputFields = layout.expressionSchema().getStruct().getTypesCount();
        Bm25Call childCall = projectCall(relation, scoreCall, layout, inputFields);
        MaterializedInput child = appendMaterializedScore(
                relation.getProject().getInput(), childCall, functionNames);
        int outputIndex = layout.outputSchema().getStruct().getTypesCount();
        var project = relation.getProject().toBuilder()
                .setInput(child.relation())
                .setCommon(SubstraitRelationLayout.withAppendedInput(relation.getProject().getCommon(), inputFields,
                        relation.getProject().getExpressionsCount(), 1, true));
        return new MaterializedInput(
                relation.toBuilder().setProject(project).build(), outputIndex);
    }

    private static Bm25Call projectCall(Rel relation, Bm25Call call,
            SubstraitRelationLayout.RelationLayout layout, int inputFields) {
        Bm25Call mapped = remapCall(call, layout.outputMapping());
        int field = SubstraitFieldReference.directFieldIndex(mapped.column());
        if (field < inputFields) return mapped;
        Expression column = relation.getProject().getExpressions(field - inputFields);
        if (SubstraitFieldReference.directFieldIndex(column) < 0) {
            throw new IllegalArgumentException("BM25 materialized source requires an unmodified source column");
        }
        return new Bm25Call(column, call.queryText());
    }

    private static Bm25Call remapCall(Bm25Call call, java.util.List<Integer> mapping) {
        int field = SubstraitFieldReference.directFieldIndex(call.column());
        if (field < 0 || field >= mapping.size()) throw new IllegalArgumentException("BM25 column is out of bounds");
        return new Bm25Call(SubstraitFieldReference.fieldReference(mapping.get(field)), call.queryText());
    }

    private static Expression rewriteSatisfiedMatch(
            Expression expression, Bm25Call scoreCall,
            Map<Integer, String> functionNames) {
        if (!expression.hasScalarFunction()) {
            return expression;
        }
        var scalar = expression.getScalarFunction();
        String functionName = functionNames.get(scalar.getFunctionReference());
        if ("match".equals(functionName)) {
            Bm25Call matchCall = requireBm25Call(expression, "MATCH");
            if (!matchCall.queryText().equals(scoreCall.queryText())
                    || SubstraitFieldReference.directFieldIndex(matchCall.column())
                    != SubstraitFieldReference.directFieldIndex(scoreCall.column())) {
                throw new IllegalArgumentException(
                        "BM25 MATCH predicate differs from its score expression");
            }
            return Expression.newBuilder().setLiteral(
                    Expression.Literal.newBuilder().setBoolean(true)).build();
        }
        if ("and".equals(functionName)) {
            var rewritten = scalar.toBuilder().clearArguments();
            for (var argument : scalar.getArgumentsList()) {
                if (!argument.hasValue()) {
                    throw new IllegalArgumentException(
                            "BM25 MATCH conjunction contains a non-expression argument");
                }
                rewritten.addArguments(argument.toBuilder().setValue(
                        rewriteSatisfiedMatch(
                                argument.getValue(), scoreCall, functionNames)));
            }
            return expression.toBuilder().setScalarFunction(rewritten).build();
        }
        if (containsMatch(expression, functionNames)) {
            throw new IllegalArgumentException(
                    "BM25 candidate pruning supports MATCH only as a positive conjunct");
        }
        return expression;
    }

    private static boolean containsMatch(
            Expression expression, Map<Integer, String> functionNames) {
        if (expression.hasScalarFunction()) {
            var scalar = expression.getScalarFunction();
            if ("match".equals(functionNames.get(scalar.getFunctionReference()))) {
                return true;
            }
            return scalar.getArgumentsList().stream()
                    .filter(io.substrait.proto.FunctionArgument::hasValue)
                    .map(io.substrait.proto.FunctionArgument::getValue)
                    .anyMatch(value -> containsMatch(value, functionNames));
        }
        if (expression.hasCast()) {
            return containsMatch(expression.getCast().getInput(), functionNames);
        }
        return false;
    }

    private static Bm25Call requireBm25Call(
            Expression expression, String label) {
        validateBm25Arguments(expression, label);
        Expression column = expression.getScalarFunction().getArguments(0).getValue();
        Expression query = expression.getScalarFunction().getArguments(1).getValue();
        validateBm25Values(column, query, label);
        return new Bm25Call(column, query.getLiteral().getString());
    }

    private static void validateBm25Arguments(Expression expression, String label) {
        if (!expression.hasScalarFunction()
                || expression.getScalarFunction().getArgumentsCount() != 2
                || !expression.getScalarFunction().getArguments(0).hasValue()
                || !expression.getScalarFunction().getArguments(1).hasValue()) {
            throw new IllegalArgumentException(label + " must have two expression arguments");
        }
    }

    private static void validateBm25Values(
            Expression column, Expression query, String label) {
        if (SubstraitFieldReference.directFieldIndex(column) < 0 || !query.hasLiteral()
                || !query.getLiteral().hasString() || query.getLiteral().getString().isBlank()) {
            throw new IllegalArgumentException(
                    label + " requires a direct column and non-empty literal query");
        }
    }

    private static String stringOption(
            RetrievalTopKBinding binding, String name) {
        return binding.invocationOptions().stream()
                .filter(option -> option.getName().equals(name))
                .map(option -> option.getValue().getStringValue())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "BM25 retrieval is missing option " + name));
    }

    private static Expression materializedScoreExpression(int fieldIndex, Type outputType) {
        return Expression.newBuilder().setCast(Expression.Cast.newBuilder()
                .setInput(SubstraitFieldReference.fieldReference(fieldIndex))
                .setType(outputType)).build();
    }

    static Type nullableFp64() {
        return Type.newBuilder().setFp64(Type.FP64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_NULLABLE)).build();
    }

    private record MaterializedInput(Rel relation, int fieldIndex) {}

    private record Bm25Call(Expression column, String queryText) {}
}
