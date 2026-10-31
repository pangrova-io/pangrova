package org.pangrova.protocol.retrieval;

import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.substrait.proto.Expression;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import io.substrait.proto.SetRel;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SortField;
import io.substrait.proto.Type;
import org.pangrova.protocol.retrieval.RetrievalFanOutBinding.Source;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.common.api.annotation.Internal;

/** Builds independent Native retrieval branches below one shuffle producer. */
@Internal
public final class RetrievalFanOutPlanBinder {

    private static final String SOURCE_FIELD_PREFIX = "proj_";
    private static final String REL_MESSAGE_NAME = "substrait.Rel";

    private RetrievalFanOutPlanBinder() {}

    /**
     * Builds independently bound retrieval branches below one producer exchange.
     *
     * @param planBytes serialized producer plan
     * @param fanOut validated retrieval sources
     * @param idPrefix canonical relation identifier prefix
     * @return rewritten plan and all branch operator bindings
     */
    public static BoundExecutionPlan bindExecution(
            byte[] planBytes, RetrievalFanOutBinding fanOut, String idPrefix) {
        if (idPrefix == null || idPrefix.isBlank()) {
            throw new IllegalArgumentException("fan-out relation ID prefix is required");
        }
        try {
            Plan original = Plan.parseFrom(planBytes);
            return bindParsed(original, fanOut, idPrefix);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "invalid Substrait fan-out plan or candidate schema", failure);
        }
    }

    private static BoundExecutionPlan bindParsed(
            Plan original, RetrievalFanOutBinding fanOut, String idPrefix)
            throws InvalidProtocolBufferException {
        requireSingleRootedExchange(original);
        RetrievalFanOutScoreContract.validate(fanOut);
        Map<Integer, String> functions = SubstraitFunctionNames.index(original);
        List<Rel> branches = new ArrayList<>();
        List<OperatorBinding> operators = new ArrayList<>();
        Rel exchangeTemplate = bindBranches(
                original, fanOut, idPrefix, functions, branches, operators);
        Rel union = Rel.newBuilder().setSet(SetRel.newBuilder()
                .setOp(SetRel.SetOp.SET_OP_UNION_ALL)
                .addAllInputs(branches)).build();
        Rel outputExchange = exchangeTemplate.toBuilder().setExchange(
                exchangeTemplate.getExchange().toBuilder().setInput(union)
                        .setCommon(SubstraitRelationLayout.withAppendedInput(
                                exchangeTemplate.getExchange().getCommon(),
                                SubstraitRelationLayout.describe(branches.get(0)).outputSchema()
                                        .getStruct().getTypesCount() - 1,
                                0, 1, true))).build();
        var root = original.getRelations(0).getRoot().toBuilder()
                .setInput(outputExchange)
                .addNames(sourceFieldName(fanOut.sources().get(0).binding()));
        Plan output = original.toBuilder().setRelations(
                0, original.getRelations(0).toBuilder().setRoot(root)).build();
        SubstraitRelationLayout.rootSchema(output.getRelations(0).getRoot());
        return new BoundExecutionPlan(output.toByteArray(), operators);
    }

    private static Rel bindBranches(
            Plan original,
            RetrievalFanOutBinding fanOut,
            String idPrefix,
            Map<Integer, String> functions,
            List<Rel> branches,
            List<OperatorBinding> operators) throws InvalidProtocolBufferException {
        Rel exchangeTemplate = null;
        int sourceIndex = 0;
        for (Source source : fanOut.sources()) {
            Plan branchPlan = prepareBranch(original, source.binding(), functions);
            BoundExecutionPlan bound = RetrievalTopKPlanBinder.bindExecution(
                    branchPlan.toByteArray(), List.of(source.binding()),
                    idPrefix + "-source-" + sourceIndex++);
            Rel exchange = Plan.parseFrom(bound.planBytes())
                    .getRelations(0).getRoot().getInput();
            requireProducerExchange(exchange);
            exchangeTemplate = requireMatchingExchange(exchangeTemplate, exchange);
            branches.add(appendSourceId(
                    exchange.getExchange().getInput(), source.sourceId(), source.binding()));
            operators.addAll(bound.operatorBindings());
        }
        return exchangeTemplate;
    }

    private static void requireProducerExchange(Rel exchange) {
        if (!exchange.hasExchange()) {
            throw new IllegalArgumentException(
                    "retrieval fan-out requires a producer exchange");
        }
    }

    private static Rel requireMatchingExchange(Rel template, Rel exchange) {
        if (template != null && !sameExchange(template, exchange)) {
            throw new IllegalArgumentException(
                    "retrieval fan-out branch exchange contracts differ");
        }
        return template == null ? exchange : template;
    }

    /** Schema transported through the singleton exchange after source tagging. */
    public static NamedStruct candidateSchema(RetrievalTopKBinding binding) {
        try {
            NamedStruct schema = NamedStruct.parseFrom(binding.candidateSchema());
            return schema.toBuilder()
                    .addNames(sourceFieldName(schema))
                    .setStruct(schema.getStruct().toBuilder().addTypes(
                            Type.newBuilder().setString(Type.String.newBuilder()
                                    .setNullability(Type.Nullability
                                            .NULLABILITY_REQUIRED))))
                    .build();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("invalid fan-out candidate schema", failure);
        }
    }

    /**
     * Field name assigned by the Native Substrait project converter to the
     * appended source literal. Keeping the transported schema identical to the
     * physical project output makes exchange deserialization fail closed on a
     * real contract mismatch instead of relying on a Coordinator-only alias.
     */
    public static String sourceFieldName(RetrievalTopKBinding binding) {
        try {
            return sourceFieldName(NamedStruct.parseFrom(binding.candidateSchema()));
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("invalid fan-out candidate schema", failure);
        }
    }

    private static String sourceFieldName(NamedStruct schema) {
        return SOURCE_FIELD_PREFIX + schema.getStruct().getTypesCount();
    }

    private static void requireSingleRootedExchange(Plan plan) {
        if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()
                || !plan.getRelations(0).getRoot().getInput().hasExchange()) {
            throw new IllegalArgumentException(
                    "retrieval fan-out requires one rooted producer exchange");
        }
    }

    private static Plan prepareBranch(
            Plan original,
            RetrievalTopKBinding binding,
            Map<Integer, String> functions) {
        BranchRewriter rewriter = new BranchRewriter(binding, functions);
        var root = original.getRelations(0).getRoot();
        Rel rewritten = rewriter.rewrite(root.getInput());
        if (rewriter.rewritten != 1) {
            throw new IllegalArgumentException(
                    "retrieval fan-out requires exactly one matching Top-K");
        }
        Plan.Builder output = original.toBuilder();
        output.setRelations(0, original.getRelations(0).toBuilder().setRoot(
                root.toBuilder().setInput(rewritten)));
        if (rewriter.bm25FunctionAnchor >= 0) {
            output.addExtensions(SimpleExtensionDeclaration.newBuilder()
                    .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction
                            .newBuilder()
                            .setFunctionAnchor(rewriter.bm25FunctionAnchor)
                            .setName("bm25_score")));
        }
        return output.build();
    }

    private static Rel appendSourceId(
            Rel input, String sourceId, RetrievalTopKBinding binding) {
        NamedStruct schema;
        try {
            schema = NamedStruct.parseFrom(binding.candidateSchema());
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("invalid candidate schema", failure);
        }
        ProjectRel.Builder project = ProjectRel.newBuilder().setInput(input);
        for (int index = 0; index < schema.getStruct().getTypesCount(); ++index) {
            project.addExpressions(fieldReference(index));
        }
        project.addExpressions(Expression.newBuilder().setLiteral(
                Expression.Literal.newBuilder().setString(sourceId)));
        int fields = schema.getStruct().getTypesCount();
        for (int ordinal = 0; ordinal <= fields; ordinal++) {
            project.getCommonBuilder().getEmitBuilder().addOutputMapping(fields + ordinal);
        }
        return Rel.newBuilder().setProject(project).build();
    }

    private static boolean sameExchange(Rel left, Rel right) {
        return left.getExchange().toBuilder().clearInput().build().equals(
                right.getExchange().toBuilder().clearInput().build());
    }

    private static Expression fieldReference(int fieldIndex) {
        return SubstraitRelationLayout.rootFieldReference(fieldIndex);
    }

    private static final class BranchRewriter {
        private final RetrievalTopKBinding binding;
        private final Map<Integer, String> functions;
        private int rewritten;
        private int bm25FunctionAnchor = -1;

        private BranchRewriter(
                RetrievalTopKBinding binding, Map<Integer, String> functions) {
            this.binding = binding;
            this.functions = functions;
        }

        private Rel rewrite(Rel relation) {
            FieldDescriptor active = relation.getDescriptorForType().findFieldByNumber(
                    relation.getRelTypeCase().getNumber());
            if (active == null || !relation.hasField(active)) {
                return relation;
            }
            Message payload = (Message) relation.getField(active);
            Message rewrittenPayload = rewriteInputs(payload);
            Rel rewrittenRelation = relation.toBuilder()
                    .setField(active, rewrittenPayload).build();
            return rewrittenRelation.hasFetch()
                    ? rewriteTopK(rewrittenRelation)
                    : rewrittenRelation;
        }

        private Message rewriteInputs(Message payload) {
            Message.Builder builder = payload.toBuilder();
            for (FieldDescriptor field : payload.getDescriptorForType().getFields()) {
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                        || !field.getMessageType().getFullName().equals(
                                REL_MESSAGE_NAME)) {
                    continue;
                }
                if (field.isRepeated()) {
                    for (int index = 0;
                            index < payload.getRepeatedFieldCount(field); ++index) {
                        builder.setRepeatedField(field, index, rewrite(
                                (Rel) payload.getRepeatedField(field, index)));
                    }
                } else if (payload.hasField(field)) {
                    builder.setField(field, rewrite((Rel) payload.getField(field)));
                }
            }
            return builder.build();
        }

        private Rel rewriteTopK(Rel relation) {
            var fetch = relation.getFetch();
            if (SubstraitFetchBounds.offset(fetch) != 0 || !fetch.getInput().hasSort()
                    || fetch.getInput().getSort().getSortsCount() != 1
                    || scoreFieldIndex(fetch.getInput().getSort().getSorts(0))
                            != binding.scoreFieldIndex()) {
                return relation;
            }
            var sort = fetch.getInput().getSort().toBuilder();
            Rel candidate = sort.getInput();
            if (BuiltInCapabilityCatalog.BM25_RETRIEVAL_ID.equals(
                    binding.capability().getCapabilityId())) {
                candidate = replaceScoreWithBm25(candidate);
            } else {
                candidate = stripPositiveMatchPredicates(candidate);
            }
            sort.setInput(candidate);
            sort.setSorts(0, sort.getSorts(0).toBuilder().setDirection(
                    binding.scoreDirection()
                            == org.pangrova.protocol.v2.ScoreDirection
                                    .SCORE_DIRECTION_HIGHER_IS_BETTER
                            ? SortField.SortDirection
                                    .SORT_DIRECTION_DESC_NULLS_LAST
                            : SortField.SortDirection
                                    .SORT_DIRECTION_ASC_NULLS_LAST));
            ++rewritten;
            return relation.toBuilder().setFetch(fetch.toBuilder()
                    .setCountExpr(SubstraitFetchBounds.literal(binding.outputLimit()))
                    .setInput(Rel.newBuilder().setSort(sort))).build();
        }

        private Rel replaceScoreWithBm25(Rel candidate) {
            if (!candidate.hasProject()) {
                throw new IllegalArgumentException(
                        "hybrid BM25 retrieval requires a projected score field");
            }
            var layout = SubstraitRelationLayout.describe(candidate);
            int scoreField = binding.scoreFieldIndex();
            if (scoreField < 0 || scoreField >= layout.outputMapping().size()) {
                throw new IllegalArgumentException("BM25 score output ordinal is invalid");
            }
            int scoreExpression = layout.outputMapping().get(scoreField)
                    - layout.expressionSchema().getStruct().getTypesCount();
            if (scoreExpression < 0
                    || scoreExpression >= candidate.getProject().getExpressionsCount()) {
                throw new IllegalArgumentException(
                        "hybrid BM25 retrieval requires a projected score field");
            }
            Bm25Call match = findPositiveMatch(candidate.getProject().getInput());
            String query = binding.invocationOptions().stream()
                    .filter(option -> option.getName().equals("query_text"))
                    .map(option -> option.getValue().getStringValue())
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "BM25 fan-out source has no query_text option"));
            if (!query.equals(match.queryText())) {
                throw new IllegalArgumentException(
                        "BM25 artifact query differs from the MATCH predicate");
            }
            bm25FunctionAnchor = functions.keySet().stream()
                    .mapToInt(Integer::intValue).max().orElse(0) + 1;
            Expression score = Expression.newBuilder().setScalarFunction(
                    Expression.ScalarFunction.newBuilder()
                            .setFunctionReference(bm25FunctionAnchor)
                            .addArguments(io.substrait.proto.FunctionArgument.newBuilder()
                                    .setValue(match.column()))
                            .addArguments(io.substrait.proto.FunctionArgument.newBuilder()
                                    .setValue(Expression.newBuilder().setLiteral(
                                            Expression.Literal.newBuilder()
                                                    .setString(match.queryText()))))
                            .setOutputType(Bm25MaterializedScoreBinder.nullableFp64()))
                    .build();
            return candidate.toBuilder().setProject(
                    candidate.getProject().toBuilder().setExpressions(
                            scoreExpression, score)).build();
        }

        private Bm25Call findPositiveMatch(Rel relation) {
            if (relation.hasFilter()) {
                Bm25Call found = findPositiveMatch(
                        relation.getFilter().getCondition());
                if (found != null) {
                    return found;
                }
                return findPositiveMatch(relation.getFilter().getInput());
            }
            if (relation.hasProject()) {
                return findPositiveMatch(relation.getProject().getInput());
            }
            throw new IllegalArgumentException(
                    "BM25 fan-out requires MATCH as a positive conjunct");
        }

        private Bm25Call findPositiveMatch(Expression expression) {
            if (!expression.hasScalarFunction()) {
                return null;
            }
            var scalar = expression.getScalarFunction();
            String name = functions.get(scalar.getFunctionReference());
            if ("match".equals(name)) {
                return requireMatchCall(scalar);
            }
            if (!"and".equals(name)) {
                return null;
            }
            return findPositiveMatchInArguments(scalar);
        }

        private Bm25Call requireMatchCall(Expression.ScalarFunction scalar) {
            if (scalar.getArgumentsCount() != 2
                    || !scalar.getArguments(0).hasValue()
                    || !scalar.getArguments(1).hasValue()) {
                throw new IllegalArgumentException("MATCH contract is invalid");
            }
            Expression column = scalar.getArguments(0).getValue();
            Expression query = scalar.getArguments(1).getValue();
            if (!query.hasLiteral() || !query.getLiteral().hasString()) {
                throw new IllegalArgumentException(
                        "MATCH fan-out query must be a string literal");
            }
            return new Bm25Call(column, query.getLiteral().getString());
        }

        private Bm25Call findPositiveMatchInArguments(Expression.ScalarFunction scalar) {
            for (var argument : scalar.getArgumentsList()) {
                if (!argument.hasValue()) {
                    continue;
                }
                Bm25Call found = findPositiveMatch(argument.getValue());
                if (found != null) {
                    return found;
                }
            }
            return null;
        }

        private Rel stripPositiveMatchPredicates(Rel relation) {
            if (relation.hasFilter()) {
                var filter = relation.getFilter().toBuilder()
                        .setInput(stripPositiveMatchPredicates(
                                relation.getFilter().getInput()))
                        .setCondition(stripPositiveMatches(
                                relation.getFilter().getCondition()));
                return relation.toBuilder().setFilter(filter).build();
            }
            if (relation.hasProject()) {
                return relation.toBuilder().setProject(
                        relation.getProject().toBuilder().setInput(
                                stripPositiveMatchPredicates(
                                        relation.getProject().getInput())))
                        .build();
            }
            return relation;
        }

        private Expression stripPositiveMatches(Expression expression) {
            if (!expression.hasScalarFunction()) {
                return expression;
            }
            var scalar = expression.getScalarFunction();
            String name = functions.get(scalar.getFunctionReference());
            if ("match".equals(name)) {
                return Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setBoolean(true)).build();
            }
            if ("and".equals(name)) {
                var rewritten = scalar.toBuilder().clearArguments();
                for (var argument : scalar.getArgumentsList()) {
                    if (!argument.hasValue()) {
                        throw new IllegalArgumentException(
                                "MATCH conjunction has a non-expression argument");
                    }
                    rewritten.addArguments(argument.toBuilder().setValue(
                            stripPositiveMatches(argument.getValue())));
                }
                return expression.toBuilder().setScalarFunction(rewritten).build();
            }
            if (containsMatch(expression)) {
                throw new IllegalArgumentException(
                        "hybrid retrieval supports MATCH only as a positive conjunct");
            }
            return expression;
        }

        private boolean containsMatch(Expression expression) {
            if (!expression.hasScalarFunction()) {
                return false;
            }
            var scalar = expression.getScalarFunction();
            if ("match".equals(functions.get(scalar.getFunctionReference()))) {
                return true;
            }
            return scalar.getArgumentsList().stream()
                    .filter(io.substrait.proto.FunctionArgument::hasValue)
                    .map(io.substrait.proto.FunctionArgument::getValue)
                    .anyMatch(this::containsMatch);
        }

        private static int scoreFieldIndex(SortField sort) {
            if (!sort.hasExpr() || !sort.getExpr().hasSelection()
                    || !sort.getExpr().getSelection().hasDirectReference()
                    || !sort.getExpr().getSelection().getDirectReference()
                            .hasStructField()) {
                return -1;
            }
            return sort.getExpr().getSelection().getDirectReference()
                    .getStructField().getField();
        }
    }

    private record Bm25Call(Expression column, String queryText) {}
}
