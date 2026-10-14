package org.pangrova.planner.substrait;

import org.pangrova.planner.PlanTable;

import org.pangrova.planner.exchange.ExchangeInputLeafDetail;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.ImmutableFetch;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.ImmutableJoin;
import io.substrait.relation.ImmutableNamedScan;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.ImmutableSort;
import io.substrait.relation.ImmutableVirtualTableScan;
import io.substrait.relation.Join;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import io.substrait.type.proto.TypeProtoConverter;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.PangrovaNestedLoopJoin;
import org.pangrova.planner.cost.join.PangrovaSortMergeJoin;
import org.pangrova.planner.RetrievalTopKPlanHint;
import org.pangrova.planner.AiInvocationPlanHint;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Top-level dispatch: routes Calcite RelNode to the appropriate converter.
 * Delegates to {@link ExpressionConverter}, {@link SetOperationConverter},
 * {@link AggregateConverter}, {@link WindowFunctionConverter},
 * {@link DmlOperationConverter}, {@link CorrelateConverter}, and
 * {@link PhysicalJoinConverter}.
 */
public final class PlanNodeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(PlanNodeConverter.class);

    private final TypeMapper typeMapper;
    private final ExpressionConverter rexConverter;
    private final SetOperationConverter setConverter;
    private final AggregateConverter aggregateConverter;
    private final WindowFunctionConverter windowConverter;
    private final DmlOperationConverter dmlConverter;
    private final CorrelateConverter correlateConverter;
    private final UnnestConverter unnestConverter;
    private final PhysicalJoinConverter physicalJoinConverter;
    private final FunctionMapper functionMapper;
    private final Map<Class<? extends RelNode>, Function<RelNode, Rel>> dispatch;
    private final List<RetrievalTopKPlanHint> retrievalTopKHints = new ArrayList<>();
    private final List<AiInvocationPlanHint> aiInvocationHints = new ArrayList<>();
    private final Set<LogicalJoin> singleMatchJoins =
            Collections.newSetFromMap(new IdentityHashMap<>());

    public PlanNodeConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                            RexBuilder rexBuilder) {
        this(typeMapper, functionMapper, rexBuilder, null);
    }

    /**
     * Creates a converter with optional native capability awareness.
     *
     * @param capabilityRegistry native engine capabilities (nullable; when non-null,
     *                           distinct set ops skip dedup if engine supports them natively)
     */
    public PlanNodeConverter(TypeMapper typeMapper, FunctionMapper functionMapper,
                            RexBuilder rexBuilder, NativeCapabilityRegistry capabilityRegistry) {
        this.typeMapper = typeMapper;
        this.functionMapper = functionMapper;
        LiteralConverter literalConverter = new LiteralConverter(typeMapper);
        this.windowConverter = new WindowFunctionConverter(
                typeMapper, functionMapper, literalConverter, rexBuilder);
        this.rexConverter = new ExpressionConverter(
                typeMapper, functionMapper, windowConverter, rexBuilder,
                literalConverter, aiInvocationHints::add);
        this.setConverter = new SetOperationConverter();
        this.aggregateConverter = new AggregateConverter(typeMapper, functionMapper);
        this.dmlConverter = new DmlOperationConverter(this::visit, singleMatchJoins::add);
        this.unnestConverter = new UnnestConverter(typeMapper, functionMapper, rexConverter, this::visit);
        this.correlateConverter = new CorrelateConverter(
                typeMapper, functionMapper, rexConverter, this::visit, unnestConverter);
        this.physicalJoinConverter = new PhysicalJoinConverter(rexConverter, this::visit);
        this.dispatch = Map.ofEntries(
                Map.entry(LogicalProject.class, n -> visitProject((LogicalProject) n)),
                Map.entry(LogicalFilter.class, n -> visitFilter((LogicalFilter) n)),
                Map.entry(LogicalValues.class, n -> visitValues((LogicalValues) n)),
                Map.entry(LogicalTableScan.class, n -> visitTableScan((LogicalTableScan) n)),
                Map.entry(LogicalSort.class, n -> visitSort((LogicalSort) n)),
                Map.entry(LogicalJoin.class, n -> visitJoin((LogicalJoin) n)),
                Map.entry(PangrovaSortMergeJoin.class,
                        n -> physicalJoinConverter.visitMergeJoin((PangrovaSortMergeJoin) n)),
                Map.entry(PangrovaHashJoin.class, n -> physicalJoinConverter.visitHashJoin((PangrovaHashJoin) n)),
                Map.entry(PangrovaBroadcastJoin.class,
                        n -> physicalJoinConverter.visitBroadcastJoin((PangrovaBroadcastJoin) n)),
                Map.entry(PangrovaNestedLoopJoin.class,
                        n -> physicalJoinConverter.visitNestedLoopJoin((PangrovaNestedLoopJoin) n)),
                Map.entry(PangrovaExchangeInput.class,
                        n -> visitExchangeInput((PangrovaExchangeInput) n)),
                Map.entry(LogicalAggregate.class,
                        n -> aggregateConverter.convert((LogicalAggregate) n,
                        visit(((LogicalAggregate) n).getInput()))),
                Map.entry(LogicalUnion.class, n -> setConverter.visitUnion((LogicalUnion) n, this::visit)),
                Map.entry(LogicalIntersect.class, n -> setConverter.visitIntersect((LogicalIntersect) n, this::visit)),
                Map.entry(LogicalMinus.class, n -> setConverter.visitMinus((LogicalMinus) n, this::visit)),
                Map.entry(LogicalWindow.class, n -> visitWindow((LogicalWindow) n)),
                Map.entry(LogicalCorrelate.class, n -> correlateConverter.convert((LogicalCorrelate) n)),
                Map.entry(Uncollect.class, n -> unnestConverter.convert((Uncollect) n)),
                Map.entry(LogicalTableModify.class, n -> dmlConverter.convert((LogicalTableModify) n)),
                Map.entry(PangrovaExchange.class, n -> physicalJoinConverter.visitExchange((PangrovaExchange) n)));
    }

    /** Dispatch a Calcite RelNode to the appropriate conversion method. */
    public Rel visit(RelNode relNode) {
        Function<RelNode, Rel> handler = dispatch.get(relNode.getClass());
        if (handler != null) return handler.apply(relNode);
        throw unsupported("RelNode: " + relNode.getClass().getSimpleName());
    }

    private Rel visitProject(LogicalProject project) {
        Rel input = visit(project.getInput());
        List<Expression> expressions = project.getProjects().stream()
                .map(rexConverter::convert)
                .toList();
        // Substrait Project appends expressions to every input field by
        // default. Calcite Project replaces its input schema, so explicitly
        // emit only the appended expression range. This also prevents hidden
        // AI binding literals from changing the SQL-visible output schema.
        return ImmutableProject.builder()
                .input(input)
                .expressions(expressions)
                .remap(Rel.Remap.offset(
                        input.getRecordType().fields().size(),
                        expressions.size()))
                .build();
    }

    private Rel visitFilter(LogicalFilter filter) {
        return io.substrait.relation.ImmutableFilter.builder()
                .input(visit(filter.getInput())).condition(rexConverter.convert(filter.getCondition())).build();
    }

    private Rel visitValues(LogicalValues values) {
        NamedStruct schema = buildNamedStruct(values.getRowType());
        var fields = values.getRowType().getFieldList();
        List<Expression.NestedStruct> rows = values.getTuples().stream()
                .map(t -> ExpressionCreator.nestedStruct(false,
                        java.util.stream.IntStream.range(0, t.size())
                                .mapToObj(i -> convertValuesField(
                                        t.get(i), fields.get(i).getType()))
                                .toList()))
                .toList();
        return ImmutableVirtualTableScan.builder()
                .initialSchema(schema)
                .addAllRows(rows)
                .build();
    }

    private Expression convertValuesField(RexLiteral literal, RelDataType fieldType) {
        Expression converted = rexConverter.convertValuesLiteral(literal, fieldType);
        var expectedType = typeMapper.map(fieldType);
        if (!converted.getType().equals(expectedType)) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "VALUES literal type " + converted.getType()
                            + " does not match declared field type " + expectedType);
        }
        return converted;
    }

    private Rel visitTableScan(LogicalTableScan scan) {
        NamedStruct ns = buildNamedStruct(scan.getRowType());
        List<String> qualifiedName = PlanTable.require(scan)
                .getQualifiedName();
        return ImmutableNamedScan.builder().names(qualifiedName)
                .initialSchema(ns).build();
    }

    private Rel visitExchangeInput(PangrovaExchangeInput input) {
        NamedStruct schema = buildNamedStruct(input.getRowType());
        return ExtensionLeaf.from(new ExchangeInputLeafDetail(input, schema))
                .build();
    }

    private Rel visitSort(LogicalSort sort) {
        if (sort.fetch != null && extractLong(sort.fetch) == 0) {
            NamedStruct schema = buildNamedStruct(sort.getRowType());
            return ImmutableVirtualTableScan.builder()
                    .initialSchema(schema)
                    .build();
        }
        Rel input = visit(sort.getInput());
        createRetrievalTopKHint(sort).ifPresent(retrievalTopKHints::add);
        input = applyCollation(input, sort);
        input = applyFetchOffset(input, sort);
        return input;
    }

    private Rel applyCollation(Rel input, LogicalSort sort) {
        if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
            return ImmutableSort.builder().input(input).sortFields(
                    sort.getCollation().getFieldCollations().stream().map(this::convertCollation).toList()).build();
        }
        return input;
    }

    private Rel applyFetchOffset(Rel input, LogicalSort sort) {
        if (sort.fetch == null && sort.offset == null) {
            return input;
        }
        long off = sort.offset != null ? extractLong(sort.offset) : 0;
        var b = ImmutableFetch.builder().input(input).offset(off);
        if (sort.fetch != null) b.count(extractLong(sort.fetch));
        return b.build();
    }

    private Rel visitJoin(LogicalJoin join) {
        Rel left = visit(join.getLeft());
        Rel right = visit(join.getRight());
        Join.JoinType joinType = singleMatchJoins.remove(join)
                ? Join.JoinType.LEFT_SINGLE : mapJoinType(join.getJoinType());

        // Non-equi (theta) joins: emit JoinRel with condition.
        // The native engine's convertJoinRel extracts equi-keys; when none are found,
        // it falls through to NestedLoopJoinNode with the condition as a filter.
        // Previously this path emitted CrossRel + post-processing injection, which
        // caused function_reference ID mismatch (the injected expression used IDs
        // from a separate ExtensionCollector that didn't match the plan's extensions).
        if (!join.getCondition().isAlwaysTrue() && isNonEquiJoin(join)) {
            var b = ImmutableJoin.builder().left(left).right(right)
                    .joinType(joinType)
                    .condition(rexConverter.convert(join.getCondition()));
            LOG.debug("event=emit_join_rel_non_equi joinType={}", join.getJoinType());
            return b.build();
        }

        var b = ImmutableJoin.builder().left(left).right(right)
                .joinType(joinType);
        if (!join.getCondition().isAlwaysTrue()) b.condition(rexConverter.convert(join.getCondition()));
        return b.build();
    }

    /**
     * Checks whether a join has no equi-join keys (pure non-equi/theta join).
     * Uses Calcite's JoinInfo to extract equality conditions between left and right inputs.
     * Returns true when no equi-join keys are found, meaning the condition is purely non-equi.
     */
    private static boolean isNonEquiJoin(LogicalJoin join) {
        JoinInfo info = join.analyzeCondition();
        return info.leftKeys.isEmpty();
    }

    /** Returns typed Top-K intents in post-order, matching serialized relation traversal. */
    List<RetrievalTopKPlanHint> getRetrievalTopKHints() {
        return List.copyOf(retrievalTopKHints);
    }

    /** Returns governed AI expression intents in deterministic conversion order. */
    List<AiInvocationPlanHint> getAiInvocationHints() {
        return List.copyOf(aiInvocationHints);
    }

    private java.util.Optional<RetrievalTopKPlanHint> createRetrievalTopKHint(
            LogicalSort sort) {
        if (sort.fetch == null || (sort.offset != null && extractLong(sort.offset) != 0)
                || sort.getCollation() == null
                || sort.getCollation().getFieldCollations().size() != 1) {
            return java.util.Optional.empty();
        }
        long limit = extractLong(sort.fetch);
        int scoreIndex = sort.getCollation().getFieldCollations().get(0).getFieldIndex();
        var fields = sort.getInput().getRowType().getFieldList();
        if (limit <= 0 || limit > 100_000 || scoreIndex < 0 || scoreIndex >= fields.size()
                || fields.get(scoreIndex).getType().getSqlTypeName() != SqlTypeName.DOUBLE
                || fields.stream().anyMatch(field -> !isScalarCandidateType(
                        field.getType().getSqlTypeName()))) {
            return java.util.Optional.empty();
        }
        boolean physicalIdentity = fields.size() >= 4 && sort.getInput().getRowType().getFieldNames()
                .subList(fields.size() - 4, fields.size()).equals(
                        org.pangrova.protocol.retrieval.RetrievalCandidateIdentity.PHYSICAL_FIELDS);
        List<Integer> stableKeys = java.util.stream.IntStream.range(
                        physicalIdentity ? fields.size() - 4 : 0, fields.size())
                .filter(index -> index != scoreIndex)
                .boxed()
                .toList();
        if (stableKeys.isEmpty()) {
            return java.util.Optional.empty();
        }
        var collation = sort.getCollation().getFieldCollations().get(0);
        var direction = collation.getDirection().isDescending()
                ? RetrievalTopKPlanHint.ScoreDirection.HIGHER_IS_BETTER
                : RetrievalTopKPlanHint.ScoreDirection.LOWER_IS_BETTER;
        byte[] schema = buildNamedStruct(sort.getInput().getRowType())
                .toProto(new TypeProtoConverter(new ExtensionCollector()))
                .toByteArray();
        RetrievalSemantic semantic = retrievalSemantic(sort, scoreIndex);
        return java.util.Optional.of(new RetrievalTopKPlanHint(
                semantic.kind(), semantic.sourceKey(), scoreIndex, (int) limit,
                direction, stableKeys, schema));
    }

    private static RetrievalSemantic retrievalSemantic(
            LogicalSort sort, int scoreIndex) {
        if (!(sort.getInput() instanceof LogicalProject project)
                || scoreIndex >= project.getProjects().size()
                || !(project.getProjects().get(scoreIndex) instanceof RexCall call)) {
            return RetrievalSemantic.EXACT;
        }
        String function = call.getOperator().getName()
                .toUpperCase(java.util.Locale.ROOT);
        RetrievalTopKPlanHint.Kind kind = switch (function) {
            case "L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT" ->
                    RetrievalTopKPlanHint.Kind.VECTOR;
            case "BM25_SCORE" -> RetrievalTopKPlanHint.Kind.FULL_TEXT;
            default -> RetrievalTopKPlanHint.Kind.EXACT;
        };
        if (kind == RetrievalTopKPlanHint.Kind.EXACT) {
            return RetrievalSemantic.EXACT;
        }
        String column = call.getOperands().stream()
                .map(PlanNodeConverter::castColumnReference)
                .filter(java.util.Objects::nonNull)
                .filter(ref -> ref.getIndex() < project.getInput().getRowType().getFieldCount())
                .map(ref -> project.getInput().getRowType().getFieldList().get(ref.getIndex()).getName())
                .findFirst()
                .orElse("");
        return column.isBlank()
                ? RetrievalSemantic.EXACT
                : new RetrievalSemantic(kind, kind.name() + ":" + column);
    }

    private static RexInputRef castColumnReference(RexNode expression) {
        RexNode current = expression;
        while (current instanceof RexCall cast && cast.getKind() == org.apache.calcite.sql.SqlKind.CAST
                && cast.getOperands().size() == 1) {
            current = cast.getOperands().get(0);
        }
        return current instanceof RexInputRef ref ? ref : null;
    }

    private record RetrievalSemantic(
            RetrievalTopKPlanHint.Kind kind, String sourceKey) {
        private static final RetrievalSemantic EXACT = new RetrievalSemantic(
                RetrievalTopKPlanHint.Kind.EXACT, "");
    }

    private static boolean isScalarCandidateType(SqlTypeName type) {
        return switch (type) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE,
                    DECIMAL, VARCHAR, CHAR, BOOLEAN, DATE, TIME, TIMESTAMP,
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE, BINARY, VARBINARY -> true;
            default -> false;
        };
    }

    private Rel visitWindow(LogicalWindow window) {
        Rel input = visit(window.getInput());
        List<Expression> projections = windowConverter.buildWindowProjections(window,
                window.getInput().getRowType().getFieldCount(),
                rexConverter::convert, this::convertCollation);
        // The projection already contains the input columns followed by the
        // window results. Substrait Project otherwise prepends its input a
        // second time, unlike Calcite LogicalWindow's output schema.
        var builder = ImmutableProject.builder()
                .input(input)
                .expressions(projections)
                .remap(Rel.Remap.offset(
                        input.getRecordType().fields().size(), projections.size()));

        return builder.build();
    }

    Expression.SortField convertCollation(RelFieldCollation c) {
        Expression ref = FieldReference.newRootStructReference(c.getFieldIndex(), TypeCreator.of(true).I32);
        Expression.SortDirection dir = SortDirectionMapper.map(c.getDirection(), c.nullDirection);
        return ImmutableExpression.SortField.builder().expr(ref).direction(dir).build();
    }

    private static final Map<JoinRelType, Join.JoinType> JOIN_TYPE_MAP = Map.of(
            JoinRelType.INNER, Join.JoinType.INNER, JoinRelType.LEFT, Join.JoinType.LEFT,
            JoinRelType.RIGHT, Join.JoinType.RIGHT, JoinRelType.FULL, Join.JoinType.OUTER,
            JoinRelType.SEMI, Join.JoinType.SEMI, JoinRelType.ANTI, Join.JoinType.ANTI);

    static Join.JoinType mapJoinType(JoinRelType t) {
        Join.JoinType mapped = JOIN_TYPE_MAP.get(t);
        if (mapped == null) {
            throw unsupported("JoinType: " + t);
        }
        return mapped;
    }

    private NamedStruct buildNamedStruct(RelDataType rowType) {
        // Substrait NamedStruct flattens nested struct field names: a top-level
        // ROW (info STRUCT(name, age, city)) emits names=[..., "info", "name",
        // "age", "city", ...] alongside a nested struct type. Without this
        // flattening, downstream native readers see auto-generated field names
        // like "col_0" and fail to match against the Parquet schema.
        List<String> names = SubstraitFieldNames.flatten(rowType, List.of());
        LOG.debug("event=build_named_struct names={}", names);
        return NamedStruct.of(names, TypeCreator.of(false).struct(
                rowType.getFieldList().stream().map(f -> typeMapper.map(f.getType())).toList()));
    }

    static long extractLong(RexNode node) {
        if (node instanceof RexLiteral lit) {
            Long value = lit.getValueAs(Long.class);
            if (value == null) {
                throw unsupported("NULL literal in LIMIT/OFFSET");
            }
            return value;
        }
        throw unsupported("non-literal in LIMIT/OFFSET: " + node.getClass().getSimpleName());
    }

    private static SubstraitConversionException unsupported(String detail) {
        return new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE, "Unsupported "
                + detail);
    }
}
