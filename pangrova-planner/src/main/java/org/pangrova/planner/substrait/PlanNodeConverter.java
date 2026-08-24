package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableExpression;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.ImmutableAdvancedExtension;
import io.substrait.relation.ImmutableFetch;
import io.substrait.relation.ImmutableJoin;
import io.substrait.relation.ImmutableNamedScan;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.ImmutableSort;
import io.substrait.relation.ImmutableVirtualTableScan;
import io.substrait.relation.Join;
import io.substrait.relation.Rel;
import io.substrait.type.NamedStruct;
import io.substrait.type.TypeCreator;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.PangrovaNestedLoopJoin;
import org.pangrova.planner.cost.join.PangrovaSortMergeJoin;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Window;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private final PhysicalJoinConverter physicalJoinConverter;
    private final FunctionMapper functionMapper;
    private final Map<Class<? extends RelNode>, Function<RelNode, Rel>> dispatch;
    private final List<CrossJoinDetail> crossJoinDetails = new ArrayList<>();

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
        this.windowConverter = new WindowFunctionConverter(typeMapper, functionMapper);
        this.rexConverter = new ExpressionConverter(typeMapper, functionMapper, windowConverter, rexBuilder);
        this.setConverter = new SetOperationConverter(typeMapper, capabilityRegistry);
        this.aggregateConverter = new AggregateConverter(typeMapper, functionMapper);
        this.dmlConverter = new DmlOperationConverter(this::visit);
        this.correlateConverter = new CorrelateConverter(
                typeMapper, functionMapper, rexConverter, this::visit);
        this.physicalJoinConverter = new PhysicalJoinConverter(typeMapper, this::visit);
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
                Map.entry(LogicalAggregate.class,
                        n -> aggregateConverter.convert((LogicalAggregate) n,
                        visit(((LogicalAggregate) n).getInput()))),
                Map.entry(LogicalUnion.class, n -> setConverter.visitUnion((LogicalUnion) n, this::visit)),
                Map.entry(LogicalIntersect.class, n -> setConverter.visitIntersect((LogicalIntersect) n, this::visit)),
                Map.entry(LogicalMinus.class, n -> setConverter.visitMinus((LogicalMinus) n, this::visit)),
                Map.entry(LogicalWindow.class, n -> visitWindow((LogicalWindow) n)),
                Map.entry(LogicalCorrelate.class, n -> correlateConverter.convert((LogicalCorrelate) n)),
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
        return ImmutableProject.builder().input(input)
                .expressions(project.getProjects().stream().map(rexConverter::convert).toList()).build();
    }

    private Rel visitFilter(LogicalFilter filter) {
        return io.substrait.relation.ImmutableFilter.builder()
                .input(visit(filter.getInput())).condition(rexConverter.convert(filter.getCondition())).build();
    }

    private Rel visitValues(LogicalValues values) {
        NamedStruct schema = buildNamedStruct(values.getRowType());
        List<Expression.StructLiteral> rows = values.getTuples().stream()
                .map(t -> ExpressionCreator.struct(false,
                        t.stream().map(l -> (Expression.Literal) rexConverter.convert(l)).toList()))
                .toList();
        return ImmutableVirtualTableScan.builder().dfsNames(schema.names()).addAllRows(rows).build();
    }

    private Rel visitTableScan(LogicalTableScan scan) {
        NamedStruct ns = buildNamedStruct(scan.getRowType());
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        return ImmutableNamedScan.builder().names(qualifiedName)
                .initialSchema(ns).build();
    }

    private Rel visitSort(LogicalSort sort) {
        if (sort.fetch != null && extractLong(sort.fetch) == 0) {
            NamedStruct schema = buildNamedStruct(sort.getRowType());
            return ImmutableVirtualTableScan.builder().dfsNames(schema.names()).build();
        }
        Rel input = visit(sort.getInput());
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

        // Non-equi (theta) joins: emit JoinRel with condition.
        // The native engine's convertJoinRel extracts equi-keys; when none are found,
        // it falls through to NestedLoopJoinNode with the condition as a filter.
        // Previously this path emitted CrossRel + post-processing injection, which
        // caused function_reference ID mismatch (the injected expression used IDs
        // from a separate ExtensionCollector that didn't match the plan's extensions).
        if (!join.getCondition().isAlwaysTrue() && isNonEquiJoin(join)) {
            var b = ImmutableJoin.builder().left(left).right(right)
                    .joinType(mapJoinType(join.getJoinType()))
                    .condition(rexConverter.convert(join.getCondition()));
            LOG.debug("event=emit_join_rel_non_equi joinType={}", join.getJoinType());
            return b.build();
        }

        var b = ImmutableJoin.builder().left(left).right(right)
                .joinType(mapJoinType(join.getJoinType()));
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

    /** Returns accumulated non-equi join details for proto post-processing. */
    List<CrossJoinDetail> getCrossJoinDetails() {
        return crossJoinDetails;
    }

    private Rel visitWindow(LogicalWindow window) {
        Rel input = visit(window.getInput());
        var builder = ImmutableProject.builder().input(input).expressions(
                windowConverter.buildWindowProjections(window,
                        window.getInput().getRowType().getFieldCount(),
                        rexConverter::convert, this::convertCollation));

        WindowOptimizationHint hint = detectWindowOptimizationHint(window);
        if (hint != null) {
            AdvancedExtension ext = ImmutableAdvancedExtension.builder()
                    .optimization(hint)
                    .build();
            builder.extension(ext);
            LOG.debug("event=window_optimization_hint eligible_row_number={} eligible_topn={} topn_limit={}",
                    hint.eligibleForRowNumberNode(), hint.eligibleForTopNRowNumberNode(), hint.topnLimit());
        }

        return builder.build();
    }

    /**
     * Detects whether a LogicalWindow is eligible for RowNumberNode or
     * TopNRowNumberNode optimization in the native engine.
     *
     * <p>Eligibility for RowNumberNode requires:
     * <ul>
     *   <li>Exactly one window group with exactly one aggregate call</li>
     *   <li>The function is row_number</li>
     *   <li>Partition keys are present</li>
     *   <li>Default frame bounds (ROWS UNBOUNDED PRECEDING to CURRENT ROW)</li>
     * </ul>
     *
     * <p>TopNRowNumberNode additionally requires ORDER BY keys in the group.
     * The actual limit K is detected by the native engine from the subsequent
     * FilterRel (lookahead not available at this conversion stage).</p>
     *
     * @return optimization hint, or null if the pattern is not eligible
     */
    private static WindowOptimizationHint detectWindowOptimizationHint(LogicalWindow window) {
        if (window.groups.size() != 1) {
            return null;
        }
        Window.Group group = window.groups.get(0);
        if (group.aggCalls.size() != 1) {
            return null;
        }
        Window.RexWinAggCall aggCall = group.aggCalls.get(0);
        String fnName = aggCall.getOperator().getName().toUpperCase();
        if (!"ROW_NUMBER".equals(fnName)) {
            return null;
        }
        if (group.keys.isEmpty()) {
            return null;
        }
        if (!isDefaultWindowFrame(group)) {
            return null;
        }

        boolean hasOrderBy = !group.orderKeys.getFieldCollations().isEmpty();
        boolean eligibleForTopN = hasOrderBy;

        return new WindowOptimizationHint(true, eligibleForTopN, 0);
    }

    /**
     * Checks whether the window group uses the default frame bounds
     * (ROWS UNBOUNDED PRECEDING to CURRENT ROW), which is the implicit
     * frame for row_number().
     */
    private static boolean isDefaultWindowFrame(Window.Group group) {
        return group.lowerBound.isUnbounded() && group.lowerBound.isPreceding()
                && group.upperBound.isCurrentRow();
    }

    Expression.SortField convertCollation(RelFieldCollation c) {
        Expression ref = FieldReference.newRootStructReference(c.getFieldIndex(), TypeCreator.of(true).I32);
        boolean desc = c.getDirection().isDescending();
        boolean nf = c.nullDirection == RelFieldCollation.NullDirection.FIRST;
        Expression.SortDirection dir = desc
                ? (nf ? Expression.SortDirection.DESC_NULLS_FIRST : Expression.SortDirection.DESC_NULLS_LAST)
                : (nf ? Expression.SortDirection.ASC_NULLS_FIRST : Expression.SortDirection.ASC_NULLS_LAST);
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
        List<String> names = new java.util.ArrayList<>();
        for (RelDataTypeField f : rowType.getFieldList()) {
            collectFlatFieldNames(f.getName(), f.getType(), names);
        }
        LOG.debug("event=build_named_struct names={}", names);
        return NamedStruct.of(names, TypeCreator.of(false).struct(
                rowType.getFieldList().stream().map(f -> typeMapper.map(f.getType())).toList()));
    }

    private static void collectFlatFieldNames(String name, RelDataType type, List<String> out) {
        out.add(name);
        recurseFieldNames(type, out);
    }

    private static void recurseFieldNames(RelDataType type, List<String> out) {
        if (type.isStruct()) {
            for (RelDataTypeField nested : type.getFieldList()) {
                collectFlatFieldNames(nested.getName(), nested.getType(), out);
            }
        } else if (type.getSqlTypeName() == SqlTypeName.ARRAY) {
            recurseFieldNames(type.getComponentType(), out);
        } else if (type.getSqlTypeName() == SqlTypeName.MAP) {
            recurseFieldNames(type.getKeyType(), out);
            recurseFieldNames(type.getValueType(), out);
        }
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
