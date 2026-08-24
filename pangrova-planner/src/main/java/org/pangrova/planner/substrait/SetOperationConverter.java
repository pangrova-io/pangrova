package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Aggregate;
import io.substrait.relation.ImmutableAggregate;
import io.substrait.relation.ImmutableGrouping;
import io.substrait.relation.ImmutableSet;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import io.substrait.type.Type;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Converts Calcite set operations (Union, Intersect, Minus/Except) to Substrait Set Rel.
 *
 * <p>For DISTINCT variants (UNION, INTERSECT, EXCEPT without ALL), the native engine's
 * SetRel implementation may use join-based strategies that preserve duplicates. To ensure
 * correct set semantics, DISTINCT variants are wrapped with a deduplication aggregate
 * (GROUP BY all columns, no measures) at the Substrait level — unless the native engine
 * reports support for distinct set ops via
 * {@link NativeCapabilityRegistry#supportsDistinctSetOps()}.</p>
 */
final class SetOperationConverter {

    private static final Logger LOG = LoggerFactory.getLogger(SetOperationConverter.class);

    private final TypeMapper typeMapper;
    private final NativeCapabilityRegistry capabilityRegistry;

    SetOperationConverter(TypeMapper typeMapper) {
        this(typeMapper, null);
    }

    /**
     * Creates a converter with optional native capability awareness.
     *
     * @param typeMapper          type mapping from Calcite to Substrait
     * @param capabilityRegistry  native engine capabilities (nullable; when non-null,
     *                            distinct set ops skip dedup if engine supports them natively)
     */
    SetOperationConverter(TypeMapper typeMapper, NativeCapabilityRegistry capabilityRegistry) {
        this.typeMapper = typeMapper;
        this.capabilityRegistry = capabilityRegistry;
    }

    Rel visitUnion(LogicalUnion union, Function<RelNode, Rel> visitFn) {
        List<Rel> inputs = union.getInputs().stream()
                .map(visitFn)
                .toList();
        if (union.all) {
            return ImmutableSet.builder()
                    .inputs(inputs)
                    .setOp(Set.SetOp.UNION_ALL)
                    .build();
        }
        // UNION DISTINCT: emit UNION_ALL + dedup aggregate unless native engine
        // handles distinct natively.
        Rel unionAll = ImmutableSet.builder()
                .inputs(inputs)
                .setOp(Set.SetOp.UNION_ALL)
                .build();
        return wrapWithDedupIfNeeded(unionAll, union.getRowType(), "UNION_DISTINCT");
    }

    Rel visitIntersect(LogicalIntersect intersect, Function<RelNode, Rel> visitFn) {
        List<Rel> inputs = intersect.getInputs().stream()
                .map(visitFn)
                .toList();
        Rel setRel = ImmutableSet.builder()
                .inputs(inputs)
                .setOp(intersect.all
                        ? Set.SetOp.INTERSECTION_MULTISET
                        : Set.SetOp.INTERSECTION_PRIMARY)
                .build();
        if (intersect.all) {
            return setRel;
        }
        return wrapWithDedupIfNeeded(setRel, intersect.getRowType(), "INTERSECT_DISTINCT");
    }

    Rel visitMinus(LogicalMinus minus, Function<RelNode, Rel> visitFn) {
        List<Rel> inputs = minus.getInputs().stream()
                .map(visitFn)
                .toList();
        Rel setRel = ImmutableSet.builder()
                .inputs(inputs)
                .setOp(minus.all
                        ? Set.SetOp.MINUS_MULTISET
                        : Set.SetOp.MINUS_PRIMARY)
                .build();
        if (minus.all) {
            return setRel;
        }
        return wrapWithDedupIfNeeded(setRel, minus.getRowType(), "EXCEPT_DISTINCT");
    }

    /**
     * Conditionally wraps a Rel with a deduplication aggregate.
     * Skips the wrap if the native engine supports distinct set ops natively,
     * avoiding double-deduplication overhead.
     */
    private Rel wrapWithDedupIfNeeded(Rel input, RelDataType rowType, String opName) {
        if (capabilityRegistry != null && capabilityRegistry.supportsDistinctSetOps()) {
            LOG.debug("event=skip_dedup_aggregate op={} reason=native_engine_supports_distinct", opName);
            return input;
        }
        return wrapWithDedup(input, rowType);
    }

    /**
     * Wraps a Rel with a deduplication aggregate (GROUP BY all columns, no measures).
     * This is equivalent to SELECT DISTINCT * and ensures set semantics for
     * DISTINCT set operations.
     */
    private Rel wrapWithDedup(Rel input, RelDataType rowType) {
        int fieldCount = rowType.getFieldCount();
        List<Expression> groupExprs = IntStream.range(0, fieldCount)
                .mapToObj(i -> {
                    Type type = typeMapper.map(rowType.getFieldList().get(i).getType());
                    return (Expression) FieldReference.newRootStructReference(i, type);
                })
                .toList();
        Aggregate.Grouping grouping = ImmutableGrouping.builder()
                .expressions(groupExprs)
                .build();
        return ImmutableAggregate.builder()
                .input(input)
                .addGroupings(grouping)
                .build();
    }
}
