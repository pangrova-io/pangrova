package org.pangrova.planner.core.substrait;

import io.substrait.relation.ImmutableSet;
import io.substrait.relation.Rel;
import io.substrait.relation.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.function.Function;

/** Emits the canonical set algebra; Native executes the exact DISTINCT/ALL contract. */
final class SetOperationConverter {
    Rel visitUnion(LogicalUnion union, Function<RelNode, Rel> visit) {
        return ImmutableSet.builder().inputs(union.getInputs().stream().map(visit).toList())
                .setOp(union.all ? Set.SetOp.UNION_ALL : Set.SetOp.UNION_DISTINCT).build();
    }

    Rel visitIntersect(LogicalIntersect intersect, Function<RelNode, Rel> visit) {
        return ImmutableSet.builder().inputs(intersect.getInputs().stream().map(visit).toList())
                .setOp(intersect.all ? Set.SetOp.INTERSECTION_MULTISET_ALL
                        : Set.SetOp.INTERSECTION_MULTISET).build();
    }

    Rel visitMinus(LogicalMinus minus, Function<RelNode, Rel> visit) {
        return ImmutableSet.builder().inputs(minus.getInputs().stream().map(visit).toList())
                .setOp(minus.all ? Set.SetOp.MINUS_PRIMARY_ALL : Set.SetOp.MINUS_PRIMARY).build();
    }
}
