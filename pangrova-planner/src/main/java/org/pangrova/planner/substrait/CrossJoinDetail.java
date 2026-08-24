package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.relation.Join;

/**
 * Encodes non-equi join metadata for CrossRel emission.
 *
 * <p>When a join condition contains no equi-join keys (pure theta join),
 * the planner emits a CrossRel instead of a JoinRel. Since the substrait-java
 * SDK's Cross class does not expose expression or type fields, this detail
 * carries the condition and join type through the conversion pipeline. The
 * serializer injects these into the proto CrossRel's expression (field 4)
 * and type (field 5) during post-processing.</p>
 *
 * @param condition the full join condition expression (non-equi predicate)
 * @param joinType the join type (INNER, LEFT, RIGHT, OUTER)
 */
record CrossJoinDetail(Expression condition, Join.JoinType joinType) {

    /** Marker key used in AdvancedExtension to identify non-equi join CrossRels. */
    static final String MARKER = "pangrova:non_equi_join";
}
