/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.parser.ast.expression.NotExpression;
import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;

import java.util.List;

/**
 * Dedicated row-level expression evaluator used by the MERGE coordinator-side
 * fast-path (when no native dispatch is configured). Centralises predicate /
 * value computation in one place so the dispatch layer ({@code PlanExecutionFactory})
 * never re-implements SQL semantics inline. New operators or types are added
 * here, not scattered across dispatchers — preventing semantic drift between
 * the coordinator fast-path and the canonical native engine.
 *
 * <p>Scope is intentionally narrow:</p>
 * <ul>
 *   <li>Boolean predicates over {@link BinaryExpression} (AND / OR / EQ / NEQ / GT / GTE / LT / LTE)</li>
 *   <li>Negation via {@link NotExpression}</li>
 *   <li>Identifier lookup against a target / source row (qualified or unqualified)</li>
 *   <li>Numeric arithmetic (+ - * /) on {@link Number} operands</li>
 *   <li>Literal extraction</li>
 * </ul>
 *
 * <p>Anything outside this set is intentionally rejected by returning
 * {@code false} (predicate) or {@code null} (value), matching the previous
 * inline behaviour exactly. Extending the evaluator should be done by adding
 * a new case here so that all dispatchers see the same semantics.</p>
 *
 * <p>Utility class — not instantiable.</p>
 */
final class MergeExpressionEvaluator {

    private MergeExpressionEvaluator() {
        throw new AssertionError("Utility class");
    }

    /**
     * Evaluates a boolean predicate against a (target, source) row pair.
     *
     * @param expr         the predicate expression to evaluate
     * @param targetRow    target row, may be {@code null} for NOT MATCHED clauses
     * @param sourceRow    source row, may be {@code null}
     * @param targetAlias  alias used for target qualified identifiers
     * @param sourceAlias  alias used for source qualified identifiers
     * @return whether the predicate evaluates to {@code true}
     */
    static boolean evaluatePredicate(Expression expr,
                                     TypedRow targetRow, TypedRow sourceRow,
                                     String targetAlias, String sourceAlias) {
        if (expr instanceof BinaryExpression bin) {
            switch (bin.operator()) {
                case AND -> {
                    return evaluatePredicate(bin.left(), targetRow, sourceRow, targetAlias, sourceAlias)
                            && evaluatePredicate(bin.right(), targetRow, sourceRow, targetAlias, sourceAlias);
                }
                case OR -> {
                    return evaluatePredicate(bin.left(), targetRow, sourceRow, targetAlias, sourceAlias)
                            || evaluatePredicate(bin.right(), targetRow, sourceRow, targetAlias, sourceAlias);
                }
                case EQUAL -> {
                    return compareBinary(bin, targetRow, sourceRow, targetAlias, sourceAlias) == 0;
                }
                case NOT_EQUAL -> {
                    return compareBinary(bin, targetRow, sourceRow, targetAlias, sourceAlias) != 0;
                }
                case GREATER_THAN -> {
                    return compareBinary(bin, targetRow, sourceRow, targetAlias, sourceAlias) > 0;
                }
                case GREATER_THAN_OR_EQUAL -> {
                    return compareBinary(bin, targetRow, sourceRow, targetAlias, sourceAlias) >= 0;
                }
                case LESS_THAN -> {
                    return compareBinary(bin, targetRow, sourceRow, targetAlias, sourceAlias) < 0;
                }
                case LESS_THAN_OR_EQUAL -> {
                    return compareBinary(bin, targetRow, sourceRow, targetAlias, sourceAlias) <= 0;
                }
                default -> {
                    return false;
                }
            }
        }
        if (expr instanceof NotExpression not) {
            return !evaluatePredicate(not.expression(), targetRow, sourceRow, targetAlias, sourceAlias);
        }
        return false;
    }

    /**
     * Resolves an expression to its concrete value, returning {@code null} when
     * the expression cannot be reduced to a literal in this evaluator.
     */
    static Object resolveValue(Expression expr,
                               TypedRow targetRow, TypedRow sourceRow,
                               String targetAlias, String sourceAlias) {
        if (expr instanceof Literal lit) {
            return extractLiteralRaw(lit);
        }
        if (expr instanceof Identifier id) {
            return resolveIdentifier(id, targetRow, sourceRow, targetAlias, sourceAlias);
        }
        if (expr instanceof BinaryExpression bin) {
            Object left = resolveValue(bin.left(), targetRow, sourceRow, targetAlias, sourceAlias);
            Object right = resolveValue(bin.right(), targetRow, sourceRow, targetAlias, sourceAlias);
            if (left instanceof Number l && right instanceof Number r) {
                return switch (bin.operator()) {
                    case ADD -> l.doubleValue() + r.doubleValue();
                    case SUBTRACT -> l.doubleValue() - r.doubleValue();
                    case MULTIPLY -> l.doubleValue() * r.doubleValue();
                    case DIVIDE -> r.doubleValue() != 0 ? l.doubleValue() / r.doubleValue() : null;
                    default -> null;
                };
            }
        }
        return null;
    }

    private static Object resolveIdentifier(Identifier id,
                                            TypedRow targetRow, TypedRow sourceRow,
                                            String targetAlias, String sourceAlias) {
        List<String> parts = id.parts();
        if (parts.size() == 2) {
            String qualifier = parts.get(0);
            String column = parts.get(1);
            if (qualifier.equalsIgnoreCase(targetAlias) && targetRow != null) {
                return ColumnValue.toRaw(targetRow.get(column));
            }
            if (qualifier.equalsIgnoreCase(sourceAlias) && sourceRow != null) {
                return ColumnValue.toRaw(sourceRow.get(column));
            }
        } else if (parts.size() == 1) {
            String column = parts.get(0);
            if (targetRow != null && targetRow.get(column) != null) {
                return ColumnValue.toRaw(targetRow.get(column));
            }
            if (sourceRow != null && sourceRow.get(column) != null) {
                return ColumnValue.toRaw(sourceRow.get(column));
            }
        }
        return null;
    }

    private static int compareBinary(BinaryExpression bin,
                                     TypedRow targetRow, TypedRow sourceRow,
                                     String targetAlias, String sourceAlias) {
        Object left = resolveValue(bin.left(), targetRow, sourceRow, targetAlias, sourceAlias);
        Object right = resolveValue(bin.right(), targetRow, sourceRow, targetAlias, sourceAlias);
        return compareValues(left, right);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Number l && right instanceof Number r) {
            return Double.compare(l.doubleValue(), r.doubleValue());
        }
        if (left instanceof Comparable c) {
            return c.compareTo(right);
        }
        return left.toString().compareTo(right.toString());
    }

    private static Object extractLiteralRaw(Literal lit) {
        LiteralValue lv = lit.value();
        if (lv instanceof LiteralValue.IntegerValue iv) {
            return iv.value();
        }
        if (lv instanceof LiteralValue.StringValue sv) {
            return sv.value();
        }
        if (lv instanceof LiteralValue.DecimalValue dv) {
            return dv.value().doubleValue();
        }
        if (lv instanceof LiteralValue.BooleanValue bv) {
            return bv.value();
        }
        return null;
    }
}
