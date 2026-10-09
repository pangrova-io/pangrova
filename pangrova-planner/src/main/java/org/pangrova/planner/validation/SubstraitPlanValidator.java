package org.pangrova.planner.validation;

import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Filter;
import io.substrait.relation.ExtensionLeaf;
import io.substrait.relation.Join;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;
import io.substrait.relation.physical.HashJoin;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.relation.physical.NestedLoopJoin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.pangrova.planner.exchange.ExchangeInputLeafDetail;

/**
 * Validates a Substrait {@link Plan} against the native engine's capability
 * registry. Recursively traverses the Rel tree and Expression tree, collecting
 * any unsupported nodes.
 *
 * <p>The validator is stateless and safe to reuse across multiple plans.</p>
 */
public final class SubstraitPlanValidator {

    private final NativeCapabilityRegistry registry;

    /**
     * Maps Substrait SDK Rel class simple names to the registry's canonical
     * Rel type names (aligned with NativePlanConverter naming).
     */
    private static final Map<String, String> REL_TYPE_NAMES = Map.ofEntries(
            Map.entry("NamedScan", "ReadRel"),
            Map.entry("LocalFiles", "ReadRel"),
            Map.entry("VirtualTableScan", "ReadRel"),
            Map.entry("EmptyScan", "ReadRel"),
            Map.entry("Filter", "FilterRel"),
            Map.entry("Project", "ProjectRel"),
            Map.entry("Aggregate", "AggregateRel"),
            Map.entry("Join", "JoinRel"),
            Map.entry("Sort", "SortRel"),
            Map.entry("Fetch", "FetchRel"),
            Map.entry("Set", "SetRel"),
            Map.entry("ScatterExchange", "ExchangeRel"),
            Map.entry("BroadcastExchange", "ExchangeRel"),
            Map.entry("RoundRobinExchange", "ExchangeRel"),
            Map.entry("ExtensionLeaf", "ExtensionLeafRel"),
            Map.entry("Cross", "JoinRel"),
            Map.entry("ExtensionMulti", "JoinRel")
    );

    /** Physical join capabilities follow the SDK relation contract, not implementation names. */
    private static final Map<Class<? extends Rel>, String> PHYSICAL_JOIN_TYPE_NAMES = Map.of(
            HashJoin.class, "HashJoinRel",
            MergeJoin.class, "MergeJoinRel",
            NestedLoopJoin.class, "NestedLoopJoinRel");

    /**
     * Maps Substrait SDK Expression class simple names to the registry's
     * canonical Expression type names.
     */
    private static final Map<String, String> EXPR_TYPE_NAMES = Map.ofEntries(
            Map.entry("FieldReference", "FieldReference"),
            Map.entry("ScalarFunctionInvocation", "ScalarFunction"),
            Map.entry("WindowFunctionInvocation", "WindowFunction"),
            Map.entry("Cast", "Cast"),
            Map.entry("IfThen", "IfThen"),
            Map.entry("NestedList", "Nested"),
            Map.entry("NestedStruct", "Nested")
    );

    public SubstraitPlanValidator(NativeCapabilityRegistry registry) {
        this.registry = registry;
    }

    /**
     * Validates every root in the plan and returns an aggregate result.
     */
    public PlanValidationResult validate(Plan plan) {
        List<UnsupportedNode> unsupported = new ArrayList<>();
        int[] relCount = {0};
        for (Plan.Root root : plan.getRoots()) {
            validateRel(root.getInput(), unsupported, 0, relCount);
        }
        return buildResult(unsupported, relCount[0]);
    }

    // ---- Rel tree traversal ------------------------------------------------

    private void validateRel(Rel rel, List<UnsupportedNode> unsupported,
                             int depth, int[] relCount) {
        relCount[0]++;
        String className = stripImmutablePrefix(rel.getClass().getSimpleName());

        String registryName = PHYSICAL_JOIN_TYPE_NAMES.entrySet().stream()
                .filter(entry -> entry.getKey().isInstance(rel))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseGet(() -> REL_TYPE_NAMES.get(className));
        boolean unsupportedDetail = rel instanceof ExtensionLeaf leaf
                && !(leaf.getDetail() instanceof ExchangeInputLeafDetail);
        if (registryName == null || !registry.isRelSupported(registryName)
                || unsupportedDetail) {
            unsupported.add(new UnsupportedNode(
                    registryName != null ? registryName : className,
                    unsupportedDetail
                            ? "Extension leaf detail not supported by native engine"
                            : "Rel type not supported by native engine",
                    depth, UnsupportedNode.Kind.RELATION));
        }

        // Validate expressions embedded in this Rel.
        collectExpressions(rel).forEach(
                expr -> validateExpression(expr, unsupported, depth));

        // Recurse into child Rels.
        for (Rel child : rel.getInputs()) {
            validateRel(child, unsupported, depth + 1, relCount);
        }
    }

    // ---- Expression tree traversal -----------------------------------------

    private void validateExpression(Expression expr,
                                    List<UnsupportedNode> unsupported, int depth) {
        // Literals are universally supported — skip validation.
        if (expr instanceof Expression.Literal) {
            return;
        }

        String className = resolveExprClassName(expr);
        String registryName = EXPR_TYPE_NAMES.get(className);
        if (registryName == null || !registry.isExprSupported(registryName)) {
            unsupported.add(new UnsupportedNode(
                    registryName != null ? registryName : className,
                    "Expression type not supported by native engine",
                    depth, UnsupportedNode.Kind.EXPRESSION));
        }

        // Function-level validation: check if the specific function is supported.
        validateFunctionIfApplicable(expr, unsupported, depth);

        // Recurse into child expressions.
        collectChildExpressions(expr).forEach(
                child -> validateExpression(child, unsupported, depth));
    }

    /**
     * Validates function-level support for ScalarFunction and WindowFunction
     * invocations against the native capability registry.
     */
    private void validateFunctionIfApplicable(Expression expr,
                                               List<UnsupportedNode> unsupported, int depth) {
        String functionName = extractFunctionName(expr);
        if (functionName == null || functionName.isEmpty()) {
            return;
        }
        if (!registry.isFunctionSupported(functionName)) {
            unsupported.add(new UnsupportedNode(
                    functionName,
                    "Function not supported by native engine",
                    depth, UnsupportedNode.Kind.FUNCTION));
        }
    }

    /**
     * Extracts the function name from a ScalarFunctionInvocation or
     * WindowFunctionInvocation. Returns null for non-function expressions.
     */
    private static String extractFunctionName(Expression expr) {
        if (expr instanceof Expression.ScalarFunctionInvocation sf) {
            return sf.declaration() != null ? sf.declaration().name() : null;
        }
        if (expr instanceof Expression.WindowFunctionInvocation wf) {
            return wf.declaration() != null ? wf.declaration().name() : null;
        }
        return null;
    }

    // ---- Helpers ------------------------------------------------------------

    /**
     * Extracts top-level expressions from a Rel node (e.g. Filter condition,
     * Project expressions, Join condition, Sort fields, Aggregate groupings).
     */
    private static List<Expression> collectExpressions(Rel rel) {
        List<Expression> result = new ArrayList<>();
        collectFromRel(rel, result);
        return result;
    }

    /**
     * Dispatch table mapping Rel types to their expression extractors.
     * Eliminates if-else chain in collectFromRel (governance: max 3 branches).
     */
    private static final Map<Class<? extends Rel>, BiConsumer<Rel, List<Expression>>>
            REL_EXPR_EXTRACTORS = Map.of(
            Filter.class, (r, out) -> out.add(((Filter) r).getCondition()),
            Project.class, (r, out) -> out.addAll(((Project) r).getExpressions()),
            Join.class, (r, out) -> collectJoinExpressions((Join) r, out),
            HashJoin.class, (r, out) -> collectHashJoinExpressions((HashJoin) r, out),
            MergeJoin.class, (r, out) -> collectMergeJoinExpressions((MergeJoin) r, out),
            NestedLoopJoin.class, (r, out) -> out.add(((NestedLoopJoin) r).getCondition()),
            Sort.class, (r, out) -> ((Sort) r).getSortFields().forEach(sf -> out.add(sf.expr())),
            Aggregate.class, (r, out) -> collectAggregateExpressions((Aggregate) r, out)
    );

    private static void collectFromRel(Rel rel, List<Expression> result) {
        REL_EXPR_EXTRACTORS.entrySet().stream()
                .filter(e -> e.getKey().isInstance(rel))
                .findFirst()
                .ifPresent(e -> e.getValue().accept(rel, result));
    }

    private static void collectJoinExpressions(Join j, List<Expression> result) {
        j.getCondition().ifPresent(result::add);
        j.getPostJoinFilter().ifPresent(result::add);
    }

    private static void collectHashJoinExpressions(HashJoin hj, List<Expression> result) {
        hj.getPostJoinFilter().ifPresent(result::add);
        result.addAll(hj.getLeftKeys());
        result.addAll(hj.getRightKeys());
    }

    private static void collectMergeJoinExpressions(MergeJoin join, List<Expression> result) {
        join.getPostJoinFilter().ifPresent(result::add);
        join.getKeys().forEach(key -> {
            result.add(key.getLeft());
            result.add(key.getRight());
        });
    }

    private static void collectAggregateExpressions(Aggregate a, List<Expression> result) {
        a.getGroupings().forEach(g -> result.addAll(g.getExpressions()));
        a.getMeasures().forEach(m -> m.getPreMeasureFilter().ifPresent(result::add));
    }

    /**
     * Dispatch list mapping Expression types to their child extractors.
     * Eliminates if-else chain in collectChildExpressions (governance: max 3 branches).
     */
    private static final List<ExprChildExtractor> EXPR_CHILD_EXTRACTORS = List.of(
            new ExprChildExtractor(Expression.Cast.class,
                    (e, out) -> out.add(((Expression.Cast) e).input())),
            new ExprChildExtractor(Expression.IfThen.class,
                    (e, out) -> collectIfThenChildren((Expression.IfThen) e, out)),
            new ExprChildExtractor(Expression.NestedList.class,
                    (e, out) -> out.addAll(((Expression.NestedList) e).values())),
            new ExprChildExtractor(Expression.NestedStruct.class,
                    (e, out) -> out.addAll(((Expression.NestedStruct) e).fields())),
            new ExprChildExtractor(Expression.ScalarFunctionInvocation.class,
                    (e, out) -> collectFunctionArgChildren(
                            ((Expression.ScalarFunctionInvocation) e).arguments(), out)),
            new ExprChildExtractor(Expression.WindowFunctionInvocation.class,
                    (e, out) -> collectFunctionArgChildren(
                            ((Expression.WindowFunctionInvocation) e).arguments(), out))
    );

    private record ExprChildExtractor(
            Class<? extends Expression> type,
            BiConsumer<Expression, List<Expression>> extractor) {
    }

    /**
     * Extracts child expressions from a parent expression for recursive
     * traversal (e.g. Cast input, IfThen clauses, ScalarFunction arguments).
     */
    private static List<Expression> collectChildExpressions(Expression expr) {
        List<Expression> children = new ArrayList<>();
        EXPR_CHILD_EXTRACTORS.stream()
                .filter(e -> e.type().isInstance(expr))
                .findFirst()
                .ifPresent(e -> e.extractor().accept(expr, children));
        return children;
    }

    private static void collectIfThenChildren(Expression.IfThen ifThen, List<Expression> out) {
        ifThen.ifClauses().forEach(clause -> {
            out.add(clause.condition());
            out.add(clause.then());
        });
        out.add(ifThen.elseClause());
    }

    private static void collectFunctionArgChildren(List<?> arguments, List<Expression> out) {
        arguments.forEach(arg -> {
            if (arg instanceof Expression argExpr) {
                out.add(argExpr);
            }
        });
    }

    /**
     * Resolves the canonical class name for an Expression, stripping the
     * Immutables-generated prefix and the outer-class qualifier.
     */
    private static String resolveExprClassName(Expression expr) {
        if (expr instanceof FieldReference) {
            return "FieldReference";
        }
        String name = expr.getClass().getSimpleName();
        name = stripImmutablePrefix(name);
        // Strip outer class prefix after $ (e.g. Expression$Cast → Cast)
        int dollarIdx = name.lastIndexOf('$');
        if (dollarIdx >= 0 && dollarIdx < name.length() - 1) {
            name = name.substring(dollarIdx + 1);
        }
        return name;
    }

    private static String stripImmutablePrefix(String name) {
        if (name.startsWith("Immutable")) {
            return name.substring("Immutable".length());
        }
        return name;
    }

    private static PlanValidationResult buildResult(List<UnsupportedNode> unsupported,
                                                int totalRelCount) {
        if (unsupported.isEmpty()) {
            return new PlanValidationResult(ValidationStatus.FULLY_NATIVE, List.of());
        }
        // Count how many Rel nodes (not expressions) are unsupported.
        long unsupportedRelCount = unsupported.stream()
                .filter(n -> n.kind() == UnsupportedNode.Kind.RELATION)
                .count();
        if (totalRelCount > 0 && unsupportedRelCount >= totalRelCount) {
            return new PlanValidationResult(ValidationStatus.JAVA_ONLY, unsupported);
        }
        return new PlanValidationResult(ValidationStatus.PARTIAL_NATIVE, unsupported);
    }
}
