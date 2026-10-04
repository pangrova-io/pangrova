package org.pangrova.planner.substrait;

import io.substrait.extension.ImmutableSimpleExtension;
import io.substrait.extension.SimpleExtension;
import io.substrait.function.ImmutableParameterizedType;
import io.substrait.function.TypeExpression;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.Map;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;
import org.pangrova.protocol.function.declaration.DeclaredFunctionReference;
import org.pangrova.protocol.v2.FunctionKind;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps Calcite SqlOperator to Substrait function references and extension variants.
 * Builds variants manually to avoid ANTLR version conflict from SimpleExtension.loadDefaults().
 */
public class FunctionMapper {

    /**
     * Wildcard placeholder return type used when the SDK serializer requires a
     * non-null returnType but the actual type must be resolved by the consumer
     * from the input expression. Using a Substrait type parameter ("any1")
     * matches the substrait spec for type-parameter-driven resolution and
     * avoids misrepresenting unrelated functions as I32/I64.
     *
     * <p>This is intentionally shared across scalar / aggregate / window variants
     * because the consumer never trusts variant.returnType() during execution
     * (see {@code SubstraitConverter} — output types come from input types).</p>
     */
    private static final TypeExpression PLACEHOLDER_RETURN_TYPE =
            ImmutableParameterizedType.StringLiteral.builder()
                    .nullable(true)
                    .value("any1")
                    .build();

    private final Map<String, SimpleExtension.ScalarFunctionVariant> scalarCache =
            new ConcurrentHashMap<>();
    private final Map<String, SimpleExtension.AggregateFunctionVariant> aggregateCache =
            new ConcurrentHashMap<>();
    private final Map<String, SimpleExtension.WindowFunctionVariant> windowCache =
            new ConcurrentHashMap<>();

    /**
     * Maps a Calcite SQL operator to its Substrait function reference.
     *
     * @param operator operator to map
     * @return resolved Substrait function reference
     */
    public FunctionReference map(SqlOperator operator) {
        String name = operator.getName();
        String kind = operator.getKind().name();
        // Named aggregate aliases such as APPROX_COUNT_DISTINCT take precedence
        // over generic COUNT, while window names belong to a separate context.
        return FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, name, "")
                .or(() -> FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_AGGREGATE, name, ""))
                .or(() -> FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, "", kind))
                .or(() -> FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_AGGREGATE, "", kind))
                .map(FunctionMapper::reference)
                .orElseThrow(() -> unsupported(operator.getName()));
    }

    private static FunctionReference reference(DeclaredFunctionReference declaration) {
        return new FunctionReference(declaration.namespace(), declaration.name());
    }

    private static SubstraitConversionException unsupported(String name) {
        return new SubstraitConversionException(PangrovaErrorCode.SUBSTRAIT_FUNCTION_MAPPING_FAILURE,
                "Unsupported operator: " + name);
    }

    public SimpleExtension.ScalarFunctionVariant resolveScalarVariant(FunctionReference ref) {
        return scalarCache.computeIfAbsent(ref.compoundName(), k -> buildScalarVariant(ref));
    }

    public SimpleExtension.AggregateFunctionVariant resolveAggregateVariant(FunctionReference ref) {
        return aggregateCache.computeIfAbsent(ref.compoundName(), k -> buildAggregateVariant(ref));
    }

    public boolean isAggregate(SqlKind kind) {
        return FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_AGGREGATE, "", kind.name()).isPresent();
    }

    public boolean isWindowFunction(String name) {
        return FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_WINDOW, name, "").isPresent();
    }

    /** Resolves a window function by name (e.g. ROW_NUMBER, RANK). */
    public FunctionReference mapWindowFunction(String name) {
        return FunctionBindingDeclarations.sqlBinding(FunctionKind.FUNCTION_KIND_WINDOW, name, "")
                .map(FunctionMapper::reference)
                .orElseThrow(() -> new SubstraitConversionException(
                        PangrovaErrorCode.SUBSTRAIT_FUNCTION_MAPPING_FAILURE, "Unsupported window function: " + name));
    }

    public SimpleExtension.WindowFunctionVariant resolveWindowVariant(FunctionReference ref) {
        return windowCache.computeIfAbsent(ref.compoundName(), k -> buildWindowVariant(ref));
    }

    /**
     * Builds a scalar variant with a Substrait wildcard placeholder return type ("any1").
     * The consumer always derives the concrete return type from input expression types,
     * so the variant return type is intentionally a type parameter rather than a
     * misleading concrete type like I32.
     */
    private static SimpleExtension.ScalarFunctionVariant buildScalarVariant(FunctionReference ref) {
        return ImmutableSimpleExtension.ScalarFunctionVariant.builder()
                .name(ref.name())
                .urn(FunctionBindingDeclarations.canonicalUrn(FunctionKind.FUNCTION_KIND_SCALAR, ref.namespace(), ref.name()))
                .returnType(PLACEHOLDER_RETURN_TYPE)
                .build();
    }

    /**
     * Builds an aggregate variant with a Substrait wildcard placeholder return type.
     * See {@link #buildScalarVariant(FunctionReference)} for rationale.
     */
    private static SimpleExtension.AggregateFunctionVariant buildAggregateVariant(FunctionReference ref) {
        return ImmutableSimpleExtension.AggregateFunctionVariant.builder()
                .name(ref.name())
                .urn(FunctionBindingDeclarations.canonicalUrn(FunctionKind.FUNCTION_KIND_AGGREGATE, ref.namespace(), ref.name()))
                .returnType(PLACEHOLDER_RETURN_TYPE)
                .build();
    }

    /**
     * Builds a window variant with a Substrait wildcard placeholder return type.
     * See {@link #buildScalarVariant(FunctionReference)} for rationale.
     */
    private static SimpleExtension.WindowFunctionVariant buildWindowVariant(FunctionReference ref) {
        return ImmutableSimpleExtension.WindowFunctionVariant.builder()
                .name(ref.name())
                .urn(FunctionBindingDeclarations.canonicalUrn(FunctionKind.FUNCTION_KIND_WINDOW, ref.namespace(), ref.name()))
                .returnType(PLACEHOLDER_RETURN_TYPE)
                .windowType(SimpleExtension.WindowType.PARTITION)
                .build();
    }

}
