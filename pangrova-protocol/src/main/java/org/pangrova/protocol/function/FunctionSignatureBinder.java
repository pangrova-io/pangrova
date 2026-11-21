package org.pangrova.protocol.function;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.FunctionSignatureContract;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Canonical registered-signature binder, used before implementation selection or factories. */
public final class FunctionSignatureBinder {
    private static final int MAX_DERIVED_PARAMETERS = 4096;

    public record Binding(Type output, Optional<Type> intermediate) {
        public Binding { intermediate = Objects.requireNonNull(intermediate, "intermediate"); }
    }

    private final FunctionPhysicalTypeProjector projector;

    public FunctionSignatureBinder(FunctionPhysicalTypeProjector projector) {
        this.projector = Objects.requireNonNull(projector, "projector");
    }

    /** Constant positions describe this concrete call, including repeated variadic arguments. */
    public static void validateConstantOrdinals(int arity, Set<Integer> constants) {
        if (arity < 0 || arity > 256 || constants.stream().anyMatch(ordinal -> ordinal < 0 || ordinal >= arity)) {
            throw new IllegalArgumentException("Constant argument ordinal is outside the actual call");
        }
    }

    /** Checks only the selected signature; failure never authorizes another matching overload. */
    public static void requireConstants(FunctionSignatureContract signature, int arity, Set<Integer> constants) {
        validateConstantOrdinals(arity, constants);
        for (int ordinal = 0; ordinal < arity; ordinal++) {
            int formal = Math.min(ordinal, signature.getConstantArgumentsCount() - 1);
            if (formal >= 0 && signature.getConstantArguments(formal) && !constants.contains(ordinal)) {
                throw new IllegalArgumentException(
                        "Selected implementation requires a constant argument at ordinal " + ordinal);
            }
        }
    }

    /** Empty means an input signature non-match; selected output failure propagates unchanged. */
    public Optional<Binding> bind(FunctionSignatureContract signature, NamedStruct arguments) {
        return bind(signature, arguments, false);
    }

    /** Aggregate state/result output is explicit; original raw arguments are never inferred from state. */
    public Optional<Binding> bind(FunctionSignatureContract signature,
                                  NamedStruct arguments, boolean intermediateOutput) {
        Objects.requireNonNull(signature, "signature");
        Objects.requireNonNull(arguments, "arguments");
        SubstraitRelationLayout.topLevelNames(arguments);
        int formal = signature.getArgumentsCount();
        int actual = arguments.getStruct().getTypesCount();
        validateArity(signature, formal, actual);
        if (signature.getVariableArity() ? actual < formal - 1 : actual != formal) return Optional.empty();
        var variables = new FunctionSignatureBindings(signature, projector);
        var matcher = new FunctionTypeTemplateMatcher(projector);
        for (int index = 0; index < actual; index++) {
            int ordinal = index;
            var template = signature.getArguments(Math.min(index, formal - 1));
            if (!matcher.match(template, arguments.getStruct().getTypes(index), variables,
                    path -> SubstraitRelationLayout.nameAtCanonicalTypePath(arguments, ordinal, path))) {
                return Optional.empty();
            }
        }
        var resolver = new FunctionTypeTemplateResolver(projector);
        Optional<Type> intermediate = signature.hasIntermediate()
                ? Optional.of(resolver.resolve(signature.getIntermediate(), variables)) : Optional.empty();
        if (intermediateOutput && intermediate.isEmpty()) {
            throw new IllegalArgumentException("State output requires an actual aggregate intermediate contract");
        }
        Type output = intermediateOutput ? intermediate.orElseThrow()
                : resolver.resolve(signature.getOutput(), variables);
        return Optional.of(new Binding(output, intermediate));
    }

    private static void validateArity(FunctionSignatureContract signature, int formal, int actual) {
        if (formal > 256 || actual > 256 || signature.getConstantArgumentsCount() != formal
                || signature.getDerivedParameterOrderCount() > MAX_DERIVED_PARAMETERS
                || (signature.getVariableArity() && formal == 0) || !signature.hasOutput()) {
            throw new IllegalArgumentException("Malformed or unbounded registered signature arity");
        }
    }
}
