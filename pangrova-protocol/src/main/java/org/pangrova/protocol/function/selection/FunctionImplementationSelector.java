package org.pangrova.protocol.function.selection;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.FunctionPhysicalTypeProjector;
import org.pangrova.protocol.function.FunctionSignatureBinder;
import org.pangrova.protocol.function.generated.GeneratedExecutionSelection;
import org.pangrova.protocol.v2.FunctionImplementationContract;
import org.pangrova.protocol.v2.FunctionImplementationFamily;
import org.pangrova.protocol.v2.FunctionPhysicalTypePattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Selects an actual immutable entry using the same generated policy as SDK execution. */
public final class FunctionImplementationSelector {
    private static final int MAX_IMPLEMENTATIONS = 4096;
    private static final int MAX_PHYSICAL_PATTERN_NODES = 4096;

    public record Selection(FunctionImplementationContract implementation, FunctionSignatureBinder.Binding binding) {}

    private record Entry(FunctionImplementationContract contract) implements GeneratedExecutionSelection.Entry {
        @Override
        public int family() {
            return contract.getFamilyValue();
        }

        @Override
        public long priority() {
            return Integer.toUnsignedLong(contract.getPriority());
        }
    }

    private final FunctionPhysicalTypeProjector projector;
    private final FunctionSignatureBinder signatures;
    private final FunctionSpecialTypeBinder specials;

    public FunctionImplementationSelector(FunctionPhysicalTypeProjector projector) {
        this.projector = Objects.requireNonNull(projector, "projector");
        signatures = new FunctionSignatureBinder(projector);
        specials = new FunctionSpecialTypeBinder(projector);
    }

    /** Never searches subsequent entries for a desired result type or constant mask. */
    public Optional<Selection> select(List<FunctionImplementationContract> implementations,
                                      NamedStruct arguments, Set<Integer> constants) {
        return select(implementations, arguments, constants, null);
    }

    /** Structural casts require a declared target; it never influences overload selection. */
    public Optional<Selection> select(List<FunctionImplementationContract> implementations,
                                      NamedStruct arguments, Set<Integer> constants, Type declaredTarget) {
        Objects.requireNonNull(arguments, "arguments");
        Set<Integer> constantOrdinals = Set.copyOf(constants);
        int arity = arguments.getStruct().getTypesCount();
        FunctionSignatureBinder.validateConstantOrdinals(arity, constantOrdinals);
        List<Entry> entries = validate(implementations);
        var selected = GeneratedExecutionSelection.select(entries,
                entry -> bind(entry.contract(), arguments, declaredTarget));
        selected.ifPresent(selection -> FunctionSignatureBinder.requireConstants(
                selection.implementation().getSignature(), arity, constantOrdinals));
        return selected;
    }

    private Optional<Selection> bind(FunctionImplementationContract implementation,
                                     NamedStruct arguments, Type declaredTarget) {
        if (implementation.getFamily() == FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM) {
            return Optional.of(new Selection(implementation, specials.bind(implementation, arguments, declaredTarget)));
        }
        return signatures.bind(implementation.getSignature(), arguments).filter(binding ->
                matchesPhysical(implementation, arguments, binding))
                .map(binding -> new Selection(implementation, binding));
    }

    private boolean matchesPhysical(FunctionImplementationContract implementation, NamedStruct arguments,
                                    FunctionSignatureBinder.Binding binding) {
        if (implementation.getFamily() != FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SCALAR) {
            return true;
        }
        int count = Math.min(arguments.getStruct().getTypesCount(), implementation.getArgumentPhysicalCount());
        for (int ordinal = 0; ordinal < count; ordinal++) {
            var actual = projector.project(arguments.getStruct().getTypes(ordinal)).physicalType();
            if (!matches(actual, implementation.getArgumentPhysical(ordinal))) return false;
        }
        return matches(projector.project(binding.output()).physicalType(), implementation.getOutputPhysical());
    }

    private static boolean matches(FunctionPhysicalTypePattern actual, FunctionPhysicalTypePattern constraint) {
        if (constraint.getRuntimeTag().equals("UNKNOWN")) return true;
        if (!actual.getRuntimeTag().equals(constraint.getRuntimeTag())
                || actual.getChildrenCount() != constraint.getChildrenCount()) return false;
        for (int ordinal = 0; ordinal < actual.getChildrenCount(); ordinal++) {
            if (!matches(actual.getChildren(ordinal), constraint.getChildren(ordinal))) return false;
        }
        return true;
    }

    private static List<Entry> validate(List<FunctionImplementationContract> implementations) {
        if (implementations.isEmpty() || implementations.size() > MAX_IMPLEMENTATIONS) {
            throw new IllegalArgumentException("Actual implementation entries are absent or unbounded");
        }
        List<Entry> entries = new ArrayList<>();
        for (int ordinal = 0; ordinal < implementations.size(); ordinal++) {
            var implementation = implementations.get(ordinal);
            if (!implementation.getUnknownFields().asMap().isEmpty()
                    || Integer.toUnsignedLong(implementation.getTraversalOrdinal()) != ordinal) {
                throw new IllegalArgumentException("Actual implementation traversal order is malformed");
            }
            validateShape(implementation);
            entries.add(new Entry(implementation));
        }
        return List.copyOf(entries);
    }

    private static void validateShape(FunctionImplementationContract implementation) {
        switch (implementation.getFamily()) {
            case FUNCTION_IMPLEMENTATION_FAMILY_SCALAR -> {
                if (!implementation.hasPriority() || !implementation.hasOutputPhysical()
                        || implementation.getArgumentPhysicalCount() > 256) {
                    throw new IllegalArgumentException(
                            "Simple implementation is missing its actual physical constraints");
                }
                validatePattern(implementation.getOutputPhysical(), 0, new int[]{MAX_PHYSICAL_PATTERN_NODES});
                for (var pattern : implementation.getArgumentPhysicalList()) {
                    validatePattern(pattern, 0, new int[]{MAX_PHYSICAL_PATTERN_NODES});
                }
            }
            case FUNCTION_IMPLEMENTATION_FAMILY_VECTOR, FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM -> {
                if (implementation.hasPriority() || implementation.hasOutputPhysical()
                        || implementation.getArgumentPhysicalCount() != 0) {
                    throw new IllegalArgumentException("Non-simple implementation contains invented physical filters");
                }
            }
            default -> throw new IllegalArgumentException("Unknown actual implementation family");
        }
        if (implementation.getFamily() != FunctionImplementationFamily.FUNCTION_IMPLEMENTATION_FAMILY_SPECIAL_FORM
                && !implementation.hasSignature()) {
            throw new IllegalArgumentException("Actual implementation has no registered signature");
        }
    }

    private static void validatePattern(FunctionPhysicalTypePattern pattern, int depth, int[] budget) {
        if (depth > 64 || --budget[0] < 0 || pattern.getChildrenCount() > 256
                || pattern.getRuntimeTag().isBlank() || pattern.getRuntimeTag().length() > 64
                || !pattern.getUnknownFields().asMap().isEmpty()
                || (pattern.getRuntimeTag().equals("UNKNOWN") && pattern.getChildrenCount() != 0)) {
            throw new IllegalArgumentException("Malformed or unbounded actual physical constraint");
        }
        for (var child : pattern.getChildrenList()) validatePattern(child, depth + 1, budget);
    }

}
