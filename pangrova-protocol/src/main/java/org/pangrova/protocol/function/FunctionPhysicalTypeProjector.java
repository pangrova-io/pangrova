package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.formula.FunctionTypeFormulaEvaluator;
import org.pangrova.protocol.v2.FunctionCatalogManifest;
import org.pangrova.protocol.v2.FunctionPhysicalTypePattern;
import org.pangrova.protocol.v2.FunctionPhysicalTypeProjection;
import org.pangrova.protocol.v2.FunctionTypeProjectionChildPath;
import org.pangrova.protocol.v2.FunctionTypeTemplate;
import org.pangrova.protocol.v2.FunctionTypeTraitRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Applies the manifest's generated Native bridge, never a Java SQL-kind table. */
public final class FunctionPhysicalTypeProjector {
    public record Projection(Type bindingType, FunctionPhysicalTypePattern physicalType,
                             boolean known, boolean orderable, boolean comparable) {}

    private final Map<Integer, List<FunctionPhysicalTypeProjection>> projections;

    /** Captures and validates the manifest's ordered canonical type projections without runtime factories. */
    public FunctionPhysicalTypeProjector(FunctionCatalogManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        Map<Integer, List<FunctionPhysicalTypeProjection>> indexed = new HashMap<>();
        if (manifest.getPhysicalTypeProjectionsCount() == 0 || manifest.getPhysicalTypeProjectionsCount() > 256) {
            throw new IllegalArgumentException("Actual catalog has no bounded canonical type bridge");
        }
        for (var projection : manifest.getPhysicalTypeProjectionsList()) {
            validate(projection);
            int kind = kind(projection.getLogicalType().getSkeleton()).getNumber();
            indexed.computeIfAbsent(kind, ignored -> new ArrayList<>()).add(projection);
        }
        indexed.replaceAll((ignored, entries) -> List.copyOf(entries));
        projections = Map.copyOf(indexed);
    }

    public Projection project(Type type) {
        return project(Objects.requireNonNull(type, "type"), new Budget(), 0);
    }

    private Projection project(Type type, Budget budget, int depth) {
        budget.consume(depth);
        var candidates = projections.get(kind(type).getNumber());
        if (candidates == null) throw new IllegalArgumentException("Type has no actual Native bridge projection");
        FunctionPhysicalTypeProjection selected = null;
        Map<String, Long> selectedVariables = Map.of();
        for (var candidate : candidates) {
            var variables = bindParameters(candidate, type);
            if (!candidate.getPredicatesList().stream().allMatch(
                    predicate -> FunctionTypeFormulaEvaluator.evaluate(predicate, variables) != 0)) {
                continue;
            }
            if (selected != null) throw new IllegalArgumentException("Actual canonical type projections overlap");
            selected = candidate;
            selectedVariables = variables;
        }
        if (selected == null) throw new IllegalArgumentException("Canonical type parameters are not representable");
        return materialize(selected, type, selectedVariables, budget, depth);
    }

    private Projection materialize(FunctionPhysicalTypeProjection declaration, Type type,
                                   Map<String, Long> variables, Budget budget, int depth) {
        Type binding = substitute(declaration.getBindingType(), variables);
        var physical = declaration.getPhysicalType().toBuilder();
        List<Projection> children = new ArrayList<>();
        for (var path : declaration.getRecursiveChildrenList()) {
            var values = FunctionProjectionChildren.read(type, path);
            List<Type> normalized = new ArrayList<>();
            for (var child : values) {
                Projection projected = project(child, budget, depth + 1);
                children.add(projected);
                normalized.add(projected.bindingType());
                physical.addChildren(projected.physicalType());
            }
            binding = FunctionProjectionChildren.replace(binding, path, normalized);
        }
        return new Projection(binding, physical.build(), trait(declaration.getKnownType(), children, 0),
                trait(declaration.getOrderable(), children, 1), trait(declaration.getComparable(), children, 2));
    }

    private static Map<String, Long> bindParameters(FunctionPhysicalTypeProjection declaration, Type type) {
        Map<String, Long> values = new HashMap<>();
        for (var slot : declaration.getLogicalType().getSlotsList()) {
            var leaf = FunctionTypePath.read(type, slot.getPathList());
            if (leaf.field().getType() != FieldDescriptor.Type.INT32) {
                throw new IllegalArgumentException("Bridge parameter is not canonical int32");
            }
            String variable = slot.getInteger().getVariableName();
            long value = (Integer) leaf.value();
            Long previous = values.putIfAbsent(variable, value);
            if (previous != null && previous != value) {
                throw new IllegalArgumentException("Bridge parameter has conflicting actual values");
            }
        }
        if (values.size() != declaration.getVariablesCount()) {
            throw new IllegalArgumentException("Bridge contains unbound parameters");
        }
        return Map.copyOf(values);
    }

    private static Type substitute(FunctionTypeTemplate template, Map<String, Long> variables) {
        Message result = template.getSkeleton();
        for (var slot : template.getSlotsList()) {
            int value = FunctionTypeFormulaEvaluator.evaluateParameter(slot.getInteger(), variables);
            result = FunctionTypePath.replace(result, slot.getPathList(), value);
        }
        return (Type) result;
    }

    private static boolean trait(FunctionTypeTraitRule rule, List<Projection> children, int ordinal) {
        return switch (rule) {
            case FUNCTION_TYPE_TRAIT_RULE_ALWAYS -> true;
            case FUNCTION_TYPE_TRAIT_RULE_NEVER -> false;
            case FUNCTION_TYPE_TRAIT_RULE_ALL_CHILDREN -> children.stream().allMatch(child -> switch (ordinal) {
                case 0 -> child.known();
                case 1 -> child.orderable();
                case 2 -> child.comparable();
                default -> throw new IllegalArgumentException("Unknown bridge trait ordinal");
            });
            default -> throw new IllegalArgumentException("Unknown actual Native type trait");
        };
    }

    private static FieldDescriptor kind(Type type) {
        return FunctionTypePath.kind(type);
    }

    private static void validate(FunctionPhysicalTypeProjection projection) {
        if (!projection.getUnknownFields().asMap().isEmpty() || !projection.hasLogicalType()
                || !projection.hasBindingType() || !projection.hasPhysicalType()
                || projection.getNativeTypeName().isBlank() || projection.getPhysicalType().getRuntimeTag().isBlank()
                || projection.getPhysicalType().getChildrenCount() != 0) {
            throw new IllegalArgumentException("Malformed canonical Native projection");
        }
        FunctionProjectionValidation.validateTemplates(projection);
        kind(projection.getLogicalType().getSkeleton());
        kind(projection.getBindingType().getSkeleton());
        trait(projection.getKnownType(), List.of(), 0);
        trait(projection.getOrderable(), List.of(), 1);
        trait(projection.getComparable(), List.of(), 2);
        for (FunctionTypeProjectionChildPath path : projection.getRecursiveChildrenList()) {
            FunctionProjectionChildren.validate(projection.getLogicalType().getSkeleton(), path);
            FunctionProjectionChildren.validate(projection.getBindingType().getSkeleton(), path);
        }
    }

    private static final class Budget {
        private int remaining = 2048;

        void consume(int depth) {
            if (depth > 64 || --remaining < 0) {
                throw new IllegalArgumentException("Native type projection is too deep or large");
            }
        }
    }
}
